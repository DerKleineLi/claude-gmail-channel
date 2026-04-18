#!/usr/bin/env python3
"""
Gmail channel for Claude Code.

Modeled on claude-scheduler-channel. Runs `gws gmail +watch` as a subprocess
and forwards each new email as a ``notifications/claude/channel`` event so
the active Claude session wakes up only when a real inbound message arrives
(no polling).

Run via Claude Code with:
    claude --dangerously-load-development-channels server:gmail ...

Environment:
    GMAIL_CHANNEL_PROJECT   GCP project ID (required unless --project flag set elsewhere)
    GMAIL_CHANNEL_LABELS    Comma-separated Gmail label IDs (default: INBOX)
    GMAIL_CHANNEL_MSG_FMT   gws --msg-format (default: metadata — fast; use full for body inline)
    GMAIL_CHANNEL_SUBSCRIPTION  Existing Pub/Sub subscription name (reuse across restarts)

Rules persist to ~/.claude/gmail_channel/rules.json.
"""
from __future__ import annotations

import asyncio
import ctypes
import json
import logging
import os
import re
import signal
import subprocess
import uuid
from pathlib import Path
from typing import Any

from mcp.server.lowlevel import NotificationOptions, Server
from mcp.server.stdio import stdio_server
from mcp.shared.message import SessionMessage
import mcp.types as types

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s gmail_channel: %(message)s",
)
log = logging.getLogger("gmail_channel")

PROJECT = os.environ.get("GMAIL_CHANNEL_PROJECT", "gws-claude-ca781a")
LABELS = os.environ.get("GMAIL_CHANNEL_LABELS", "INBOX")
MSG_FMT = os.environ.get("GMAIL_CHANNEL_MSG_FMT", "metadata")
SUBSCRIPTION = os.environ.get("GMAIL_CHANNEL_SUBSCRIPTION", "")
TOPIC = os.environ.get("GMAIL_CHANNEL_TOPIC", "")  # default: derive from SUBSCRIPTION
WATCH_RENEW_EVERY_SEC = int(os.environ.get("GMAIL_CHANNEL_WATCH_RENEW_SEC", str(6 * 24 * 3600)))
# Gmail watch expires after 7 days; renew at 6 days so we always have margin.

RULES_FILE = Path.home() / ".claude" / "gmail_channel" / "rules.json"
ARCHIVE_FILE = Path.home() / ".claude" / "gmail_channel" / "archive.ndjson"

server: Server = Server("gmail")
_session: Any = None
_watcher_task: asyncio.Task | None = None
_renewer_task: asyncio.Task | None = None
_stats = {
    "started_at": None,
    "events": 0,
    "restarts": 0,
    "last_error": None,
    "watch_last_registered_at": None,
    "watch_last_expiration": None,
    "watch_renewals": 0,
}


def _derive_topic() -> str:
    if TOPIC:
        return TOPIC
    # "projects/X/subscriptions/Y" → "projects/X/topics/Y"
    if SUBSCRIPTION and "/subscriptions/" in SUBSCRIPTION:
        return SUBSCRIPTION.replace("/subscriptions/", "/topics/", 1)
    return ""


async def _register_gmail_watch() -> bool:
    """Call `gmail.users.watch` so Gmail publishes changes to our topic."""
    topic = _derive_topic()
    if not topic:
        log.warning("no topic to publish to; skipping users.watch. Set GMAIL_CHANNEL_TOPIC or GMAIL_CHANNEL_SUBSCRIPTION.")
        return False
    body = json.dumps({
        "topicName": topic,
        "labelIds": [l for l in LABELS.split(",") if l],
        "labelFilterBehavior": "include",
    })
    cmd = ["gws", "gmail", "users", "watch", "--params", '{"userId":"me"}', "--json", body]
    log.info("registering gmail watch against %s", topic)
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env={**os.environ, "PATH": os.environ.get("PATH", "") + ":/usr/bin:/usr/local/bin"},
        )
        stdout, stderr = await proc.communicate()
    except FileNotFoundError as e:
        log.error("gws not on PATH; cannot register watch: %s", e)
        _stats["last_error"] = f"gws not on PATH: {e}"
        return False
    if proc.returncode != 0:
        log.error("gmail users.watch failed rc=%s stderr=%s", proc.returncode, stderr.decode(errors="replace").strip()[:500])
        _stats["last_error"] = f"users.watch rc={proc.returncode}"
        return False
    try:
        resp = json.loads(stdout.decode(errors="replace"))
        _stats["watch_last_expiration"] = resp.get("expiration")
    except Exception:
        pass
    _stats["watch_last_registered_at"] = asyncio.get_event_loop().time()
    _stats["watch_renewals"] += 1
    log.info("gmail watch registered; expiration=%s", _stats["watch_last_expiration"])
    return True


async def _run_watch_renewer() -> None:
    """Register the Gmail watch on startup, then renew every WATCH_RENEW_EVERY_SEC."""
    backoff = 60
    while True:
        ok = await _register_gmail_watch()
        if ok:
            await asyncio.sleep(WATCH_RENEW_EVERY_SEC)
            backoff = 60
        else:
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 900)  # cap at 15 min


# ---------------------------------------------------------------------------
# Rules
# ---------------------------------------------------------------------------
def load_rules() -> list[dict[str, Any]]:
    if not RULES_FILE.exists():
        return []
    try:
        return json.loads(RULES_FILE.read_text(encoding="utf-8"))
    except Exception as e:
        log.error("failed to load rules: %s", e)
        return []


def save_rules(rules: list[dict[str, Any]]) -> None:
    RULES_FILE.parent.mkdir(parents=True, exist_ok=True)
    RULES_FILE.write_text(json.dumps(rules, indent=2), encoding="utf-8")


def _compile_rule_regexes(rule: dict[str, Any]) -> dict[str, re.Pattern]:
    out: dict[str, re.Pattern] = {}
    for field in ("from_regex", "subject_regex", "label_regex"):
        pat = rule.get(field)
        if pat:
            out[field] = re.compile(pat, re.IGNORECASE)
    return out


def _match_rule(rule: dict[str, Any], ctx: dict[str, str]) -> bool:
    try:
        compiled = _compile_rule_regexes(rule)
    except re.error as e:
        log.warning("bad regex in rule %s: %s", rule.get("id"), e)
        return False
    if "from_regex" in compiled and not compiled["from_regex"].search(ctx["from"]):
        return False
    if "subject_regex" in compiled and not compiled["subject_regex"].search(ctx["subject"]):
        return False
    if "label_regex" in compiled and not compiled["label_regex"].search(ctx["labels"]):
        return False
    return True


DEFAULT_PROMPT = (
    "New email received. Triage it.\n\n"
    "From:    {from}\n"
    "Subject: {subject}\n"
    "Date:    {date}\n"
    "Labels:  {labels}\n"
    "MsgId:   {msg_id}\n"
    "ThreadId:{thread_id}\n\n"
    "Snippet: {snippet}\n\n"
    "Fetch the full body with:\n"
    "  gws gmail users messages get --params '{{\"userId\":\"me\",\"id\":\"{msg_id}\",\"format\":\"full\"}}'\n\n"
    "Decide: spam (trash silently) / security-critical (Telegram now) / "
    "actionable (act, then Telegram) / archivable (leave, daily digest will pick up)."
)


def _headers_to_dict(headers: list[dict[str, str]]) -> dict[str, str]:
    return {h.get("name", "").lower(): h.get("value", "") for h in headers or []}


def _build_context(msg: dict[str, Any]) -> dict[str, str]:
    payload = msg.get("payload", {}) or {}
    headers = _headers_to_dict(payload.get("headers", []))
    return {
        "from": headers.get("from", ""),
        "subject": headers.get("subject", ""),
        "date": headers.get("date", ""),
        "snippet": msg.get("snippet") or "",
        "labels": ",".join(msg.get("labelIds", []) or []),
        "msg_id": msg.get("id", ""),
        "thread_id": msg.get("threadId", ""),
    }


def _format_prompt(template: str, ctx: dict[str, str]) -> str:
    """Safe substitution — unknown placeholders stay literal, no KeyError."""
    class _SafeDict(dict):
        def __missing__(self, key: str) -> str:
            return "{" + key + "}"
    try:
        return template.format_map(_SafeDict(ctx))
    except Exception as e:
        log.warning("prompt template format failed: %s; using raw template", e)
        return template


def _resolve_prompt(ctx: dict[str, str]) -> tuple[str, str]:
    """Return (prompt_text, matched_rule_name)."""
    for rule in load_rules():
        if _match_rule(rule, ctx):
            return _format_prompt(rule.get("prompt", DEFAULT_PROMPT), ctx), rule.get("name", rule.get("id", ""))
    return _format_prompt(DEFAULT_PROMPT, ctx), ""


# ---------------------------------------------------------------------------
# Watcher
# ---------------------------------------------------------------------------
def _resolve_rule(ctx: dict[str, str]) -> dict[str, Any] | None:
    """Return the first matching rule, or None for default-fallback behavior."""
    for rule in load_rules():
        if _match_rule(rule, ctx):
            return rule
    return None


def _append_archive(ctx: dict[str, str], rule_name: str) -> None:
    """Append an archived email entry to ARCHIVE_FILE (NDJSON)."""
    ARCHIVE_FILE.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        "ts": __import__("datetime").datetime.utcnow().isoformat() + "Z",
        "msg_id": ctx["msg_id"],
        "thread_id": ctx["thread_id"],
        "from": ctx["from"],
        "subject": ctx["subject"],
        "date": ctx["date"],
        "labels": ctx["labels"],
        "snippet": ctx["snippet"],
        "rule": rule_name,
    }
    with ARCHIVE_FILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


async def _push(msg: dict[str, Any]) -> None:
    if _session is None:
        log.warning("no session captured yet; dropping event for msgId=%s", msg.get("id"))
        return
    ctx = _build_context(msg)
    rule = _resolve_rule(ctx)

    # Archive-only rule: save locally and DO NOT wake the session.
    if rule and rule.get("archive"):
        try:
            _append_archive(ctx, rule.get("name", rule.get("id", "")))
            _stats["archived"] = _stats.get("archived", 0) + 1
            log.info("archived msgId=%s rule=%s from=%s", ctx["msg_id"], rule.get("name"), ctx["from"])
        except Exception as e:
            log.exception("failed to archive: %s", e)
            _stats["last_error"] = f"archive failed: {e}"
        return

    # Normal path: render prompt and forward as channel notification.
    template = rule.get("prompt", DEFAULT_PROMPT) if rule else DEFAULT_PROMPT
    prompt = _format_prompt(template, ctx)
    matched = rule.get("name", rule.get("id", "")) if rule else ""
    try:
        raw = types.JSONRPCNotification(
            jsonrpc="2.0",
            method="notifications/claude/channel",
            params={
                "content": prompt,
                "meta": {
                    "source": "gmail",
                    "msg_id": ctx["msg_id"],
                    "thread_id": ctx["thread_id"],
                    "from": ctx["from"],
                    "subject": ctx["subject"],
                    "rule": matched,
                },
            },
        )
        await _session.send_message(SessionMessage(message=types.JSONRPCMessage(root=raw)))
        _stats["events"] += 1
    except Exception as e:
        log.exception("failed to push notification: %s", e)
        _stats["last_error"] = str(e)


_PRCTL_PR_SET_PDEATHSIG = 1


def _set_pdeathsig() -> None:
    """preexec hook: tell the Linux kernel to SIGTERM this subprocess when its
    parent (the MCP Python process) dies, so we can't leave orphan `gws +watch`
    processes lying around after a hard kill. Safe no-op if libc isn't available.
    """
    try:
        libc = ctypes.CDLL("libc.so.6", use_errno=True)
        libc.prctl(_PRCTL_PR_SET_PDEATHSIG, signal.SIGTERM)
    except OSError:
        pass


def _kill_orphan_watchers() -> None:
    """Kill any pre-existing `gws gmail +watch` processes bound to the same
    subscription — they're orphans from a previous MCP instance that would
    otherwise compete for the same Pub/Sub messages and silently swallow them.
    """
    pat = "gws gmail +watch"
    if SUBSCRIPTION:
        pat = SUBSCRIPTION  # more precise; pkill -f matches full cmdline
    try:
        subprocess.run(["pkill", "-f", pat], check=False, timeout=5)
        log.info("pkill -f %r (orphan cleanup)", pat)
    except Exception as e:
        log.warning("pkill failed (non-fatal): %s", e)


async def _kill_proc(proc: asyncio.subprocess.Process) -> None:
    try:
        proc.terminate()
    except ProcessLookupError:
        return
    try:
        await asyncio.wait_for(proc.wait(), timeout=5)
    except asyncio.TimeoutError:
        try:
            proc.kill()
        except ProcessLookupError:
            pass
        try:
            await asyncio.wait_for(proc.wait(), timeout=5)
        except asyncio.TimeoutError:
            log.warning("watcher subprocess did not exit after SIGKILL")


async def _run_watcher() -> None:
    _kill_orphan_watchers()  # first boot of this task — clear any pre-existing ones
    backoff = 2.0
    while True:
        cmd = [
            "gws", "gmail", "+watch",
            "--project", PROJECT,
            "--label-ids", LABELS,
            "--msg-format", MSG_FMT,
        ]
        if SUBSCRIPTION:
            cmd += ["--subscription", SUBSCRIPTION]
        log.info("starting watcher: %s", " ".join(cmd))
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env={**os.environ, "PATH": os.environ.get("PATH", "") + ":/usr/bin:/usr/local/bin"},
                preexec_fn=_set_pdeathsig,
            )
        except FileNotFoundError as e:
            log.error("gws not on PATH; watcher cannot start: %s", e)
            _stats["last_error"] = f"gws not on PATH: {e}"
            await asyncio.sleep(60)
            continue

        async def drain_stderr() -> None:
            assert proc.stderr
            try:
                async for line in proc.stderr:
                    log.info("watcher stderr: %s", line.decode(errors="replace").rstrip())
            except asyncio.CancelledError:
                pass

        stderr_task = asyncio.create_task(drain_stderr())

        try:
            assert proc.stdout
            async for raw_line in proc.stdout:
                line = raw_line.decode(errors="replace").strip()
                if not line:
                    continue
                try:
                    msg = json.loads(line)
                except json.JSONDecodeError as e:
                    log.warning("non-JSON line from watcher: %r (%s)", line[:200], e)
                    continue
                await _push(msg)
        except asyncio.CancelledError:
            log.info("watcher cancelled, killing subprocess pid=%s", proc.pid)
            await _kill_proc(proc)
            stderr_task.cancel()
            raise

        rc = await proc.wait()
        stderr_task.cancel()
        log.warning("watcher exited rc=%s; restarting in %.1fs", rc, backoff)
        _stats["restarts"] += 1
        _stats["last_error"] = f"watcher exited rc={rc}"
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 300)


def _capture_session() -> None:
    global _session, _watcher_task, _renewer_task
    if _session is None:
        try:
            _session = server.request_context.session
            log.info("captured session ref")
        except Exception as e:
            log.warning("could not capture session: %s", e)
    if _renewer_task is None or _renewer_task.done():
        _renewer_task = asyncio.create_task(_run_watch_renewer())
        log.info("watch renewer task started")
    if _watcher_task is None or _watcher_task.done():
        _watcher_task = asyncio.create_task(_run_watcher())
        _stats["started_at"] = asyncio.get_event_loop().time()
        log.info("watcher task started")


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------
@server.list_tools()
async def list_tools() -> list[types.Tool]:
    # list_tools is called very early in the MCP handshake (before the client
    # invokes any actual tool), so this is the best reliable hook to capture
    # the session ref and kick off the watcher + renewer. Without this, the
    # watcher would not start until the first tool call — emails arriving in
    # the interim would be silently dropped.
    _capture_session()
    rule_fields = {
        "name": {"type": "string", "description": "Short human-readable label."},
        "from_regex": {"type": "string", "description": "Regex against From header (case-insensitive, re.search)."},
        "subject_regex": {"type": "string", "description": "Regex against Subject header."},
        "label_regex": {"type": "string", "description": "Regex against comma-joined Gmail label IDs (e.g. 'INBOX|IMPORTANT')."},
        "prompt": {
            "type": "string",
            "description": (
                "Prompt template injected into Claude when this rule matches. "
                "Placeholders: {from} {subject} {date} {snippet} {labels} {msg_id} {thread_id}. "
                "Unknown placeholders stay literal. Ignored if archive=true."
            ),
        },
        "archive": {
            "type": "boolean",
            "description": (
                "If true, silently archive matching emails to ~/.claude/gmail_channel/archive.ndjson "
                "instead of firing a channel notification. Use for self-sent mail, newsletters, bulk "
                "senders you want to keep a record of but not be woken up by. Query later via read_archive."
            ),
        },
    }
    return [
        types.Tool(
            name="status",
            description="Report watcher status: started_at, events forwarded, restarts, last_error.",
            inputSchema={"type": "object", "properties": {}},
        ),
        types.Tool(
            name="restart_watcher",
            description="Force-restart the underlying `gws gmail +watch` subprocess (e.g. after 7-day watch expiry).",
            inputSchema={"type": "object", "properties": {}},
        ),
        types.Tool(
            name="add_rule",
            description=(
                "Append a triage rule. A rule matches when all provided regexes match "
                "their respective fields (omitted fields = match-any). First-match-wins on "
                "the rule list; no rule = use the default prompt."
            ),
            inputSchema={
                "type": "object",
                "properties": rule_fields,
                "required": ["name", "prompt"],
            },
        ),
        types.Tool(
            name="list_rules",
            description="List triage rules in priority order (first match wins).",
            inputSchema={"type": "object", "properties": {}},
        ),
        types.Tool(
            name="remove_rule",
            description="Remove a rule by id.",
            inputSchema={
                "type": "object",
                "properties": {"id": {"type": "string"}},
                "required": ["id"],
            },
        ),
        types.Tool(
            name="reorder_rules",
            description="Reorder rules by supplying the full list of rule ids in desired priority order.",
            inputSchema={
                "type": "object",
                "properties": {
                    "ids": {"type": "array", "items": {"type": "string"}},
                },
                "required": ["ids"],
            },
        ),
        types.Tool(
            name="test_rule",
            description=(
                "Dry-run matching: given a synthetic email context, report which rule "
                "would fire and the rendered prompt."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "from": {"type": "string", "default": ""},
                    "subject": {"type": "string", "default": ""},
                    "labels": {"type": "string", "default": "INBOX"},
                    "snippet": {"type": "string", "default": ""},
                    "msg_id": {"type": "string", "default": "TESTID"},
                },
            },
        ),
        types.Tool(
            name="read_archive",
            description=(
                "Read archived emails (matched by an archive=true rule, never surfaced as channel "
                "events). Returns most-recent-first."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "limit": {"type": "integer", "default": 20, "description": "Max entries to return."},
                    "from_regex": {"type": "string", "description": "Optional regex filter on From header."},
                    "subject_regex": {"type": "string", "description": "Optional regex filter on Subject."},
                    "since": {"type": "string", "description": "ISO-8601 UTC timestamp; only entries at or after."},
                },
            },
        ),
    ]


@server.call_tool()
async def call_tool(name: str, args: dict[str, Any]) -> list[types.TextContent]:
    global _watcher_task, _renewer_task
    _capture_session()

    if name == "status":
        body = {
            "project": PROJECT,
            "labels": LABELS,
            "msg_format": MSG_FMT,
            "topic": _derive_topic() or "(unset)",
            "watcher_alive": _watcher_task is not None and not _watcher_task.done(),
            "renewer_alive": _renewer_task is not None and not _renewer_task.done(),
            "rules_file": str(RULES_FILE),
            "rule_count": len(load_rules()),
            **_stats,
        }
        return [types.TextContent(type="text", text=json.dumps(body, indent=2, default=str))]

    if name == "restart_watcher":
        if _watcher_task and not _watcher_task.done():
            _watcher_task.cancel()
            try:
                await asyncio.wait_for(_watcher_task, timeout=10)
            except (asyncio.CancelledError, asyncio.TimeoutError, Exception):
                pass
        _kill_orphan_watchers()
        _watcher_task = asyncio.create_task(_run_watcher())
        await _register_gmail_watch()
        return [types.TextContent(type="text", text="watcher restarted + gmail watch re-registered")]

    if name == "add_rule":
        rules = load_rules()
        rule_id = uuid.uuid4().hex[:8]
        rule: dict[str, Any] = {"id": rule_id, "name": args["name"], "prompt": args.get("prompt", "")}
        if args.get("archive"):
            rule["archive"] = True
        for field in ("from_regex", "subject_regex", "label_regex"):
            val = args.get(field)
            if val:
                try:
                    re.compile(val)
                except re.error as e:
                    return [types.TextContent(type="text", text=f"Bad regex in {field!r}: {e}")]
                rule[field] = val
        rules.append(rule)
        save_rules(rules)
        mode = "archive-only" if rule.get("archive") else "notify"
        return [types.TextContent(type="text", text=f"Added rule id={rule_id} '{rule['name']}' [{mode}] (now {len(rules)} rule(s)).")]

    if name == "list_rules":
        rules = load_rules()
        if not rules:
            return [types.TextContent(type="text", text="No rules. Default prompt used for every email.")]
        lines = []
        for i, r in enumerate(rules):
            crit = " / ".join(
                f"{k}={r[k]!r}" for k in ("from_regex", "subject_regex", "label_regex") if r.get(k)
            ) or "(match-any)"
            if r.get("archive"):
                body = "archive only (no notification)"
            else:
                body = "prompt: " + r.get("prompt", "").replace("\n", " ")[:80]
            lines.append(f"#{i} id={r['id']}  name={r['name']}\n  match: {crit}\n  {body}")
        return [types.TextContent(type="text", text="\n\n".join(lines))]

    if name == "remove_rule":
        rules = load_rules()
        target = args["id"]
        new = [r for r in rules if r["id"] != target]
        save_rules(new)
        return [types.TextContent(type="text", text=f"Removed {len(rules) - len(new)} rule(s) with id={target}.")]

    if name == "reorder_rules":
        rules = load_rules()
        desired = args["ids"]
        by_id = {r["id"]: r for r in rules}
        missing = [i for i in desired if i not in by_id]
        extra = [r["id"] for r in rules if r["id"] not in desired]
        if missing or extra:
            return [types.TextContent(
                type="text",
                text=f"ids must exactly cover existing rules. missing={missing} extra={extra}",
            )]
        save_rules([by_id[i] for i in desired])
        return [types.TextContent(type="text", text=f"Reordered {len(desired)} rule(s).")]

    if name == "read_archive":
        limit = int(args.get("limit", 20))
        from_re = re.compile(args["from_regex"], re.I) if args.get("from_regex") else None
        subj_re = re.compile(args["subject_regex"], re.I) if args.get("subject_regex") else None
        since = args.get("since")
        entries: list[dict[str, Any]] = []
        if ARCHIVE_FILE.exists():
            with ARCHIVE_FILE.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        e = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if since and e.get("ts", "") < since:
                        continue
                    if from_re and not from_re.search(e.get("from", "")):
                        continue
                    if subj_re and not subj_re.search(e.get("subject", "")):
                        continue
                    entries.append(e)
        entries.reverse()  # most-recent-first
        entries = entries[:limit]
        if not entries:
            return [types.TextContent(type="text", text=f"no archived entries match (file: {ARCHIVE_FILE})")]
        return [types.TextContent(type="text", text=json.dumps({"count": len(entries), "entries": entries}, indent=2, ensure_ascii=False))]

    if name == "test_rule":
        ctx = {
            "from": args.get("from", ""),
            "subject": args.get("subject", ""),
            "date": "",
            "snippet": args.get("snippet", ""),
            "labels": args.get("labels", "INBOX"),
            "msg_id": args.get("msg_id", "TESTID"),
            "thread_id": "TESTTHREAD",
        }
        prompt, matched = _resolve_prompt(ctx)
        return [types.TextContent(
            type="text",
            text=json.dumps({"matched_rule": matched or "(default)", "rendered_prompt": prompt}, indent=2),
        )]

    return [types.TextContent(type="text", text=f"Unknown tool: {name}")]


INSTRUCTIONS = (
    "You are connected to a gmail channel.\n\n"
    "New-email events arrive as <channel source=\"gmail\" msg_id=\"...\" subject=\"...\" from=\"...\" rule=\"...\">. "
    "The content is a triage prompt — either the default (spam/security/actionable/archivable) or a custom "
    "prompt from the first matching rule (see list_rules). Treat it as an instruction and act.\n\n"
    "Manage the rule list with add_rule, list_rules, remove_rule, reorder_rules, test_rule. "
    "Rules are first-match-wins; omitted regex fields match anything; prompts may reference "
    "{from} {subject} {date} {snippet} {labels} {msg_id} {thread_id}. "
    "Use status and restart_watcher to inspect/reset the underlying `gws gmail +watch`."
)


async def main() -> None:
    async with stdio_server() as (read_stream, write_stream):
        init_opts = server.create_initialization_options(
            notification_options=NotificationOptions(),
            experimental_capabilities={"claude/channel": {}},
        )
        init_opts.instructions = INSTRUCTIONS
        await server.run(read_stream, write_stream, init_opts)


def _cli() -> None:
    asyncio.run(main())


if __name__ == "__main__":
    _cli()
