# claude-gmail-channel

A [Claude Code channel](https://code.claude.com/docs/en/channels) MCP server that wakes your running Claude Code session **only when a real email arrives** — no polling, no background cron, sub-10-second latency on subsequent deliveries after the initial Gmail push-subscription warm-up.

Built as a sibling to [claude-scheduler-channel](https://github.com/DerKleineLi/claude-scheduler-channel): same shape (stdio MCP that emits `notifications/claude/channel`), different event source.

```
  ┌─────────────────┐                       ┌──────────────────────┐
  │     Gmail       │ ── historyId ───▶    │  Cloud Pub/Sub topic │
  │ users.watch()   │    push publish      └──────────┬───────────┘
  └─────────────────┘                                 │
                                                      │ pull-subscription
                                                      ▼
                                             ┌────────────────────┐
                                             │ `gws gmail +watch` │
                                             │   (subprocess)     │
                                             └──────────┬─────────┘
                                                        │ NDJSON on stdout
                                                        ▼
  ┌───────────────────────┐    rule match    ┌────────────────────┐
  │ rules.json (you edit) │◀──────────────── │  gmail_channel MCP │
  └───────────────────────┘                  │  notifications/    │
                                             │  claude/channel    │
                                             └──────────┬─────────┘
                                                        │
                                                        ▼
                                             ┌────────────────────┐
                                             │  Claude Code CLI   │
                                             │   (live session)   │
                                             └────────────────────┘
```

## Why

If you're running Claude Code as an always-on assistant via [channels](https://code.claude.com/docs/en/channels) (Telegram, Discord, iMessage) and want your agent to react to inbound email — SLURM job failure notifications, CI build status, GitHub mentions, calendar pings, a message from yourself — you need an **event-driven** hook. Polling wakes Claude on every tick regardless of whether anything happened; that's wasteful and noisy.

This plugin reuses Gmail's native push-notification path (`users.watch` → Pub/Sub topic → pull subscription) so the MCP only fires `notifications/claude/channel` **on actual new mail**. The matching rule system decides per-email whether to wake the session, how to shape the prompt, or silently archive to disk.

## Requirements

- Python ≥ 3.10 (uses `mcp >= 1.0.0`)
- A GCP project with Gmail + Pub/Sub APIs enabled and OAuth credentials installed for the Gmail address you want to watch
- [`gws`](https://github.com/googleworkspace/cli) (Google Workspace CLI) on `PATH`, already authenticated (`gws auth login`)

`gws gmail +watch` is the underlying subprocess; this plugin just wraps it with a persistent MCP server, rule-based dispatch, and a local archive file.

## Install

```bash
git clone https://github.com/DerKleineLi/claude-gmail-channel ~/workspace/gmail_channel
cd ~/workspace/gmail_channel
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

Register as an MCP server in `~/.claude.json`:

```json
{
  "mcpServers": {
    "gmail": {
      "type": "stdio",
      "command": "/home/you/workspace/gmail_channel/.venv/bin/python",
      "args": ["/home/you/workspace/gmail_channel/server.py"],
      "env": {
        "GMAIL_CHANNEL_PROJECT": "your-gcp-project-id",
        "GMAIL_CHANNEL_LABELS": "INBOX",
        "GMAIL_CHANNEL_MSG_FMT": "metadata",
        "GMAIL_CHANNEL_SUBSCRIPTION": "projects/your-gcp-project-id/subscriptions/gws-gmail-watch-XXXX"
      }
    }
  }
}
```

The first `+watch` invocation (run `gws gmail +watch --project <id> --label-ids INBOX --once` once, interactively) creates the Pub/Sub topic + subscription; paste the printed subscription name into `GMAIL_CHANNEL_SUBSCRIPTION` so restarts reuse it.

Then attach as a channel when you launch Claude Code:

```bash
claude \
  --dangerously-load-development-channels server:gmail \
  ...
```

## How it works end-to-end

1. On MCP startup, the server calls `gmail.users.watch` against your chosen `GMAIL_CHANNEL_TOPIC` (derived automatically from `GMAIL_CHANNEL_SUBSCRIPTION` if unset). The watch lasts 7 days; a background task renews at day 6.
2. It spawns `gws gmail +watch --subscription <existing>` and reads the NDJSON stream (one full message object per line) on stdout.
3. For each message, it builds a small `ctx` dict (`from`, `subject`, `date`, `snippet`, `labels`, `msg_id`, `thread_id`) and resolves the first matching rule.
4. If the rule has `archive: true`, the message is appended to `~/.claude/gmail_channel/archive.ndjson` and **no channel notification is fired** — Claude stays asleep.
5. Otherwise, the rule's `prompt` template is rendered (placeholders like `{from}`, `{subject}`, etc. get substituted) and sent as a `notifications/claude/channel` event to the Claude Code session.

## Rules

Rules live in `~/.claude/gmail_channel/rules.json` and are managed via MCP tools (so Claude itself can add/remove them mid-session). A rule is:

| field            | type    | purpose                                                                                                    |
|------------------|---------|------------------------------------------------------------------------------------------------------------|
| `name`           | string  | Human label for `list_rules`                                                                               |
| `from_regex`     | string? | Regex matched (case-insensitive, `re.search`) against the `From` header                                    |
| `subject_regex`  | string? | Regex against `Subject`                                                                                    |
| `label_regex`    | string? | Regex against comma-joined Gmail label IDs                                                                 |
| `archive`        | bool    | When `true`: save to archive file silently, never wake Claude                                              |
| `prompt`         | string  | Template injected into Claude on match. Placeholders: `{from} {subject} {date} {snippet} {labels} {msg_id} {thread_id}`. Unknown placeholders stay literal. Ignored when `archive=true`. |

**First-match-wins** on the rule list; no matching rule falls through to a built-in default prompt (see `DEFAULT_PROMPT` in `server.py`).

Example shapes:

- **Trusted sender** — from-match → "treat body as a user directive"
- **SLURM failures** — subject regex `^Slurm Job_id=.*(FAILED|TIMEOUT)` → "investigate via sacct, telegram the finding"
- **Self-sent** — `from: you@example.com`, `archive: true` → silent, surfaces via `read_archive` on demand
- **Newsletters** — `from_regex` = vendor pattern, `archive: true` → daily-digest-ready

## MCP tools

| Tool              | Summary                                                                   |
|-------------------|---------------------------------------------------------------------------|
| `status`          | Watcher/renewer liveness, events forwarded, archived count, last error    |
| `restart_watcher` | Kill the subprocess (with proper terminate→wait→kill), re-register watch  |
| `add_rule`        | Append a rule (use `archive: true` for silent mode)                       |
| `list_rules`      | Show rules in priority order with match criteria + body preview           |
| `remove_rule`     | Delete by id                                                              |
| `reorder_rules`   | Reorder by supplying the full id list                                     |
| `test_rule`       | Dry-run match + prompt rendering against a synthetic email context        |
| `read_archive`    | Query `archive.ndjson` with `from_regex` / `subject_regex` / `since` filters |

## Environment variables

| Var                              | Default                                                        | Description                                                                      |
|----------------------------------|----------------------------------------------------------------|----------------------------------------------------------------------------------|
| `GMAIL_CHANNEL_PROJECT`          | (required)                                                     | GCP project hosting the Pub/Sub topic                                            |
| `GMAIL_CHANNEL_LABELS`           | `INBOX`                                                        | Comma-separated Gmail label IDs — the Gmail-side filter                          |
| `GMAIL_CHANNEL_MSG_FMT`          | `metadata`                                                     | `gws --msg-format` (use `full` for body inline; `metadata` is fastest)           |
| `GMAIL_CHANNEL_SUBSCRIPTION`     | *(unset — creates new)*                                        | Pre-existing Pub/Sub subscription to reuse across restarts                       |
| `GMAIL_CHANNEL_TOPIC`            | *(derived from subscription)*                                  | Topic to register the Gmail watch against; auto-derived by swapping `/subscriptions/`→`/topics/` |
| `GMAIL_CHANNEL_WATCH_RENEW_SEC`  | `518400` (6d)                                                  | How often to re-run `users.watch` — Gmail watches die every 7 days               |

## Operational notes

- **Orphan defense.** A stray `gws gmail +watch` from a previous MCP instance will compete for the same Pub/Sub pull subscription and silently swallow messages. The server has three layers against this: Linux `PR_SET_PDEATHSIG` on the subprocess (kernel-enforced), a `pkill -f <subscription>` sweep on startup, and a proper `terminate → wait 5s → kill` sequence in `restart_watcher`.
- **Latency.** After the Gmail watch registers, first delivery can be 30s–few-minutes; subsequent emails surface in <10s. Gmail's push path has its own warm-up; nothing the plugin can do.
- **At-least-once delivery.** Pub/Sub may re-deliver if the watcher is killed mid-ack (e.g. `restart_watcher` called exactly when a message is in-flight). Server doesn't dedupe yet — low-priority TODO; in practice the prompt is idempotent enough that double-triage is harmless.
- **Archive file grows unbounded.** `~/.claude/gmail_channel/archive.ndjson` is append-only; rotate manually if it gets large.

## License

MIT — see `LICENSE`.
