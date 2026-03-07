# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

Install dependency:
```bash
pip install discord.py
```

Run the bot (one-time server setup):
```bash
python discord_setup.py
```

Required `.env` file:
```
DISCORD_TOKEN=your_token_here
GUILD_ID=your_guild_id_here
```

## Architecture

This is a **one-shot setup bot** — it runs once, builds the entire Discord server structure, then exits. It is not a persistent bot with ongoing commands.

### Key functions in `discord_setup.py`

- `load_members_from_csv(track, csv_file)` — reads participant CSV files, filters out dropouts (`탈락`), returns member list. Not yet wired into channel/role assignment.
- `setup_track(guild, track_name, num_groups, track_key)` — creates a category, roles (track-wide, leader, per-group), and channels (announcements, task verification, leader-only, per-team text+voice) for one track.
- `on_ready()` — iterates over all 6 configured tracks, calls `setup_track()` for each with rate-limit delays, then calls `await bot.close()`.

### Tracks (defined in `on_ready`)
6 tracks: 크리에이터, 빌더 기초, 빌더 심화, 세일즈 실전, AI 에이전트, 앱 개발

### Configuration constants (top of file)
- `GENERATION` — cohort label (e.g. `"7기"`), used in role names
- `CSV_FILES` — maps track keys to CSV paths for member import
- `TRACK_EXTRA_CHANNELS` — defines task-verification channel names per track type

### Permissions model
Each channel uses `discord.PermissionOverwrite` to restrict visibility:
- `@everyone` denied view
- Track role granted view
- Specific roles (leader, group) granted additional access per channel type
