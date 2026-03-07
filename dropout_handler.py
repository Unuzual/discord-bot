#!/usr/bin/env python3
"""
탈락자 자동 처리 스크립트

Usage:
  python dropout_handler.py "이름" [--track "트랙명"] [--generation 7기] [--dry-run]
  python dropout_handler.py rollback "이름"
  python dropout_handler.py report daily|weekly|season [--season 7기]
"""

from __future__ import annotations

import json
import os
import sys
import argparse
from collections import Counter
from datetime import datetime, timedelta, date
from pathlib import Path

import requests
import discord
from discord.ext import commands
from dotenv import load_dotenv

load_dotenv()

# ─── 환경 변수 ───
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
GUILD_ID      = int(os.getenv("GUILD_ID"))
NOTION_API_KEY = os.getenv("NOTION_API_KEY")

# ─── Notion DB IDs ───
MEMBER_DB_ID       = "2df6400e9268805abb5efe6a2ad848e6"  # 멤버 마스터 DB
TRACK_SIGNUP_DB_ID = "2e16400e926880faa394ee108237265b"  # 트랙 신청 DB
TRACK_GROUP_DB_ID  = "2e06400e926880708decce6525f0bd82"  # 트랙/조 DB

# ─── 로컬 파일 경로 ───
ROLLBACK_DIR = Path("rollback")
DROPOUT_LOG  = Path("dropout_log.json")

# ─── dry-run 플래그 (process_dropout 진입 전에 설정) ───
DRY_RUN = False

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_API_KEY}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}


# ═══════════════════════════════════════════════════════
# Notion 공통 유틸
# ═══════════════════════════════════════════════════════

_TIMEOUT = 15  # 초

def _get(url: str) -> dict:
    res = requests.get(url, headers=NOTION_HEADERS, timeout=_TIMEOUT)
    res.raise_for_status()
    return res.json()

def _post(url: str, payload: dict) -> dict:
    res = requests.post(url, headers=NOTION_HEADERS, json=payload, timeout=_TIMEOUT)
    res.raise_for_status()
    return res.json()

def _patch(url: str, payload: dict) -> dict:
    res = requests.patch(url, headers=NOTION_HEADERS, json=payload, timeout=_TIMEOUT)
    res.raise_for_status()
    return res.json()

def _delete(url: str) -> dict:
    res = requests.delete(url, headers=NOTION_HEADERS, timeout=_TIMEOUT)
    res.raise_for_status()
    return res.json()

def prop_text(props: dict, key: str) -> str:
    """rich_text 또는 title 속성에서 텍스트 추출"""
    prop = props.get(key, {})
    for field in ("rich_text", "title"):
        items = prop.get(field, [])
        if items:
            return items[0].get("plain_text", "").strip()
    return ""

def prop_select(props: dict, key: str) -> str | None:
    sel = props.get(key, {}).get("select")
    return sel["name"] if sel else None

def prop_multi_select(props: dict, key: str) -> list[str]:
    return [t["name"] for t in props.get(key, {}).get("multi_select", [])]

def prop_status(props: dict, key: str) -> str | None:
    s = props.get(key, {}).get("status")
    return s["name"] if s else None


# ═══════════════════════════════════════════════════════
# 멤버 마스터 DB
# ═══════════════════════════════════════════════════════

def member_find(name: str) -> dict | None:
    """이름(rich_text)으로 멤버 마스터 DB 검색"""
    data = _post(
        f"https://api.notion.com/v1/databases/{MEMBER_DB_ID}/query",
        {"filter": {"property": "이름", "rich_text": {"equals": name}}},
    )
    results = data.get("results", [])
    return results[0] if results else None


def member_update_dropout(page_id: str, original_tracks: list[str], dropout_track: str) -> None:
    """트랙 목록에서 dropout_track만 제거"""
    new_tracks = [t for t in original_tracks if t != dropout_track]
    if DRY_RUN:
        remaining = new_tracks or ["(없음)"]
        print(f"  [DRY-RUN] [멤버 마스터 DB] 트랙 변경: {original_tracks} → {remaining}")
        return
    _patch(
        f"https://api.notion.com/v1/pages/{page_id}",
        {"properties": {"트랙": {"multi_select": [{"name": t} for t in new_tracks]}}},
    )
    print(f"  ✅ [멤버 마스터 DB] 트랙에서 '{dropout_track}' 제거")


def member_add_memo(page_id: str, generation: str, track: str) -> str:
    """페이지 본문에 탈락 메모 callout 블록 추가, block_id 반환"""
    today = datetime.now().strftime("%Y-%m-%d")
    text = f"{generation} {track} 탈락({today})"
    if DRY_RUN:
        print(f"  [DRY-RUN] [멤버 마스터 DB] 메모 추가 예정: '🚫 {text}'")
        return ""
    result = _post(
        f"https://api.notion.com/v1/blocks/{page_id}/children",
        {
            "children": [{
                "object": "block",
                "type": "callout",
                "callout": {
                    "rich_text": [{"type": "text", "text": {"content": text}}],
                    "icon": {"emoji": "🚫"},
                    "color": "red_background",
                },
            }]
        },
    )
    block_id = result["results"][0]["id"]
    print(f"  ✅ [멤버 마스터 DB] 메모 추가: '{text}'")
    return block_id


def member_rollback(page_id: str, original_tracks: list[str], original_status: str, memo_block_id: str | None) -> None:
    _patch(
        f"https://api.notion.com/v1/pages/{page_id}",
        {
            "properties": {
                "트랙": {"multi_select": [{"name": t} for t in original_tracks]},
            }
        },
    )
    if memo_block_id:
        try:
            _delete(f"https://api.notion.com/v1/blocks/{memo_block_id}")
        except Exception:
            print("  ⚠️  메모 블록 삭제 실패 (이미 삭제됐을 수 있음)")
    print("  ✅ [멤버 마스터 DB] 복구 완료")


# ═══════════════════════════════════════════════════════
# 트랙 신청 DB
# ═══════════════════════════════════════════════════════

SIGNUP_TRACK_COLS = ["월요일 트랙", "화요일 트랙", "수요일 트랙"]


def signup_find(name: str) -> dict | None:
    """이름(title)으로 트랙 신청 DB 검색"""
    data = _post(
        f"https://api.notion.com/v1/databases/{TRACK_SIGNUP_DB_ID}/query",
        {"filter": {"property": "이름", "title": {"equals": name}}},
    )
    results = data.get("results", [])
    return results[0] if results else None


def signup_clear_track(page_id: str, props: dict, dropout_track: str) -> dict:
    """
    월/화/수 트랙 속성 중 dropout_track과 일치하는 값을 null로 제거.
    원본 값 dict 반환 (롤백용).
    """
    original = {}
    updates = {}
    for col in SIGNUP_TRACK_COLS:
        val = prop_select(props, col)
        original[col] = val
        if val and val.strip() == dropout_track.strip():
            updates[col] = {"select": None}

    if updates:
        if DRY_RUN:
            for col in updates:
                print(f"  [DRY-RUN] [트랙 신청 DB] '{col}' 선택값 제거 예정: '{original[col]}' → (없음)")
        else:
            _patch(f"https://api.notion.com/v1/pages/{page_id}", {"properties": updates})
            print(f"  ✅ [트랙 신청 DB] 트랙 선택값 제거: {', '.join(updates.keys())}")
    else:
        print(f"  ℹ️  [트랙 신청 DB] 제거할 트랙 선택값 없음")

    return original


def signup_rollback(page_id: str, original_cols: dict) -> None:
    updates = {
        col: {"select": {"name": val} if val else None}
        for col, val in original_cols.items()
    }
    _patch(f"https://api.notion.com/v1/pages/{page_id}", {"properties": updates})
    print("  ✅ [트랙 신청 DB] 복구 완료")


# ═══════════════════════════════════════════════════════
# 트랙/조 DB
# ═══════════════════════════════════════════════════════

def group_find_rows(name: str) -> list[dict]:
    """이름(title)으로 트랙/조 DB 검색 (동명이인 대비 여러 행 반환)"""
    data = _post(
        f"https://api.notion.com/v1/databases/{TRACK_GROUP_DB_ID}/query",
        {"filter": {"property": "이름", "title": {"equals": name}}},
    )
    return data.get("results", [])


def group_archive_rows(rows: list[dict]) -> list[str]:
    """행 아카이브(논리 삭제), 아카이브된 page_id 목록 반환"""
    if not rows:
        print(f"  ℹ️  [트랙/조 DB] 해당 멤버 없음 - 스킵")
        return []
    if DRY_RUN:
        print(f"  [DRY-RUN] [트랙/조 DB] {len(rows)}개 행 삭제 예정")
        return []
    ids = []
    for row in rows:
        _patch(f"https://api.notion.com/v1/pages/{row['id']}", {"archived": True})
        ids.append(row["id"])
    print(f"  ✅ [트랙/조 DB] {len(ids)}개 행 삭제")
    return ids


def group_rollback_rows(page_ids: list[str]) -> None:
    for pid in page_ids:
        _patch(f"https://api.notion.com/v1/pages/{pid}", {"archived": False})
    print(f"  ✅ [트랙/조 DB] {len(page_ids)}개 행 복구")


# ═══════════════════════════════════════════════════════
# 롤백 파일
# ═══════════════════════════════════════════════════════

def rollback_save(name: str, snapshot: dict) -> Path:
    ROLLBACK_DIR.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = ROLLBACK_DIR / f"{name}_{ts}.json"
    path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2))
    print(f"\n💾 롤백 파일 저장: {path}")
    return path


def rollback_load_latest(name: str) -> tuple[Path, dict] | tuple[None, None]:
    if not ROLLBACK_DIR.exists():
        return None, None
    files = sorted(ROLLBACK_DIR.glob(f"{name}_*.json"), reverse=True)
    if not files:
        return None, None
    path = files[0]
    return path, json.loads(path.read_text())


def do_rollback(name: str) -> None:
    path, snapshot = rollback_load_latest(name)
    if not snapshot:
        print(f"❌ '{name}'의 롤백 파일이 없습니다.")
        return

    print(f"🔄 롤백 시작: {name}  (파일: {path.name})\n")

    mb = snapshot.get("member_db", {})
    if mb.get("page_id"):
        member_rollback(mb["page_id"], mb["original_tracks"], mb["original_status"], mb.get("memo_block_id"))

    ts = snapshot.get("track_signup_db", {})
    if ts.get("page_id"):
        signup_rollback(ts["page_id"], ts["original_cols"])

    tg = snapshot.get("track_group_db", [])
    if tg:
        group_rollback_rows(tg)

    path.unlink()
    print(f"\n✅ 롤백 완료. 파일 삭제: {path.name}")


# ═══════════════════════════════════════════════════════
# 탈락 로그 & 리포트
# ═══════════════════════════════════════════════════════

def log_append(name: str, track: str, generation: str) -> None:
    log: list[dict] = []
    if DROPOUT_LOG.exists():
        log = json.loads(DROPOUT_LOG.read_text())
    log.append({
        "date": datetime.now().strftime("%Y-%m-%d"),
        "name": name,
        "track": track,
        "generation": generation,
    })
    DROPOUT_LOG.write_text(json.dumps(log, ensure_ascii=False, indent=2))


def do_report(mode: str, season: str | None = None) -> None:
    if not DROPOUT_LOG.exists():
        print("📋 탈락 처리 기록이 없습니다.")
        return

    log: list[dict] = json.loads(DROPOUT_LOG.read_text())
    today = date.today()

    if mode == "daily":
        entries = [e for e in log if e["date"] == today.strftime("%Y-%m-%d")]
        title = f"일별 리포트 ({today})"
    elif mode == "weekly":
        cutoff = (today - timedelta(days=7)).strftime("%Y-%m-%d")
        entries = [e for e in log if e["date"] >= cutoff]
        title = "주간 리포트 (최근 7일)"
    elif mode == "season":
        if not season:
            print("❌ --season 옵션이 필요합니다. 예: --season 7기")
            sys.exit(1)
        entries = [e for e in log if e["generation"] == season]
        title = f"기수별 리포트 ({season})"
    else:
        print(f"❌ 알 수 없는 모드: {mode}")
        sys.exit(1)

    print(f"\n📅 {title}")
    print("─" * 52)
    if not entries:
        print("  (데이터 없음)\n")
        return

    print(f"  총 탈락자 수: {len(entries)}명\n")
    print(f"  {'이름':<12} {'트랙':<24} {'날짜'}")
    print("  " + "─" * 48)
    for e in entries:
        print(f"  {e['name']:<12} {e['track']:<24} {e['date']}")

    print("\n  [트랙별 집계]")
    for track, cnt in Counter(e["track"] for e in entries).most_common():
        print(f"  {track}: {cnt}명")
    print()


# ═══════════════════════════════════════════════════════
# Discord
# ═══════════════════════════════════════════════════════

intents = discord.Intents.default()
intents.members = True
bot = commands.Bot(command_prefix="!", intents=intents)
_ctx: dict = {}


def _track_short(track_name: str) -> str:
    return track_name.replace(" 트랙", "").replace(" ", "-")


async def _discord_find_member(guild: discord.Guild, user_id: str, discord_id: str) -> discord.Member | None:
    # 1) 숫자 ID로 직접 조회
    if user_id and user_id.isdigit():
        try:
            return await guild.fetch_member(int(user_id))
        except discord.NotFound:
            pass

    # 2) username으로 gateway 검색 (Members intent 없이도 동작)
    if discord_id:
        username = discord_id.lstrip("@")
        results = await guild.query_members(query=username, limit=5)
        for m in results:
            if m.name == username or m.display_name == username:
                return m

    return None


def _discord_find_leader(guild, leader_role, group_role, exclude):
    for m in guild.members:
        if m == exclude or leader_role not in m.roles:
            continue
        if group_role and group_role not in m.roles:
            continue
        return m
    return None


@bot.event
async def on_ready():
    print(f"🤖 봇 로그인: {bot.user}\n")
    guild = bot.get_guild(GUILD_ID)
    if not guild:
        print("❌ Discord 서버를 찾을 수 없습니다.")
        await bot.close()
        return

    name         = _ctx["name"]
    generation   = _ctx["generation"]
    dropout_track = _ctx["dropout_track"]
    discord_id   = _ctx.get("discord_id", "")
    user_id      = _ctx.get("user_id", "")
    group_str    = _ctx.get("group_str", "")

    discord_member = await _discord_find_member(guild, user_id, discord_id)
    if not discord_member:
        print(f"  ⚠️  Discord 멤버를 찾을 수 없습니다 (ID: {discord_id})")
        await bot.close()
        return

    print(f"  Discord 멤버: {discord_member.display_name} ({discord_member.name})\n")

    # 역할 목록 조사 (dry-run 여부 무관하게 조회)
    ts = _track_short(dropout_track)
    role_names = [f"{ts}-{generation}", f"{ts}-{generation}-조장"]
    if group_str:
        role_names.append(f"{ts}-{generation}-{group_str}조")
    roles_to_remove = [
        rn for rn in role_names
        if (r := discord.utils.get(guild.roles, name=rn)) and r in discord_member.roles
    ]

    # 조장 조회
    leader_role = discord.utils.get(guild.roles, name=f"{ts}-{generation}-조장")
    group_role  = discord.utils.get(guild.roles, name=f"{ts}-{generation}-{group_str}조") if group_str else None
    leader      = _discord_find_leader(guild, leader_role, group_role, discord_member) if leader_role else None

    if DRY_RUN:
        print(f"  [DRY-RUN] [Discord] 탈락자 DM 전송 예정 → {discord_member.display_name} ({generation} {dropout_track} 탈락 안내)")
        if roles_to_remove:
            print(f"  [DRY-RUN] [Discord] 역할 제거 예정: {', '.join(roles_to_remove)}")
        else:
            print(f"  [DRY-RUN] [Discord] 제거할 역할 없음")
        if leader:
            print(f"  [DRY-RUN] [Discord] 조장 DM 전송 예정 → {leader.display_name}")
        else:
            print(f"  [DRY-RUN] [Discord] 조장을 찾을 수 없음")
        await bot.close()
        return

    # 탈락자 DM
    try:
        await discord_member.send(
            f"안녕하세요! ASC 커뮤니티 매니저 유주입니다 🙌\n\n"
            f"이번 {generation} {dropout_track} 과제 미제출로 확인되어, 아쉽게도 해당 트랙에서 탈락 처리될 예정임을 안내드립니다.\n\n"
            f"혹시 제출하셨는데 착오가 있는 것 같다면 편하게 말씀해 주세요!\n"
            f"바로 확인해 드리겠습니다 🙏\n\n"
            f"해당 트랙은 다음 차수에도 언제든지 다시 참여하실 수 있으니, 다음 기회에 꼭 함께했으면 합니다!\n\n"
            f"감사합니다 😊"
        )
        print("  ✅ [Discord] 탈락자 DM 전송")
    except discord.Forbidden:
        print("  ⚠️  탈락자 DM 실패 (DM 차단)")

    # 역할 제거
    for rn in roles_to_remove:
        role = discord.utils.get(guild.roles, name=rn)
        await discord_member.remove_roles(role)
    if roles_to_remove:
        print(f"  ✅ [Discord] 역할 제거: {', '.join(roles_to_remove)}")
    else:
        print(f"  ℹ️  [Discord] 제거할 역할 없음")

    # 조장 DM
    if leader:
        try:
            await leader.send(
                f"안녕하세요 조장님. 커뮤니티 매니저 유주입니다. "
                f"{dropout_track} {generation} {group_str}조의 {name}님께서 과제 미제출로 트랙 탈락하셨음을 알립니다~! "
                f"{name}님은 채널에서 제외되셔서 더이상 조별 모임 등은 참가가 불가능하니 이 점 참고해 주세요~! 감사합니다."
            )
            print(f"  ✅ [Discord] 조장 DM 전송: {leader.display_name}")
        except discord.Forbidden:
            print(f"  ⚠️  조장 DM 실패: {leader.display_name} (DM 차단)")
    else:
        print(f"  ⚠️  조장을 찾을 수 없음")

    await bot.close()


# ═══════════════════════════════════════════════════════
# 메인 탈락 처리 흐름
# ═══════════════════════════════════════════════════════

def process_dropout(name: str, dropout_track: str | None, generation: str) -> None:
    if DRY_RUN:
        print("=" * 52)
        print("  DRY-RUN 모드  |  실제 변경 없이 처리 내용만 표시")
        print("=" * 52)
    print(f"🚨 탈락 처리 시작: {name} ({generation})\n")

    # ── 멤버 마스터 DB 조회 ──
    print("🔍 Notion 멤버 마스터 DB 조회 중...")
    member_page = member_find(name)
    if not member_page:
        print(f"❌ '{name}'을(를) 멤버 마스터 DB에서 찾을 수 없습니다.")
        sys.exit(1)

    props = member_page["properties"]
    original_tracks = prop_multi_select(props, "트랙")
    discord_id_str  = prop_text(props, "디스코드 ID")
    user_id_str     = prop_text(props, "사용자 ID")
    group_str       = prop_text(props, "조").replace("조", "").strip()

    # dropout_track 미지정 시 자동 결정
    if not dropout_track:
        if len(original_tracks) == 1:
            dropout_track = original_tracks[0]
        elif len(original_tracks) == 0:
            print(f"❌ '{name}'의 트랙 정보가 없습니다.")
            sys.exit(1)
        else:
            print(f"⚠️  여러 트랙이 있습니다: {original_tracks}")
            print(f"   --track 옵션으로 탈락 트랙을 지정해주세요.")
            sys.exit(1)

    print(f"  이름: {name} | 트랙: {dropout_track} | 조: {group_str or '없음'}\n")

    # ── 롤백 스냅샷 초기화 ──
    snapshot: dict = {
        "name": name,
        "track": dropout_track,
        "generation": generation,
        "timestamp": datetime.now().isoformat(),
        "member_db": {
            "page_id": member_page["id"],
            "original_tracks": original_tracks,
            "memo_block_id": None,
        },
        "track_signup_db": {},
        "track_group_db": [],
    }

    # ── 1. 멤버 마스터 DB 업데이트 ──
    print("📝 멤버 마스터 DB 처리 중...")
    member_update_dropout(member_page["id"], original_tracks, dropout_track)
    block_id = member_add_memo(member_page["id"], generation, dropout_track)
    snapshot["member_db"]["memo_block_id"] = block_id

    # ── 2. 트랙 신청 DB 처리 ──
    print("\n📝 트랙 신청 DB 처리 중...")
    signup_page = signup_find(name)
    if signup_page:
        orig_cols = signup_clear_track(signup_page["id"], signup_page["properties"], dropout_track)
        snapshot["track_signup_db"] = {
            "page_id": signup_page["id"],
            "original_cols": orig_cols,
        }
    else:
        print(f"  ℹ️  트랙 신청 DB에서 '{name}' 항목 없음")

    # ── 3. 트랙/조 DB 처리 ──
    print("\n📝 트랙/조 DB 처리 중...")
    group_rows = group_find_rows(name)
    archived_ids = group_archive_rows(group_rows)
    snapshot["track_group_db"] = archived_ids

    # ── 롤백 파일 저장 / 탈락 로그 기록 (dry-run 시 스킵) ──
    if DRY_RUN:
        print("\n  [DRY-RUN] 롤백 파일 저장 스킵")
        print("  [DRY-RUN] 탈락 로그 기록 스킵")
    else:
        rollback_save(name, snapshot)
        log_append(name, dropout_track, generation)

    # ── 4. Discord 처리 ──
    print("\n🎮 Discord 처리 중...")
    _ctx.update({
        "name": name,
        "generation": generation,
        "dropout_track": dropout_track,
        "discord_id": discord_id_str,
        "user_id": user_id_str,
        "group_str": group_str,
    })
    bot.run(DISCORD_TOKEN)

    if DRY_RUN:
        print("\n✅ DRY-RUN 완료. 실제 변경된 내용은 없습니다.")
    else:
        print("\n🎉 탈락 처리 모두 완료!")


# ═══════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════

if __name__ == "__main__":
    # "이름"만 입력 시 dropout 서브커맨드로 자동 처리
    if len(sys.argv) >= 2 and sys.argv[1] not in ("dropout", "rollback", "report", "-h", "--help"):
        sys.argv.insert(1, "dropout")

    parser = argparse.ArgumentParser(description="탈락자 자동 처리 스크립트")
    sub = parser.add_subparsers(dest="command")

    # dropout
    p_drop = sub.add_parser("dropout", help="탈락 처리")
    p_drop.add_argument("name", help="탈락자 이름 (Notion 멤버 마스터 DB 기준)")
    p_drop.add_argument("--track", help="탈락 트랙명 (트랙이 하나면 자동 선택)")
    p_drop.add_argument("--generation", default="7기", help="기수 (기본: 7기)")
    p_drop.add_argument("--dry-run", action="store_true", help="실제 변경 없이 처리 내용만 미리보기")

    # rollback
    p_rb = sub.add_parser("rollback", help="탈락 처리 롤백")
    p_rb.add_argument("name", help="롤백할 멤버 이름")

    # report
    p_rep = sub.add_parser("report", help="탈락 리포트 출력")
    p_rep.add_argument("mode", choices=["daily", "weekly", "season"])
    p_rep.add_argument("--season", help="기수 (예: 7기) — season 모드 전용")

    args = parser.parse_args()

    if args.command == "dropout":
        DRY_RUN = args.dry_run
        process_dropout(args.name, args.track, args.generation)
    elif args.command == "rollback":
        do_rollback(args.name)
    elif args.command == "report":
        do_report(args.mode, getattr(args, "season", None))
    else:
        parser.print_help()
