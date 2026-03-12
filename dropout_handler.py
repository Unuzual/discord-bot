#!/usr/bin/env python3
"""
탈락자 자동 처리 스크립트

Usage:
  python dropout_handler.py "이름" [--track "트랙명"] [--generation 7기] [--dry-run]
  python dropout_handler.py batch dropouts.csv [--generation 7기] [--dry-run]
  python dropout_handler.py rollback "이름"
  python dropout_handler.py report daily|weekly|season [--season 7기]

Batch CSV 형식 (UTF-8):
  이름,트랙,기수
  박현정,크리에이터 트랙,
  김민수,,
  이영희,빌더 기초 트랙,7기
  * 트랙/기수 컬럼은 선택. 비워두면 자동 감지.
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
    return sel["name"].strip() if sel else None

def prop_multi_select(props: dict, key: str) -> list[str]:
    return [t["name"].strip() for t in props.get(key, {}).get("multi_select", [])]

def prop_status(props: dict, key: str) -> str | None:
    s = props.get(key, {}).get("status")
    return s["name"] if s else None


# ═══════════════════════════════════════════════════════
# 멤버 마스터 DB
# ═══════════════════════════════════════════════════════

def member_find(name: str) -> dict | None:
    """이름(rich_text)으로 멤버 마스터 DB 검색
    - 정확히 일치하는 항목 우선 반환
    - 없으면 contains로 재검색 (예: '고원규' → '고원규/1Q/6기' 매칭)
    """
    # 1차: equals 정확 검색
    data = _post(
        f"https://api.notion.com/v1/databases/{MEMBER_DB_ID}/query",
        {"filter": {"property": "이름", "rich_text": {"equals": name}}},
    )
    results = data.get("results", [])
    if results:
        return results[0]

    # 2차: contains 검색
    data = _post(
        f"https://api.notion.com/v1/databases/{MEMBER_DB_ID}/query",
        {"filter": {"property": "이름", "rich_text": {"contains": name}}},
    )
    results = data.get("results", [])
    if len(results) > 1:
        print(f"  ⚠️  '{name}' 검색 결과가 {len(results)}명입니다. 첫 번째 결과를 사용합니다.")
        for r in results:
            title = r.get("properties", {}).get("이름", {})
            print(f"     - {title}")
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


def member_add_memo(page_id: str, generation: str, track: str, props: dict) -> str:
    """비고 속성에 탈락 메모 텍스트 누적 추가, 추가된 텍스트 반환"""
    today = datetime.now().strftime("%Y-%m-%d")
    new_line = f"🚫 {generation} {track} 탈락({today})"

    # 기존 비고 내용 읽기
    existing = prop_text(props, "기타사항")
    updated = f"{existing}\n{new_line}".strip() if existing else new_line

    if DRY_RUN:
        print(f"  [DRY-RUN] [멤버 마스터 DB] 기타사항 메모 추가 예정: '{new_line}'")
        return new_line
    _patch(
        f"https://api.notion.com/v1/pages/{page_id}",
        {"properties": {"기타사항": {"rich_text": [{"type": "text", "text": {"content": updated}}]}}},
    )
    print(f"  ✅ [멤버 마스터 DB] 기타사항 메모 추가: '{new_line}'")
    return new_line


def member_rollback(page_id: str, original_tracks: list[str], original_status: str, memo_text: str | None) -> None:
    # 트랙 복구
    _patch(
        f"https://api.notion.com/v1/pages/{page_id}",
        {
            "properties": {
                "트랙": {"multi_select": [{"name": t} for t in original_tracks]},
            }
        },
    )
    # 비고에서 해당 메모 줄 제거
    if memo_text:
        try:
            page = _get(f"https://api.notion.com/v1/pages/{page_id}")
            existing = prop_text(page["properties"], "기타사항")
            lines = [l for l in existing.split("\n") if l.strip() != memo_text.strip()]
            updated = "\n".join(lines).strip()
            _patch(
                f"https://api.notion.com/v1/pages/{page_id}",
                {"properties": {"기타사항": {"rich_text": [{"type": "text", "text": {"content": updated}}] if updated else []}}},
            )
        except Exception:
            print("  ⚠️  비고 메모 제거 실패")
    print("  ✅ [멤버 마스터 DB] 복구 완료")


# ═══════════════════════════════════════════════════════
# 트랙 신청 DB
# ═══════════════════════════════════════════════════════

SIGNUP_TRACK_COLS = ["월요일 트랙", "화요일 트랙", "수요일 트랙"]

# Notion 멤버 마스터 DB 트랙명 → 트랙 신청 DB 선택값 매핑
# (신청 DB에는 라이트 트랙이 상위 트랙명으로 저장되는 경우 대비)
TRACK_ALIAS_MAP = {
    "크리에이터 숏폼 트랙":          ["크리에이터 숏폼 트랙", "크리에이터 트랙"],
    "크리에이터 롱폼 트랙":          ["크리에이터 롱폼 트랙", "크리에이터 트랙"],
    "크리에이터 라이트 트랙 (숏폼)": ["크리에이터 라이트 트랙 (숏폼)", "크리에이터 트랙"],
    "크리에이터 라이트 트랙 (롱폼)": ["크리에이터 라이트 트랙 (롱폼)", "크리에이터 트랙"],
    "빌더 기초 트랙":                ["빌더 기초 트랙"],
    "빌더 심화 트랙":                ["빌더 심화 트랙"],
    "빌더 라이트 트랙 (기초)":       ["빌더 라이트 트랙 (기초)", "빌더 기초 트랙"],
    "빌더 라이트 트랙 (심화)":       ["빌더 라이트 트랙 (심화)", "빌더 심화 트랙"],
    "세일즈 실전 트랙":              ["세일즈 실전 트랙"],
    "AI 에이전트 트랙":              ["AI 에이전트 트랙"],
    "앱 개발 트랙":                  ["앱 개발 트랙"],
}


def signup_find(name: str, generation: str | None = None) -> dict | None:
    """이름(title) + 기수로 트랙 신청 DB 검색 (contains로 '조민석/4기' 형태도 매칭)"""

    def _build_filter(name_filter: dict) -> dict:
        """기수가 있으면 AND 복합 필터, 없으면 이름 단독 필터"""
        if generation:
            return {
                "and": [
                    name_filter,
                    {"property": "기수", "rich_text": {"equals": generation}},
                ]
            }
        return name_filter

    # 1차: equals
    data = _post(
        f"https://api.notion.com/v1/databases/{TRACK_SIGNUP_DB_ID}/query",
        {"filter": _build_filter({"property": "이름", "title": {"equals": name}})},
    )
    results = data.get("results", [])
    if results:
        return results[0]
    # 2차: contains
    data = _post(
        f"https://api.notion.com/v1/databases/{TRACK_SIGNUP_DB_ID}/query",
        {"filter": _build_filter({"property": "이름", "title": {"contains": name}})},
    )
    results = data.get("results", [])
    if len(results) > 1:
        print(f"  ⚠️  트랙 신청 DB '{name}' 검색 결과 {len(results)}명, 첫 번째 사용")
    return results[0] if results else None


def signup_clear_track(page_id: str, props: dict, dropout_track: str) -> dict:
    """
    월/화/수 트랙 속성 중 dropout_track과 일치하는 값을 null로 제거.
    원본 값 dict 반환 (롤백용).
    트랙명 별칭(TRACK_ALIAS_MAP)도 함께 비교.
    """
    aliases = {a.strip() for a in TRACK_ALIAS_MAP.get(dropout_track, [dropout_track])}
    aliases.add(dropout_track.strip())

    original = {}
    updates = {}
    for col in SIGNUP_TRACK_COLS:
        val = prop_select(props, col)
        original[col] = val
        if val and val.strip() in aliases:
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

def group_find_rows(name: str, track: str | None = None) -> list[dict]:
    """트랙/조 DB(상위)에서 이름+트랙으로 검색. 조 번호 추출용"""
    if track:
        f = {"and": [
            {"property": "이름", "title": {"contains": name}},
            {"property": "트랙명", "multi_select": {"contains": track}},
        ]}
    else:
        f = {"property": "이름", "title": {"contains": name}}
    data = _post(
        f"https://api.notion.com/v1/databases/{TRACK_GROUP_DB_ID}/query",
        {"filter": f},
    )
    return data.get("results", [])


# 트랙명 → 트랙/조 하위 DB 최상위 페이지 ID 매핑
# 각 트랙 하위에 '6기 N조' DB들이 있음
TRACK_GROUP_PAGE_MAP = {
    "빌더 기초 트랙":   "2e06400e926880c58eb6e1fea94ced14",
    "크리에이터 트랙":  "2e06400e926880ec9d2bfa2d6b19a51f",
    "빌더 심화 트랙":   "2e16400e926880758dbaddc7326c09a9",
    "AI 에이전트 트랙": "2fb6400e926880bdbf76f3f15ea50e47",
    "세일즈 실전 트랙": "2fb6400e926880c0838dec6b741e5d19",
}

# 트랙 페이지 ID 매핑 (트랙/조 상위 DB의 row page ID)
# 각 트랙 페이지 안에 조 DB들이 인라인으로 존재
TRACK_PAGE_ID_MAP = {
    "빌더 기초 트랙":   "2e06400e926880c58eb6e1fea94ced14",
    "크리에이터 트랙":  "2e06400e926880ec9d2bfa2d6b19a51f",
    "빌더 심화 트랙":   "2e16400e926880758dbaddc7326c09a9",
    "AI 에이전트 트랙": "2fb6400e926880bdbf76f3f15ea50e47",
    "세일즈 실전 트랙": "2fb6400e926880c0838dec6b741e5d19",
}

# 숏폼/롱폼/라이트 트랙 → 상위 트랙 페이지 ID로 fallback
# 트랙/조 DB는 숏폼·롱폼 구분 없이 '크리에이터 트랙' 하나로 관리
_TRACK_PAGE_FALLBACK = {
    "크리에이터 숏폼 트랙":          "크리에이터 트랙",
    "크리에이터 롱폼 트랙":          "크리에이터 트랙",
    "크리에이터 라이트 트랙 (숏폼)": "크리에이터 트랙",
    "크리에이터 라이트 트랙 (롱폼)": "크리에이터 트랙",
    "빌더 라이트 트랙 (기초)":       "빌더 기초 트랙",
    "빌더 라이트 트랙 (심화)":       "빌더 심화 트랙",
}


def _get_inline_dbs(track_page_id: str) -> list[dict]:
    """트랙 페이지 하위 inline DB 블록 목록 반환 (child_database 블록)"""
    data = _get(f"https://api.notion.com/v1/blocks/{track_page_id}/children")
    return [b for b in data.get("results", []) if b.get("type") == "child_database"]


def group_find_member_in_track(name: str, track: str) -> tuple[str, str, str | None]:
    """
    트랙 페이지 하위 인라인 조 DB들을 순회해서 name과 일치하는 멤버를 찾음.
    반환: (조 번호 str, 조 DB ID, 조장 discord_id | None)
    못 찾으면 ("", "", None)
    """
    import re as _re

    track_page_id = TRACK_PAGE_ID_MAP.get(track)
    if not track_page_id:
        # fallback: 숏폼/롱폼/라이트 → 상위 트랙명으로 재시도
        fallback_track = _TRACK_PAGE_FALLBACK.get(track)
        if fallback_track:
            track_page_id = TRACK_PAGE_ID_MAP.get(fallback_track)
    if not track_page_id:
        # 상위 트랙/조 DB에서 트랙 페이지 ID 동적 조회
        data = _post(
            f"https://api.notion.com/v1/databases/{TRACK_GROUP_DB_ID}/query",
            {"filter": {"property": "트랙명", "multi_select": {"contains": track}}},
        )
        rows = data.get("results", [])
        if not rows:
            return ("", "", None)
        track_page_id = rows[0]["id"]

    # 트랙 페이지 하위 inline DB 목록 (각 조 DB)
    inline_dbs = _get_inline_dbs(track_page_id)
    if not inline_dbs:
        return ("", "", None)

    for db_block in inline_dbs:
        db_id = db_block["id"]
        db_title = db_block.get("child_database", {}).get("title", "")
        m = _re.search(r"(\d+)조", db_title)
        group_num = m.group(1) if m else ""

        # 멤버 검색 (조 DB title 컬럼명은 "ID")
        try:
            member_data = _post(
                f"https://api.notion.com/v1/databases/{db_id}/query",
                {"filter": {"property": "ID", "title": {"contains": name}}},
            )
        except Exception:
            continue

        members = member_data.get("results", [])
        if members:
            # 조장 찾기
            leader_discord = None
            try:
                leader_data = _post(
                    f"https://api.notion.com/v1/databases/{db_id}/query",
                    {"filter": {"property": "직책", "select": {"equals": "조장"}}},
                )
                leaders = leader_data.get("results", [])
                if leaders:
                    discord_texts = leaders[0].get("properties", {}).get("디스코드 ID", {}).get("rich_text", [])
                    leader_discord = discord_texts[0]["plain_text"].strip() if discord_texts else None
            except Exception:
                pass
            return (group_num, db_id, leader_discord)

    return ("", "", None)


def group_find_number(name: str, track: str) -> str:
    """트랙/조 하위 DB에서 특정 트랙의 조 번호 조회. 없으면 빈 문자열 반환"""
    group_num, _, _ = group_find_member_in_track(name, track)
    return group_num


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
        member_rollback(mb["page_id"], mb["original_tracks"], mb.get("original_status", ""), mb.get("memo_text"))

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


DROPOUT_REPORT_PARENT_PAGE_ID = "3196400e-9268-804e-9aea-daf3d2b7b76a"  # 탈락자 관리 페이지


def _notion_create_report_page(title: str, entries: list[dict]) -> str | None:
    """노션에 탈락자 리포트 페이지 생성, 생성된 페이지 URL 반환"""
    track_counts = Counter(e["track"] for e in entries)

    # 탈락자 목록 테이블
    rows = "\n".join(
        f"| {e['name']} | {e['track']} | {e['date']} |"
        for e in entries
    )
    track_summary = "\n".join(
        f"- {track}: {cnt}명"
        for track, cnt in track_counts.most_common()
    )

    content = f"""## 📊 요약
- 총 탈락자 수: **{len(entries)}명**

{track_summary}

## 📋 탈락자 목록

| 이름 | 트랙 | 처리일 |
|------|------|--------|
{rows}
"""

    payload = {
        "parent": {"page_id": DROPOUT_REPORT_PARENT_PAGE_ID},
        "properties": {
            "title": [{"text": {"content": title}}]
        },
        "children": [
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [{"type": "text", "text": {"content": content}}]
                }
            }
        ]
    }

    # Notion blocks API로 직접 생성
    res = requests.post(
        "https://api.notion.com/v1/pages",
        headers=NOTION_HEADERS,
        json={
            "parent": {"page_id": DROPOUT_REPORT_PARENT_PAGE_ID.replace("-", "")},
            "properties": {
                "title": [{"text": {"content": title}}]
            }
        }
    )
    if res.status_code != 200:
        print(f"  ⚠️  노션 페이지 생성 실패: {res.text}")
        return None

    page_id = res.json()["id"]
    page_url = res.json()["url"]

    # 내용 추가
    block_children = []

    # 요약 헤딩
    block_children.append({
        "object": "block", "type": "heading_2",
        "heading_2": {"rich_text": [{"type": "text", "text": {"content": "📊 요약"}}]}
    })
    block_children.append({
        "object": "block", "type": "paragraph",
        "paragraph": {"rich_text": [{"type": "text", "text": {"content": f"총 탈락자 수: {len(entries)}명"}, "annotations": {"bold": True}}]}
    })

    # 트랙별 집계
    for track, cnt in track_counts.most_common():
        block_children.append({
            "object": "block", "type": "bulleted_list_item",
            "bulleted_list_item": {"rich_text": [{"type": "text", "text": {"content": f"{track}: {cnt}명"}}]}
        })

    # 탈락자 목록 헤딩
    block_children.append({
        "object": "block", "type": "heading_2",
        "heading_2": {"rich_text": [{"type": "text", "text": {"content": "📋 탈락자 목록"}}]}
    })

    # 테이블
    table_rows = [["이름", "트랙", "처리일"]] + [[e["name"], e["track"], e["date"]] for e in entries]
    table_block = {
        "object": "block", "type": "table",
        "table": {
            "table_width": 3,
            "has_column_header": True,
            "has_row_header": False,
            "children": [
                {
                    "object": "block", "type": "table_row",
                    "table_row": {"cells": [[{"type": "text", "text": {"content": cell}}] for cell in row]}
                }
                for row in table_rows
            ]
        }
    }
    block_children.append(table_block)

    requests.patch(
        f"https://api.notion.com/v1/blocks/{page_id}/children",
        headers=NOTION_HEADERS,
        json={"children": block_children}
    )

    return page_url


def do_report(mode: str, season: str | None = None) -> None:
    if not DROPOUT_LOG.exists():
        print("📋 탈락 처리 기록이 없습니다.")
        return

    log: list[dict] = json.loads(DROPOUT_LOG.read_text())
    today = date.today()

    if mode == "daily":
        entries = [e for e in log if e["date"] == today.strftime("%Y-%m-%d")]
        title = f"탈락자 리포트 ({today})"
    elif mode == "weekly":
        cutoff = (today - timedelta(days=7)).strftime("%Y-%m-%d")
        entries = [e for e in log if e["date"] >= cutoff]
        title = f"탈락자 리포트 주간 ({(today - timedelta(days=7)).strftime('%Y-%m-%d')} ~ {today})"
    elif mode == "season":
        if not season:
            print("❌ --season 옵션이 필요합니다. 예: --season 7기")
            sys.exit(1)
        entries = [e for e in log if e["generation"] == season]
        title = f"탈락자 리포트 ({season})"
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

    # 노션 리포트 페이지 생성
    print("📝 노션 리포트 페이지 생성 중...")
    url = _notion_create_report_page(title, entries)
    if url:
        print(f"  ✅ 노션 페이지 생성 완료: {url}")
    else:
        print("  ⚠️  노션 페이지 생성 실패 (터미널 출력만 저장됨)")


# ═══════════════════════════════════════════════════════
# Discord
# ═══════════════════════════════════════════════════════

intents = discord.Intents.default()
intents.members = True
bot = commands.Bot(command_prefix="!", intents=intents)
_ctx: dict = {}


# 트랙명 → Discord 역할 접두사 매핑
_TRACK_DISCORD_PREFIX = {
    "크리에이터 트랙":                "크리에이터",
    "크리에이터 숏폼 트랙":          "크리에이터",
    "크리에이터 롱폼 트랙":          "크리에이터",
    "크리에이터 라이트 트랙 (숏폼)": "크리에이터",
    "크리에이터 라이트 트랙 (롱폼)": "크리에이터",
    "빌더 기초 트랙":                "빌더-기초",
    "빌더 라이트 트랙 (기초)":       "빌더-기초",
    "빌더 심화 트랙":                "빌더-심화",
    "빌더 라이트 트랙 (심화)":       "빌더-심화",
    "세일즈 실전 트랙":              "세일즈-실전",
    "AI 에이전트 트랙":              "AI에이전트-실전",
    "앱 개발 트랙":                  "앱개발",
}


def _track_short(track_name: str) -> str:
    return _TRACK_DISCORD_PREFIX.get(track_name, track_name.replace(" 트랙", "").replace(" ", "-"))


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


async def _discord_process_one(guild: discord.Guild, entry: dict) -> None:
    """Discord에서 한 명의 탈락 처리 (DM 전송 + 역할 제거). 배치/단건 공용."""
    name              = entry["name"]
    generation        = entry["generation"]
    dropout_track     = entry["dropout_track"]
    discord_id        = entry.get("discord_id", "")
    user_id           = entry.get("user_id", "")
    group_str         = entry.get("group_str", "")
    leader_discord_id = entry.get("leader_discord_id", "")

    discord_member = await _discord_find_member(guild, user_id, discord_id)
    if not discord_member:
        print(f"  ⚠️  Discord 멤버를 찾을 수 없습니다 (ID: {discord_id})")
        return

    print(f"  Discord 멤버: {discord_member.display_name} ({discord_member.name})")

    # 역할 목록 조사
    ts = _track_short(dropout_track)
    role_names = [f"{ts}-{generation}", f"{ts}-{generation}-조장"]
    if group_str:
        role_names.append(f"{ts}-{generation}-{group_str}조")
    roles_to_remove = [
        rn for rn in role_names
        if (r := discord.utils.get(guild.roles, name=rn)) and r in discord_member.roles
    ]

    # 조장 조회
    leader = None
    if leader_discord_id:
        results = await guild.query_members(query=leader_discord_id.lstrip("@"), limit=5)
        for m in results:
            if m.name == leader_discord_id.lstrip("@") or m.display_name == leader_discord_id.lstrip("@"):
                leader = m
                break
    if not leader:
        leader_role = discord.utils.get(guild.roles, name=f"{ts}-{generation}-조장")
        group_role  = discord.utils.get(guild.roles, name=f"{ts}-{generation}-{group_str}조") if group_str else None
        leader      = _discord_find_leader(guild, leader_role, group_role, discord_member) if leader_role else None

    if DRY_RUN:
        print(f"  [DRY-RUN] [Discord] 탈락자 DM 전송 예정 → {discord_member.display_name} ({generation} {dropout_track} 탈락 안내)")
        if roles_to_remove:
            print(f"  [DRY-RUN] [Discord] 역할 제거 예정: {', '.join(roles_to_remove)}")
        else:
            print(f"  [DRY-RUN] [Discord] 제거할 역할 없음")
        if leader and leader.id != discord_member.id:
            print(f"  [DRY-RUN] [Discord] 조장 DM 전송 예정 → {leader.display_name}")
        else:
            print(f"  [DRY-RUN] [Discord] 조장을 찾을 수 없음")
        return

    # 탈락자 DM
    try:
        await discord_member.send(
            f"안녕하세요! ASC 커뮤니티 운영진입니다 🙌\n\n"
            f"이번 {generation} {dropout_track} 과제 미제출로 확인되어, 아쉽게도 해당 트랙에서 탈락 처리될 예정임을 안내드립니다.\n\n"
            f"혹시 제출하셨는데 착오가 있는 것 같다면 커뮤니티 매니저에게 말씀해 주세요!\n"
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

   # 조장 DM (탈락자 본인이 조장이면 스킵)
    if leader and leader.id != discord_member.id:
        try:
            await leader.send(
                f"안녕하세요 조장님. ASC 커뮤니티 운영진입니다. "
                f"{dropout_track} {generation} {group_str}조의 {name}님께서 과제 미제출로 트랙 탈락하셨음을 알립니다~! "
                f"{name}님은 채널에서 제외되셔서 더이상 조별 모임 등은 참가가 불가능하니 이 점 참고해 주세요~! 감사합니다."
            )
            print(f"  ✅ [Discord] 조장 DM 전송: {leader.display_name}")
        except discord.Forbidden:
            print(f"  ⚠️  조장 DM 실패: {leader.display_name} (DM 차단)")
    else:
        print(f"  ⚠️  조장을 찾을 수 없음")


@bot.event
async def on_ready():
    print(f"🤖 봇 로그인: {bot.user}\n")
    guild = bot.get_guild(GUILD_ID)
    if not guild:
        print("❌ Discord 서버를 찾을 수 없습니다.")
        await bot.close()
        return

    # 배치 모드: _batch_queue가 있으면 전원 순회 처리
    batch_queue = _ctx.get("_batch_queue")
    if batch_queue:
        total = len(batch_queue)
        for idx, entry in enumerate(batch_queue, start=1):
            print(f"\n  ── Discord [{idx}/{total}] {entry['name']} ──")
            try:
                await _discord_process_one(guild, entry)
            except Exception as e:
                print(f"  ❌ Discord 처리 오류 ({entry['name']}): {e}")
    else:
        # 단건 모드 (기존 호환)
        await _discord_process_one(guild, _ctx)

    await bot.close()


# ═══════════════════════════════════════════════════════
# 메인 탈락 처리 흐름
# ═══════════════════════════════════════════════════════

def _process_notion(name: str, dropout_track: str | None, generation: str | None, group: str | None = None) -> dict | None:
    """Notion 처리만 수행하고 Discord 처리에 필요한 정보를 dict로 반환.
    실패 시 None 반환 (배치 모드에서 다음 멤버로 진행하기 위해 sys.exit 대신 None).
    """
    # ── 멤버 마스터 DB 조회 ──
    print("🔍 Notion 멤버 마스터 DB 조회 중...")
    member_page = member_find(name)
    if not member_page:
        print(f"❌ '{name}'을(를) 멤버 마스터 DB에서 찾을 수 없습니다.")
        return None

    props = member_page["properties"]
    original_tracks = prop_multi_select(props, "트랙")
    discord_id_str  = prop_text(props, "디스코드 ID")
    user_id_str     = prop_text(props, "사용자 ID")

    # ── 기수 자동 감지 ──
    if not generation:
        notion_gen = prop_select(props, "기수")
        if not notion_gen:
            print(f"❌ '{name}'의 기수 정보가 Notion DB에 없습니다. --generation 옵션을 지정해주세요.")
            return None
        generation = notion_gen
        print(f"  ℹ️  기수 자동 감지: {generation}")

    print(f"\n🚨 탈락 처리 시작: {name} ({generation})\n")

    # dropout_track 미지정 시 자동 결정
    if not dropout_track:
        if len(original_tracks) == 1:
            dropout_track = original_tracks[0]
        elif len(original_tracks) == 0:
            # 트랙이 아예 없으면 대시보드가 이미 제거했을 가능성 → --track 필수
            print(f"❌ '{name}'의 트랙 정보가 없습니다.")
            print(f"   대시보드에서 이미 트랙을 제거했다면 --track 옵션으로 트랙명을 지정해주세요.")
            return None

        else:
            print(f"⚠️  여러 트랙이 있습니다: {original_tracks}")
            print(f"   --track 옵션으로 탈락 트랙을 지정해주세요.")
            return None
    elif dropout_track not in original_tracks:
        # ── 대시보드 선처리 감지 ──
        # 트랙이 이미 제거된 경우, 기타사항 메모로 대시보드가 먼저 처리했는지 확인
        existing_memo = prop_text(props, "기타사항")
        # 대시보드 메모 형식: "🚫 6기 AI 에이전트 트랙 탈락(4주차)"
        # 내 봇 메모 형식:   "🚫 7기 빌더 기초 트랙 탈락(2026-03-12)"
        # 트랙명 별칭도 포함해서 검사 (숏폼/롱폼 → "크리에이터 트랙" 등)
        track_aliases = list(TRACK_ALIAS_MAP.get(dropout_track, [])) + [dropout_track]
        track_mentioned = any(alias in existing_memo for alias in track_aliases)
        already_processed = track_mentioned and "탈락" in existing_memo
        if already_processed:
            print(f"  ℹ️  대시보드에서 이미 Notion 처리 완료 감지")
            print(f"      (기타사항 메모 확인: '{existing_memo[:80]}')")
            print(f"  → Notion 업데이트 스킵, Discord 처리만 진행합니다.\n")

            # 조 번호는 트랙/조 DB에서 조회 (아직 아카이브 안 됐을 수 있음)
            group_str, _group_db_id, leader_discord_id = group_find_member_in_track(name, dropout_track)
            # 조 조회 실패 시 --group 옵션으로 수동 지정
            if not group_str and group:
                group_str = group
                print(f"  ℹ️  조 번호 수동 지정: {group_str}조")
            print(f"  이름: {name} | 트랙: {dropout_track} | 조: {group_str or '없음'}\n")

            # 트랙/조 DB 아카이브 (대시보드는 이걸 안 건드리므로 여기서 처리)
            print("📝 트랙/조 DB 처리 중...")
            if _group_db_id:
                _grp_data = _post(
                    f"https://api.notion.com/v1/databases/{_group_db_id}/query",
                    {"filter": {"property": "ID", "title": {"contains": name}}},
                )
                group_rows = _grp_data.get("results", [])
                group_archive_rows(group_rows)
            else:
                print(f"  ℹ️  [트랙/조 DB] 해당 멤버 없음 - 스킵")

            # 탈락 로그만 기록 (Notion 처리는 대시보드가 했으므로 롤백 파일 제외)
            if not DRY_RUN:
                log_append(name, dropout_track, generation)

            return {
                "name": name,
                "generation": generation,
                "dropout_track": dropout_track,
                "discord_id": discord_id_str,
                "user_id": user_id_str,
                "group_str": group_str,
                "leader_discord_id": leader_discord_id or "",
            }
        else:
            print(f"❌ '{dropout_track}'은(는) '{name}'의 트랙 목록에 없습니다: {original_tracks}")
            print(f"   트랙명을 정확히 확인해주세요.")
            return None

    # ── 조 번호: 트랙/조 DB에서 해당 트랙 기준으로 조회 ──
    group_str, _group_db_id, leader_discord_id = group_find_member_in_track(name, dropout_track)
    if not group_str and group:
        group_str = group
        print(f"  ℹ️  조 번호 수동 지정: {group_str}조")
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
            "memo_text": None,
        },
        "track_signup_db": {},
        "track_group_db": [],
    }

    # ── 1. 멤버 마스터 DB 업데이트 ──
    print("📝 멤버 마스터 DB 처리 중...")
    member_update_dropout(member_page["id"], original_tracks, dropout_track)
    memo_text = member_add_memo(member_page["id"], generation, dropout_track, props)
    snapshot["member_db"]["memo_text"] = memo_text

    # ── 2. 트랙 신청 DB 처리 ──
    print("\n📝 트랙 신청 DB 처리 중...")
    signup_page = signup_find(name, generation)
    if signup_page:
        orig_cols = signup_clear_track(signup_page["id"], signup_page["properties"], dropout_track)
        snapshot["track_signup_db"] = {
            "page_id": signup_page["id"],
            "original_cols": orig_cols,
        }
    else:
        print(f"  ℹ️  트랙 신청 DB에서 '{name}' 항목 없음")

    # ── 3. 트랙/조 DB 처리 (하위 인라인 조 DB에서 멤버 삭제) ──
    print("\n📝 트랙/조 DB 처리 중...")
    if _group_db_id:
        _grp_data = _post(
            f"https://api.notion.com/v1/databases/{_group_db_id}/query",
            {"filter": {"property": "ID", "title": {"contains": name}}},
        )
        group_rows = _grp_data.get("results", [])
        archived_ids = group_archive_rows(group_rows)
    else:
        print(f"  ℹ️  [트랙/조 DB] 해당 멤버 없음 - 스킵")
        archived_ids = []
    snapshot["track_group_db"] = archived_ids

    # ── 롤백 파일 저장 / 탈락 로그 기록 ──
    if DRY_RUN:
        print("\n  [DRY-RUN] 롤백 파일 저장 스킵")
        print("  [DRY-RUN] 탈락 로그 기록 스킵")
    else:
        rollback_save(name, snapshot)
        log_append(name, dropout_track, generation)

    return {
        "name": name,
        "generation": generation,
        "dropout_track": dropout_track,
        "discord_id": discord_id_str,
        "user_id": user_id_str,
        "group_str": group_str,
        "leader_discord_id": leader_discord_id or "",
    }


def process_dropout(name: str, dropout_track: str | None, generation: str | None, group: str | None = None) -> None:
    """단건 탈락 처리 (기존 호환)"""
    if DRY_RUN:
        print("=" * 52)
        print("  DRY-RUN 모드  |  실제 변경 없이 처리 내용만 표시")
        print("=" * 52)

    result = _process_notion(name, dropout_track, generation, group)
    if result is None:
        sys.exit(1)

    # ── Discord 처리 ──
    print("\n🎮 Discord 처리 중...")
    _ctx.update(result)
    bot.run(DISCORD_TOKEN)

    if DRY_RUN:
        print("\n✅ DRY-RUN 완료. 실제 변경된 내용은 없습니다.")
    else:
        print("\n🎉 탈락 처리 모두 완료!")


# ═══════════════════════════════════════════════════════
# 배치 처리
# ═══════════════════════════════════════════════════════

import csv

def _load_batch_csv(filepath: str) -> list[dict]:
    """CSV 파일에서 탈락 대상 명단 로드.
    CSV 컬럼: 이름 (필수), 트랙 (선택), 기수 (선택)
    """
    entries = []
    path = Path(filepath)
    if not path.exists():
        print(f"❌ 파일을 찾을 수 없습니다: {filepath}")
        sys.exit(1)

    with open(path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=2):
            name = (row.get("이름") or "").strip()
            if not name:
                print(f"  ⚠️  {i}행: 이름이 비어있어 스킵")
                continue
            entries.append({
                "name": name,
                "track": (row.get("트랙") or "").strip() or None,
                "generation": (row.get("기수") or "").strip() or None,
            })

    return entries


def process_batch(filepath: str, generation: str | None) -> None:
    """CSV 배치 탈락 처리: Notion 일괄 → Discord 1회 로그인"""
    entries = _load_batch_csv(filepath)
    if not entries:
        print("❌ 처리할 대상이 없습니다.")
        return

    total = len(entries)
    if DRY_RUN:
        print("=" * 52)
        print("  DRY-RUN 모드  |  실제 변경 없이 처리 내용만 표시")
        print("=" * 52)
    print(f"\n📋 배치 처리 시작: 총 {total}명\n")

    # ── Phase 1: Notion 일괄 처리 ──
    discord_queue: list[dict] = []  # Discord 처리 대기열
    results_summary: list[dict] = []

    for idx, entry in enumerate(entries, start=1):
        print(f"\n{'─' * 52}")
        print(f"  [{idx}/{total}] {entry['name']}")
        print(f"{'─' * 52}")

        gen = entry.get("generation") or generation
        try:
            result = _process_notion(entry["name"], entry.get("track"), gen, None)
        except Exception as e:
            print(f"  ❌ 처리 중 오류: {e}")
            results_summary.append({"name": entry["name"], "status": "❌ 오류", "detail": str(e)})
            continue

        if result is None:
            results_summary.append({"name": entry["name"], "status": "⚠️ 스킵", "detail": "Notion 조회 실패"})
            continue

        discord_queue.append(result)
        results_summary.append({
            "name": entry["name"],
            "status": "✅ 완료",
            "detail": f"{result['dropout_track']} ({result['generation']})",
        })

    # ── Phase 2: Discord 일괄 처리 (봇 1회 로그인) ──
    if discord_queue:
        print(f"\n\n{'═' * 52}")
        print(f"  🎮 Discord 일괄 처리: {len(discord_queue)}명")
        print(f"{'═' * 52}")

        # _ctx를 리스트로 변환하여 on_ready에서 순회 처리
        _ctx["_batch_queue"] = discord_queue
        bot.run(DISCORD_TOKEN)
    else:
        print("\n  ℹ️  Discord 처리 대상 없음")

    # ── 결과 요약 ──
    print(f"\n\n{'═' * 52}")
    print(f"  📊 배치 처리 결과 요약")
    print(f"{'═' * 52}")
    print(f"\n  {'이름':<12} {'상태':<10} {'상세'}")
    print(f"  {'─' * 48}")
    ok = skip = err = 0
    for r in results_summary:
        print(f"  {r['name']:<12} {r['status']:<10} {r['detail']}")
        if "완료" in r["status"]:
            ok += 1
        elif "스킵" in r["status"]:
            skip += 1
        else:
            err += 1
    print(f"\n  총 {total}명 | ✅ 성공 {ok} | ⚠️ 스킵 {skip} | ❌ 오류 {err}")

    if DRY_RUN:
        print("\n✅ DRY-RUN 완료. 실제 변경된 내용은 없습니다.")
    else:
        print("\n🎉 배치 탈락 처리 모두 완료!")


# ═══════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════

if __name__ == "__main__":
    # "이름"만 입력 시 dropout 서브커맨드로 자동 처리
    if len(sys.argv) >= 2 and sys.argv[1] not in ("dropout", "rollback", "report", "batch", "-h", "--help"):
        sys.argv.insert(1, "dropout")

    parser = argparse.ArgumentParser(description="탈락자 자동 처리 스크립트")
    sub = parser.add_subparsers(dest="command")

    # dropout
    p_drop = sub.add_parser("dropout", help="탈락 처리")
    p_drop.add_argument("name", help="탈락자 이름 (Notion 멤버 마스터 DB 기준)")
    p_drop.add_argument("--track", help="탈락 트랙명 (트랙이 하나면 자동 선택)")
    p_drop.add_argument("--generation", default=None, help="기수 (미지정 시 Notion DB에서 자동 감지)")
    p_drop.add_argument("--group", default=None, help="조 번호 직접 지정 (예: 1) — 대시보드 선처리 후 조 조회 실패 시 사용")
    p_drop.add_argument("--dry-run", action="store_true", help="실제 변경 없이 처리 내용만 미리보기")

    # batch
    p_batch = sub.add_parser("batch", help="CSV 파일로 일괄 탈락 처리")
    p_batch.add_argument("file", help="탈락 대상 CSV 파일 (컬럼: 이름, 트랙(선택), 기수(선택))")
    p_batch.add_argument("--generation", default=None, help="기수 일괄 지정 (CSV에 기수 컬럼이 없을 때)")
    p_batch.add_argument("--dry-run", action="store_true", help="실제 변경 없이 처리 내용만 미리보기")

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
        process_dropout(args.name, args.track, args.generation, args.group)
    elif args.command == "batch":
        DRY_RUN = args.dry_run
        process_batch(args.file, args.generation)
    elif args.command == "rollback":
        do_rollback(args.name)
    elif args.command == "report":
        do_report(args.mode, getattr(args, "season", None))
    else:
        parser.print_help()
