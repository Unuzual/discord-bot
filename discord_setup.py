import discord
from discord.ext import commands
import asyncio
import csv
import os
from collections import defaultdict

# ====== 설정 ======
DISCORD_TOKEN = os.environ.get("DISCORD_TOKEN", "여기에_디스코드_봇_토큰")
GUILD_ID = int(os.environ.get("GUILD_ID", "여기에_서버_ID"))
GENERATION = "7기"  # 기수 변경

# CSV 파일 경로들 (노션에서 내보낸 파일)
CSV_FILES = {
    "크리에이터 트랙": [
        "6기_1조.csv",
        "6기_2조.csv",
        "6기_3조.csv",
    ]
    # 다른 트랙 추가 가능
}

# ====== 채널 구조 정의 ======
# 트랙별 과제인증 채널 커스터마이징
TRACK_EXTRA_CHANNELS = {
    "크리에이터 트랙": ["숏폼-과제-인증", "롱폼-과제-인증"],
    "빌더 기초 트랙": ["과제-인증"],
    "빌더 심화 트랙": ["과제-인증"],
    "세일즈 실전 트랙": ["과제-인증"],
    "AI 에이전트 트랙": ["과제-인증"],
    "앱 개발 트랙": ["과제-인증"],
}

intents = discord.Intents.default()
intents.members = True
bot = commands.Bot(command_prefix="!", intents=intents)


def load_members_from_csv(filepath):
    """CSV에서 멤버 정보 로드 (탈락자 제외)"""
    members = []
    with open(filepath, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row.get("ID", "")
            discord_id = row.get("디스코드 ID", "").strip()
            role = row.get("직책", "").strip()
            track = row.get("트랙", "").strip()

            # 탈락자 제외
            if "탈락" in name:
                print(f"  ⏭️  탈락자 제외: {name}")
                continue
            if discord_id and discord_id != "@ceoyujin_":
                members.append({
                    "name": name,
                    "discord_id": discord_id,
                    "role": role,
                    "track": track,
                })
    return members


async def setup_track(guild, track_name, group_count, generation):
    """트랙별 카테고리 및 채널 생성"""
    gen = generation
    track_short = track_name.replace(" 트랙", "").replace(" ", "-")
    category_name = f"====={track_name}====="

    print(f"\n📁 카테고리 생성 중: {category_name}")

    # 기존 카테고리 있으면 스킵
    existing = discord.utils.get(guild.categories, name=category_name)
    if existing:
        print(f"  ⏭️  이미 존재함, 스킵")
        category = existing
    else:
        category = await guild.create_category(category_name)
        print(f"  ✅ 카테고리 생성 완료")

    # 역할 생성: 트랙 전체 역할
    track_role_name = f"{track_short}-{gen}"
    track_role = discord.utils.get(guild.roles, name=track_role_name)
    if not track_role:
        track_role = await guild.create_role(name=track_role_name)
        print(f"  ✅ 역할 생성: {track_role_name}")

    # 조장 역할
    leader_role_name = f"{track_short}-{gen}-조장"
    leader_role = discord.utils.get(guild.roles, name=leader_role_name)
    if not leader_role:
        leader_role = await guild.create_role(name=leader_role_name)
        print(f"  ✅ 역할 생성: {leader_role_name}")

    # 조별 역할 생성
    group_roles = {}
    for i in range(1, group_count + 1):
        role_name = f"{track_short}-{gen}-{i}조"
        role = discord.utils.get(guild.roles, name=role_name)
        if not role:
            role = await guild.create_role(name=role_name)
            print(f"  ✅ 역할 생성: {role_name}")
        group_roles[i] = role

    # 권한 설정 헬퍼
    def track_overwrites(extra_roles=None):
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(read_messages=False),
            track_role: discord.PermissionOverwrite(read_messages=True, send_messages=True),
        }
        if extra_roles:
            for r in extra_roles:
                overwrites[r] = discord.PermissionOverwrite(read_messages=True, send_messages=True)
        return overwrites

    def group_overwrites(group_role):
        return {
            guild.default_role: discord.PermissionOverwrite(read_messages=False),
            group_role: discord.PermissionOverwrite(read_messages=True, send_messages=True),
            leader_role: discord.PermissionOverwrite(read_messages=True, send_messages=True),
        }

    # 공지 채널 (트랙 전체 읽기 가능, 쓰기 불가)
    announce_name = f"{track_short}-{gen}-공지"
    if not discord.utils.get(guild.text_channels, name=announce_name):
        announce_overwrites = {
            guild.default_role: discord.PermissionOverwrite(read_messages=False),
            track_role: discord.PermissionOverwrite(read_messages=True, send_messages=False),
        }
        await guild.create_text_channel(announce_name, category=category, overwrites=announce_overwrites)
        print(f"  📢 공지 채널 생성: {announce_name}")

    # 과제인증 채널들
    extra_channels = TRACK_EXTRA_CHANNELS.get(track_name, ["과제-인증"])
    for ch_name in extra_channels:
        full_name = f"{track_short}-{gen}-{ch_name}"
        if not discord.utils.get(guild.text_channels, name=full_name):
            await guild.create_text_channel(full_name, category=category, overwrites=track_overwrites())
            print(f"  # 채널 생성: {full_name}")

    # 조장 채널
    leader_ch_name = f"{track_short}-{gen}-조장"
    if not discord.utils.get(guild.text_channels, name=leader_ch_name):
        leader_ch_overwrites = {
            guild.default_role: discord.PermissionOverwrite(read_messages=False),
            leader_role: discord.PermissionOverwrite(read_messages=True, send_messages=True),
        }
        await guild.create_text_channel(leader_ch_name, category=category, overwrites=leader_ch_overwrites)
        print(f"  # 채널 생성: {leader_ch_name}")

    # 조별 텍스트 + 음성 채널
    for i in range(1, group_count + 1):
        group_role = group_roles[i]

        text_ch_name = f"{track_short}-{gen}-{i}조"
        if not discord.utils.get(guild.text_channels, name=text_ch_name):
            await guild.create_text_channel(text_ch_name, category=category, overwrites=group_overwrites(group_role))
            print(f"  # 텍스트 채널 생성: {text_ch_name}")

        voice_ch_name = f"{track_short}-{gen}-{i}조-화상미팅"
        if not discord.utils.get(guild.voice_channels, name=voice_ch_name):
            await guild.create_voice_channel(voice_ch_name, category=category, overwrites=group_overwrites(group_role))
            print(f"  🔊 음성 채널 생성: {voice_ch_name}")

    return track_role, leader_role, group_roles


@bot.event
async def on_ready():
    print(f"\n🤖 봇 로그인: {bot.user}")
    guild = bot.get_guild(GUILD_ID)
    if not guild:
        print("❌ 서버를 찾을 수 없습니다. GUILD_ID를 확인하세요.")
        await bot.close()
        return

    print(f"✅ 서버 연결: {guild.name}")
    print(f"🚀 {GENERATION} 채널 세팅 시작...\n")

    # ====== 여기서 트랙/조 수 설정 ======
    tracks = {
        "크리에이터 트랙": 3,
        "빌더 기초 트랙": 5,
        "빌더 심화 트랙": 3,
        "세일즈 실전 트랙": 3,
        "AI 에이전트 트랙": 3,
        "앱 개발 트랙": 3,
    }

    for track_name, group_count in tracks.items():
        await setup_track(guild, track_name, group_count, GENERATION)
        await asyncio.sleep(1)  # API 레이트 리밋 방지

    print("\n\n✅ 모든 채널 세팅 완료!")
    await bot.close()


bot.run(DISCORD_TOKEN)
