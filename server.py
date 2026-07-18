from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, FileResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import os
import redis.asyncio as aioredis
import json, math, time, httpx
from typing import Dict, List
from datetime import datetime
import google.auth.transport.requests
import google.oauth2.service_account

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

BRANCH_LAT = 31.2071871
BRANCH_LNG = 29.9328765

# FCM v1 — Service Account JSON من environment variable
FIREBASE_PROJECT_ID = "rwa7el-87810"
_FCM_SA_JSON = os.environ.get("FIREBASE_SERVICE_ACCOUNT_JSON", "")

def _get_fcm_access_token() -> str:
    """يجيب OAuth2 access token من الـ service account"""
    sa_info = json.loads(_FCM_SA_JSON)
    credentials = google.oauth2.service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=["https://www.googleapis.com/auth/firebase.messaging"]
    )
    credentials.refresh(google.auth.transport.requests.Request())
    return credentials.token

redis_client = aioredis.from_url(
    os.environ.get("REDIS_URL", "redis://localhost:6379"),
    decode_responses=True, protocol=2
)

admin_connections: list[WebSocket] = []
driver_connections: Dict[str, WebSocket] = {}

# --- دوال المساعدة لـ Redis ---
async def get_drivers_from_redis() -> Dict[str, dict]:
    drivers_raw = await redis_client.hgetall("fleet:drivers")
    drivers = {}
    for name, data_str in drivers_raw.items():
        drivers[name] = json.loads(data_str)
    return drivers

async def save_driver_to_redis(name: str, data: dict):
    await redis_client.hset("fleet:drivers", name, json.dumps(data))

async def delete_driver_from_redis(name: str):
    await redis_client.hdel("fleet:drivers", name)

async def get_queue_from_redis() -> List[str]:
    queue_str = await redis_client.get("fleet:queue")
    if queue_str:
        return json.loads(queue_str)
    return []

async def save_queue_to_redis(queue_list: List[str]):
    await redis_client.set("fleet:queue", json.dumps(queue_list))


def haversine(lat1, lng1, lat2, lng2):
    R = 6371000
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lng2 - lng1)
    a = math.sin(dp/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return round(R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a)))

def get_dashboard_stats(drivers: dict):
    stats = {"Total Drivers": len(drivers), "Waiting": 0, "Out": 0, "Break": 0, "Orders": 0, "Returns": 0, "Misses": 0}
    for d in drivers.values():
        if d["state"] == "Waiting": stats["Waiting"] += 1
        elif d["state"] == "Out": stats["Out"] += 1
        elif d["state"] == "Break": stats["Break"] += 1
        stats["Orders"] += d.get("orders", 0)
        stats["Returns"] += d.get("returns", 0)
        stats["Misses"] += d.get("misses", 0)
    return stats

def get_drivers_list(drivers: dict, queue: list):
    for i, name in enumerate(queue):
        if name in drivers: drivers[name]["queue_pos"] = i + 1
    return list(drivers.values())

async def broadcast_state(event_type="update"):
    drivers = await get_drivers_from_redis()
    queue = await get_queue_from_redis()
    drivers_list = get_drivers_list(drivers, queue)
    stats = get_dashboard_stats(drivers)

    msg = json.dumps({"type": event_type, "drivers": drivers_list, "stats": stats})
    dead_admins = []
    for ws in admin_connections:
        try: await ws.send_text(msg)
        except: dead_admins.append(ws)
    for ws in dead_admins: admin_connections.remove(ws)

    dead_drivers = []
    for d_name, d_ws in driver_connections.items():
        if d_name in drivers:
            try: await d_ws.send_text(json.dumps({"type": "sync", "me": drivers[d_name]}))
            except: dead_drivers.append(d_name)
    for d in dead_drivers: 
        if d in driver_connections: del driver_connections[d]

async def broadcast_location_update(driver_data: dict):
    """ إرسال تحديثات المواقع للآدمن فقط (لتقليل الضغط) """
    msg = json.dumps({
        "type": "location_update",
        "driver": driver_data["name"],
        "lat": driver_data["lat"],
        "lng": driver_data["lng"],
        "speed": driver_data.get("speed", 0),
        "heading": driver_data.get("heading", 0),
        "distance": driver_data["distance"],
        "last_seen": driver_data["last_seen"]
    })
    dead_admins = []
    for ws in admin_connections:
        try: await ws.send_text(msg)
        except: dead_admins.append(ws)
    for ws in dead_admins: admin_connections.remove(ws)


async def change_driver_state(driver_name: str, new_state: str):
    drivers = await get_drivers_from_redis()
    queue = await get_queue_from_redis()

    if driver_name not in drivers: return
    
    drivers[driver_name]["state"] = new_state
    
    if new_state == "Waiting":
        drivers[driver_name]["last_return"] = datetime.now().strftime("%I:%M %p")
        drivers[driver_name]["break_end"] = None
        if driver_name not in queue: queue.append(driver_name)
    else:
        if driver_name in queue: queue.remove(driver_name)
        if new_state == "Out":
            drivers[driver_name]["last_out"] = datetime.now().strftime("%I:%M %p")
            drivers[driver_name]["orders"] += 1
            drivers[driver_name]["break_end"] = None
        elif new_state == "Break":
            drivers[driver_name]["break_end"] = int(time.time()) + 3600

    await save_driver_to_redis(driver_name, drivers[driver_name])
    await save_queue_to_redis(queue)
    await broadcast_state("update")

@app.post("/api/clear_all")
async def clear_all():
    """
    أدمن يضغط Clear → نمسح كل الدرايفرز من Redis ونقفل كل connections
    """
    msg = json.dumps({"type": "kicked"})
    for d_name, d_ws in list(driver_connections.items()):
        try: await d_ws.send_text(msg)
        except: pass
    driver_connections.clear()

    await redis_client.delete("fleet:drivers")
    await redis_client.delete("fleet:queue")
    await broadcast_state("update")
    return {"ok": True}

@app.post("/api/force_update")
async def force_update():
    msg = json.dumps({"type": "force_refresh"})
    dead = []
    for d_name, d_ws in driver_connections.items():
        try: await d_ws.send_text(msg)
        except: dead.append(d_name)
    for d in dead:
        if d in driver_connections: del driver_connections[d]
    await broadcast_state("update")
    return {"ok": True, "pinged": len(driver_connections)}

class FcmTokenBody(BaseModel):
    name: str
    token: str

@app.post("/api/fcm_token")
async def save_fcm_token(body: FcmTokenBody):
    """السواق بيبعت الـ FCM token لما يفتح الـ App"""
    await redis_client.hset("fleet:fcm_tokens", body.name, body.token)
    return {"ok": True}

class NotifyBody(BaseModel):
    driver: str

@app.post("/api/notify_driver")
async def notify_driver(body: NotifyBody):
    """الأدمن يضغط زرار → إشعار للسواق إن دوره جه"""
    token = await redis_client.hget("fleet:fcm_tokens", body.driver)
    if not token:
        return {"ok": False, "reason": "no_token"}
    if not _FCM_SA_JSON:
        return {"ok": False, "reason": "no_service_account"}

    try:
        access_token = _get_fcm_access_token()
    except Exception as e:
        return {"ok": False, "reason": f"auth_error: {e}"}

    payload = {
        "message": {
            "token": token,
            "data": {
                "type": "your_turn",
                "title": "🚚 دورك جه!",
                "body": f"يا {body.driver}، دورك في الطابور — روح استلم الأوردر!"
            },
            "android": {
                "priority": "high"
            }
        }
    }

    async with httpx.AsyncClient() as client:
        r = await client.post(
            f"https://fcm.googleapis.com/v1/projects/{FIREBASE_PROJECT_ID}/messages:send",
            json=payload,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json"
            },
            timeout=10
        )
    return {"ok": r.status_code == 200, "fcm": r.json()}

def no_cache_html(filepath: str):
    with open(filepath, "rb") as f:
        content = f.read()
    return Response(
        content=content, media_type="text/html",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache", "Expires": "0"
        }
    )

@app.get("/")
async def root(): return no_cache_html("index.html")

@app.get("/join")
async def join_page(): return no_cache_html("join.html")

@app.get("/sw.js")
async def service_worker():
    """Service Worker — يُخدَم بدون cache عشان التحديثات تتطبق فوراً"""
    with open("sw.js", "rb") as f:
        content = f.read()
    return Response(
        content=content,
        media_type="application/javascript",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Service-Worker-Allowed": "/"
        }
    )

@app.get("/manifest.json")
async def manifest():
    """PWA Manifest — بيخلي Android يعاملها كـ app حقيقية"""
    import json as _json
    data = {
        "name": "Rwa7el - رواحل",
        "short_name": "Rwa7el",
        "start_url": "/join",
        "display": "standalone",
        "background_color": "#07070f",
        "theme_color": "#07070f",
        "orientation": "portrait",
        "icons": [
            {"src": "/icon-192.png", "sizes": "192x192", "type": "image/png"},
            {"src": "/icon-512.png", "sizes": "512x512", "type": "image/png"}
        ]
    }
    return Response(
        content=_json.dumps(data),
        media_type="application/json",
        headers={"Cache-Control": "no-store"}
    )

@app.websocket("/ws/admin")
async def admin_ws(ws: WebSocket):
    await ws.accept()
    admin_connections.append(ws)
    
    drivers = await get_drivers_from_redis()
    queue = await get_queue_from_redis()
    await ws.send_text(json.dumps({
        "type": "update", 
        "drivers": get_drivers_list(drivers, queue), 
        "stats": get_dashboard_stats(drivers)
    }))
    
    try:
        while True:
            raw = await ws.receive_text()
            data = json.loads(raw)
            if data["type"] == "reorder":
                await save_queue_to_redis(data["new_queue"])
                await broadcast_state("update")
            elif data["type"] == "admin_change_state":
                await change_driver_state(data["driver"], data["state"])
            elif data["type"] == "kick_driver":
                d_name = data["driver"]
                drivers = await get_drivers_from_redis()
                queue = await get_queue_from_redis()
                if d_name in drivers: await delete_driver_from_redis(d_name)
                if d_name in queue: queue.remove(d_name)
                await save_queue_to_redis(queue)
                
                if d_name in driver_connections:
                    try: await driver_connections[d_name].send_text(json.dumps({"type": "kicked"}))
                    except: pass
                    del driver_connections[d_name]
                await broadcast_state("update")
    except WebSocketDisconnect:
        if ws in admin_connections: admin_connections.remove(ws)

@app.websocket("/ws/driver")
async def driver_ws(ws: WebSocket):
    await ws.accept()
    driver_name = None
    try:
        while True:
            raw = await ws.receive_text()
            data = json.loads(raw)
            
            if data["type"] == "join":
                driver_name = data["name"].strip()
                # لو في connection قديم لنفس الدرايفر (reconnect)، بنبدّله بالجديد بدون أي noise
                driver_connections[driver_name] = ws
                drivers = await get_drivers_from_redis()
                queue = await get_queue_from_redis()
                
                is_new = driver_name not in drivers
                if is_new:
                    # دخول جديد خالص - نسجله من الأول
                    drivers[driver_name] = {
                        "name": driver_name, "state": "Waiting", 
                        "orders": 0, "returns": 0, "misses": 0,
                        "last_out": "-", "last_return": datetime.now().strftime("%I:%M %p"),
                        "battery": "100%", "queue_pos": 0, "distance": None, "break_end": None,
                        "lat": None, "lng": None, "speed": 0, "heading": 0, "last_seen": int(time.time())
                    }
                    await save_driver_to_redis(driver_name, drivers[driver_name])
                else:
                    # reconnect - بنبعتله state الحالي فوراً عشان يعرف هو فين
                    await ws.send_text(json.dumps({"type": "sync", "me": drivers[driver_name]}))

                if driver_name not in queue and drivers[driver_name]["state"] == "Waiting":
                    queue.append(driver_name)
                    await save_queue_to_redis(queue)

                # broadcast عشان الادمن يشوف إن الدرايفر اتوصل تاني
                await broadcast_state("update")
            
            elif data["type"] == "location" and driver_name:
                dist = haversine(data["lat"], data["lng"], BRANCH_LAT, BRANCH_LNG)
                now_ts = int(time.time())
                
                drivers = await get_drivers_from_redis()
                if driver_name in drivers:
                    drivers[driver_name].update({
                        "distance": dist,
                        "lat": data["lat"],
                        "lng": data["lng"],
                        "speed": data.get("speed", 0),
                        "heading": data.get("heading", 0),
                        "last_seen": now_ts
                    })
                    await save_driver_to_redis(driver_name, drivers[driver_name])
                    
                    # إرسال التحديث الصغير للآدمنز فوراً بدون تحميل كامل الداتا
                    await broadcast_location_update(drivers[driver_name])
                    
                    # تحديث المندوب بحالته
                    await ws.send_text(json.dumps({"type": "distance", "meters": dist}))

                    # AUTO-RETURN: لو Out وراجع للفرع (≤200m) → Waiting تلقائي
                    if drivers[driver_name]["state"] == "Out" and dist <= 200:
                        await change_driver_state(driver_name, "Waiting")

            elif data["type"] == "ping":
                await ws.send_text(json.dumps({"type": "pong"}))

            elif data["type"] == "change_state" and driver_name:
                if data["state"] == "Waiting":
                    await change_driver_state(driver_name, "Waiting")

            elif data["type"] == "battery" and driver_name:
                drivers = await get_drivers_from_redis()
                if driver_name in drivers:
                    drivers[driver_name]["battery"] = f"{int(data['level'] * 100)}%"
                    drivers[driver_name]["last_seen"] = int(time.time())  # نحدث last_seen مع كل بطارية
                    await save_driver_to_redis(driver_name, drivers[driver_name])
                    # delta update للـ admins فقط بدل full board re-render
                    battery_msg = json.dumps({
                        "type": "battery_update",
                        "driver": driver_name,
                        "battery": drivers[driver_name]["battery"]
                    })
                    dead = []
                    for aws in admin_connections:
                        try: await aws.send_text(battery_msg)
                        except: dead.append(aws)
                    for aws in dead: admin_connections.remove(aws)

    except WebSocketDisconnect:
        # نمسح الـ connection بس من الـ dict - مش بنغير state ومش بنعمل broadcast
        # الدرايفر يفضل موجود في Redis بنفس state وآخر location وبطارية
        if driver_name and driver_connections.get(driver_name) is ws:
            del driver_connections[driver_name]
