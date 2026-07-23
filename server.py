from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, FileResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import os
import asyncio
import redis.asyncio as aioredis
import json, math, time, httpx
from typing import Dict, List
from datetime import datetime
import google.auth.transport.requests
import google.oauth2.service_account

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.on_event("startup")
async def _start_background_tasks():
    asyncio.create_task(connection_watchdog())
    asyncio.create_task(auto_force_refresh_loop())

@app.get("/health")
async def health_check():
    """
    Healthcheck حقيقي لـ Railway: مش بس "السيرفر بيرد" — لازم يتأكد إن Redis
    فعليًا متاح، لأن كل حاجة في السيستم (drivers, queue, fcm tokens) متخزنة هناك.
    لو Redis واقع أو الاتصال اتقطع، السيرفر بيفضل شغال ورادّ 200 بس فعليًا
    "ميت" وظيفيًا — الـ healthcheck ده بيخلي Railway يعرف يعيد تشغيله بدل ما
    يفضل يعتبره "healthy" غلط.
    """
    try:
        await redis_client.ping()
        return {"status": "ok", "redis": "connected"}
    except Exception as e:
        return Response(
            content=json.dumps({"status": "error", "redis": "unreachable", "detail": str(e)}),
            media_type="application/json",
            status_code=503
        )

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

async def send_fcm_to_driver(driver_name: str, notif_type: str, title: str, body: str) -> bool:
    """
    مساعد عام لإرسال إشعار FCM لمندوب واحد — نفس الـ payload shape المستخدم في كل
    أماكن الإرسال التانية (auto_returned, chat_message, your_turn...). بيرجع True/False
    بس عشان نعرف نسجل/نتصرف، ومبيرميش استثناء لأي فشل (توكن مش موجود، السيرفس أكاونت
    مش متظبط، مشكلة شبكة مؤقتة مع FCM) عشان استخدامه من أماكن زي الـ watchdog
    ماينفعش يوقف حاجة تانية لو فشل.
    """
    if not _FCM_SA_JSON:
        return False
    try:
        fcm_token = await redis_client.hget("fleet:fcm_tokens", driver_name)
        if not fcm_token:
            return False
        access_token = _get_fcm_access_token()
        payload = {
            "message": {
                "token": fcm_token,
                "data": {"type": notif_type, "title": title, "body": body},
                "android": {"priority": "high"}
            }
        }
        async with httpx.AsyncClient() as client:
            r = await client.post(
                f"https://fcm.googleapis.com/v1/projects/{FIREBASE_PROJECT_ID}/messages:send",
                json=payload,
                headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
                timeout=10
            )
        return r.status_code == 200
    except Exception:
        return False

admin_connections: list[WebSocket] = []
driver_connections: Dict[str, WebSocket] = {}
driver_last_activity: Dict[str, float] = {}  # آخر مرة استقبلنا فيها أي رسالة من كل مندوب (ping/location/battery)

driver_disconnected_since: Dict[str, float] = {}  # وقت أول لحظة لاحظنا فيها إن اتصال المندوب انقطع
# (WebSocketDisconnect أو watchdog) — مستخدمة عشان نستنى DISCONNECT_ALERT_GRACE_SECS قبل
# ما نبعت إشعار "الاتصال انقطع"، ونلغيه لو رجع اتصل تاني (join) قبل ما المدة تخلص.
driver_last_disconnect_alert: Dict[str, float] = {}  # آخر مرة فعليًا اتبعت فيها الإشعار
# ده لكل مندوب — عشان الـ cooldown يمنع سبام لو الاتصال فضل يقطع ويرجع بشكل متكرر

driver_last_location_msg: Dict[str, float] = {}  # آخر مرة وصلت فيها رسالة "location" فعلية
# (مش ping/battery) من كل مندوب متصل — منفصلة عن driver_last_activity اللي بتتحدث بأي
# رسالة. الفرق ده هو اللي بيخلينا نكتشف حالة "متصل بس GPS واقف" (شوف STALE_GPS_ALERT_AFTER)
driver_last_stale_gps_alert: Dict[str, float] = {}  # آخر مرة اتبعت فيها إشعار "GPS واقف" لمندوب

driver_rejoin_blocked_until: Dict[str, float] = {}  # اسم المندوب (lowercase) → الوقت اللي
# لحد ما ينفع يعمل join تاني بعده. بيتظبط عند kick أو end_shift، وبيتفحص أول حاجة في الـ
# join handler. الاسم مخزّن lowercase عشان يتماشى مع مقارنة الأسماء التانية في الكود
# (زي is_gps_exempt) ومايفوتش case mismatch.

# قفل واحد لكل عمليات قراءة-تعديل-كتابة على fleet:queue في Redis.
# المشكلة الأصلية: كل مكان كان بيعمل get_queue_from_redis() ثم يعدّل القائمة في الذاكرة
# ثم save_queue_to_redis() — وبين الـ get والـ save فيه await (I/O لـ Redis)، يعني ممكن
# طلبين مختلفين (join / change_state / drag reorder) يقرأوا نفس النسخة القديمة في نفس اللحظة
# وبعدين كل واحد يكتب فوق التاني (lost update) — ده اللي كان بيسبب اختفاء رقم مندوب من
# الطابور (queue_pos بيتحسب من index في الليستة) وترتيب الأرقام المبعثر.
# الحل: أي عملية بتقرا الطابور بنية تعدّله وتحفظه، لازم تحصل كلها جوه نفس الـ lock.
queue_lock = asyncio.Lock()

HEARTBEAT_CHECK_INTERVAL = 10   # كل كام ثانية نفحص الاتصالات
HEARTBEAT_DEAD_AFTER = 35       # لو مفيش نشاط من المندوب لأكتر من كده، نعتبر الاتصال ميت ونقفله
# أقصى وقت اكتشاف = CHECK_INTERVAL + DEAD_AFTER ≈ 45 ثانية (بدل 80 ثانية الأصلية، وبدل 23 ثانية اللي كانت بتفصل كتير)
# المندوب بيبعت ping كل 10 ثواني، يعني هامش أمان كويس (×3.5 تقريبًا) قبل ما نعتبره ميت — بيقلل false positives
# على نت عادي، وبرضه أسرع بكتير من الوضع الأصلي

DISCONNECT_ALERT_GRACE_SECS = 25  # لما اتصال مندوب ينقطع (سواء فجأة WebSocketDisconnect أو
# زومبي عن طريق الـ watchdog)، منبعتش FCM فورًا — ده بيحصل بشكل طبيعي كتير (تطبيق راح
# خلفية، الشبكة اهتزت لحظة) وبيتصلح لوحده بـ reconnect خلال ثواني. بننتظر المدة دي، ولو
# المندوب رجع اتصل تاني خلالها، بنلغي الإشعار. لو معملش reconnect، يبقى غالبًا الـ Service
# اتقفل فعليًا (زي ما بيحصل على شوية أجهزة لما المستخدم يسحب التطبيق من recent apps)
# ووقتها الإشعار هو الطريقة الوحيدة اللي ممكن توصّله يفتح التطبيق تاني.
DISCONNECT_ALERT_COOLDOWN_SECS = 600  # أقل مدة بين إشعارين متتاليين لنفس المندوب، عشان لو
# فضل بيقطع ويرجع (شبكة سيئة مستمرة) منقصفوش بإشعارات كل شوية

STALE_GPS_ALERT_AFTER = 120  # مندوب متصل فعليًا (WebSocket شغال وping بيوصل) بس آخر رسالة
# location وصلت منه من أكتر من كده — الحالة دي مختلفة عن انقطاع الاتصال (driver_ws watchdog
# فوق ده)، لأنها بتحصل لما المستخدم يقفل الـ GPS/Location من إعدادات الجهاز نفسه بينما
# التطبيق والـ Service لسه شغالين عاديين. الـ onProviderDisabled في الجافا مش بيعمل أي حاجة
# (فاضية)، فالسيرفر مش هياخد أي إشارة غير إن location مبقتش توصل رغم إن ping لسه بييجي.
# ده أخطر من انقطاع كامل لأن المندوب بيبان "متصل" عند الأدمن بس بموقع مجمد/قديم.
STALE_GPS_ALERT_COOLDOWN_SECS = 600  # نفس فكرة الـ cooldown بتاع انقطاع الاتصال

REJOIN_BLOCK_SECS = 120  # لما مندوب يتطرد (kick) أو ينهي شيفته، بنمنع أي join جديد بنفس
# الاسم للمدة دي. السبب: زرار "إنهاء الشيفت"/الطرد بيقفل بس الـ WebSocket والـ GPS اللي
# جوه الـ WebView (JS)، لكن مبيوقفش LocationForegroundService.java نفسه (الـ Android bridge
# onDriverLeft مش موجود أصلاً في MainActivity.java، فالنداء ليها بيفشل بصمت) — يعني الـ
# Service الحقيقي فاضل شغال وشايل نفس الاسم القديم، وهيرجع يعمل reconnect ويبعت join
# تلقائي تاني خلال ثواني من غير أي تدخل من المستخدم. المنع المؤقت ده بيدي فرصة إن المستخدم
# (أو النظام) يقفل التطبيق فعليًا قبل ما يقدر "يرجع نفسه" تلقائي بعد الطرد مباشرة.
# ده حل من ناحية السيرفر بس — مش بيوقف الـ Service نفسه، بس بيمنعه يظهر تاني عند الأدمن.

AUTO_FORCE_REFRESH_INTERVAL = 180  # كل 3 دقايق، نعمل force refresh تلقائي لكل المناديب المتصلين
# (نفس اللي بيحصل لما الأدمن يدوس زرار "تحديث فوري" يدويًا) — عشان نضمن إن آخر GPS/بطارية
# محدثة بانتظام حتى لو محدش دوس الزرار

AUTO_OUT_DISTANCE_M = 200  # لو مندوب Waiting وبعيد عن الفرع أكتر من كده، يتحول Out تلقائي (لو وضع Auto مفعّل)
AUTO_OUT_CONFIRM_SECS = 20  # لازم يفضل بره النطاق مستمر للمدة دي قبل ما يتحول Out تلقائي —
# بيحمي من نقطة GPS واحدة كاذبة/قفزة وهمية وهو أصلاً واقف ثابت جوه الفرع (نفس فكرة auto-return)

# --- دوال المساعدة لـ Redis ---
async def get_drivers_from_redis() -> Dict[str, dict]:
    drivers_raw = await redis_client.hgetall("fleet:drivers")
    drivers = {}
    for name, data_str in drivers_raw.items():
        drivers[name] = json.loads(data_str)
    return drivers

async def get_one_driver_from_redis(name: str) -> dict | None:
    """
    HGET لمندوب واحد بس بدل HGETALL لكل المناديب. مهم جدًا لـ location handler
    اللي بينده كل كام ثواني لكل مندوب متصل — مع 30-100 مندوب شغالين سوا،
    HGETALL هنا كان بيبقى عنق زجاجة حقيقي (مئات القراءات الكاملة/ثانية بدل
    قراءة مستهدفة لمندوب واحد بس محتاجينه).
    """
    raw = await redis_client.hget("fleet:drivers", name)
    return json.loads(raw) if raw else None

async def save_driver_to_redis(name: str, data: dict):
    await redis_client.hset("fleet:drivers", name, json.dumps(data))

async def delete_driver_from_redis(name: str):
    await redis_client.hdel("fleet:drivers", name)

async def delete_fcm_token(name: str):
    await redis_client.hdel("fleet:fcm_tokens", name)

async def get_queue_from_redis() -> List[str]:
    queue_str = await redis_client.get("fleet:queue")
    if queue_str:
        return json.loads(queue_str)
    return []

async def save_queue_to_redis(queue_list: List[str]):
    await redis_client.set("fleet:queue", json.dumps(queue_list))

async def get_auto_out_enabled() -> bool:
    val = await redis_client.get("fleet:auto_out")
    return val == "1"

async def set_auto_out_enabled(enabled: bool):
    await redis_client.set("fleet:auto_out", "1" if enabled else "0")

async def save_driver_avatar(name: str, avatar_b64: str):
    await redis_client.hset("fleet:avatars", name, avatar_b64)

async def get_driver_avatar(name: str):
    return await redis_client.hget("fleet:avatars", name)

CHAT_HISTORY_MAX = 200  # أقصى عدد رسائل نحتفظ بيها لكل مندوب (تفادي تضخم الليستة في Redis)

async def append_chat_history(driver_name: str, sender: str, text: str) -> dict:
    """
    بيسجل رسالة (من الإدارة أو من المندوب) في تاريخ الشات الخاص بيه في Redis،
    عشان الـ modal عند الأدمن والمودال عند المندوب يقدروا يعرضوا كل المحادثة
    مش بس آخر رسالة عابرة زي الـ toast/popup.
    sender: "admin" أو "driver"
    """
    entry = {"from": sender, "text": text, "ts": int(time.time())}
    raw = await redis_client.hget("fleet:chat_history", driver_name)
    history = json.loads(raw) if raw else []
    history.append(entry)
    if len(history) > CHAT_HISTORY_MAX:
        history = history[-CHAT_HISTORY_MAX:]
    await redis_client.hset("fleet:chat_history", driver_name, json.dumps(history))
    return entry

async def get_chat_history(driver_name: str) -> List[dict]:
    raw = await redis_client.hget("fleet:chat_history", driver_name)
    return json.loads(raw) if raw else []

async def bump_weekly_stat(name: str, orders_delta: int = 0, km_delta: float = 0.0):

    """يسجل إحصائية يومية للمندوب عشان نعرض تشارت آخر 7 أيام في تاب حسابي"""
    day_key = datetime.now().strftime("%Y-%m-%d")
    field = f"{name}:{day_key}"
    raw = await redis_client.hget("fleet:daily_stats", field)
    cur = json.loads(raw) if raw else {"orders": 0, "km": 0.0}
    cur["orders"] += orders_delta
    cur["km"] = round(cur["km"] + km_delta, 2)
    await redis_client.hset("fleet:daily_stats", field, json.dumps(cur))

async def get_weekly_stats(name: str) -> List[dict]:
    """آخر 7 أيام (من الأقدم للأحدث) — كل يوم فيه orders و km"""
    from datetime import timedelta
    out = []
    today = datetime.now()
    for i in range(6, -1, -1):
        d = today - timedelta(days=i)
        day_key = d.strftime("%Y-%m-%d")
        field = f"{name}:{day_key}"
        raw = await redis_client.hget("fleet:daily_stats", field)
        data = json.loads(raw) if raw else {"orders": 0, "km": 0.0}
        weekday_ar = ["الإثنين","الثلاثاء","الأربعاء","الخميس","الجمعة","السبت","الأحد"][d.weekday()]
        out.append({"date": day_key, "day": weekday_ar, "orders": data["orders"], "km": data["km"]})
    return out


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
    # شبكة أمان: أي مندوب حالته Waiting بس مش موجود في queue (drift قديم أو bug متوقع)
    # مايفضلش من غير رقم في العرض — نديله رقم آخر الطابور بشكل ثابت (مش 0)،
    # مع إنه ده عرض بس هنا ومش بيصلح fleet:queue في Redis نفسه (بيتصلح لوحده عند
    # أي عملية join/change_state تانية جوه الـ lock).
    next_pos = len(queue) + 1
    for name, d in drivers.items():
        if d.get("state") == "Waiting" and name not in queue:
            d["queue_pos"] = next_pos
            next_pos += 1
    # السبب الجذري لمشكلة "الترتيب بيتلخبط عشوائيًا لما حاجة تحصل": drivers هنا جايه من
    # get_drivers_from_redis() اللي بتستخدم HGETALL على fleet:drivers (وده HASH في Redis،
    # مش list). ترتيب مفاتيح HASH في Redis مش جزء من ضمانات الـ API — مش مرتبط بترتيب
    # queue خالص، وممكن يتغير بين استدعاء وتاني لمجرد إن أي HSET حصل على أي مندوب (تحديث
    # GPS/بطارية/حالة، وده بيحصل باستمرار). فكنا بنرجّع list(drivers.values()) بترتيب
    # عشوائي فعليًا من ناحية insertion order في الـ hash، رغم إن queue_pos جوه كل عنصر
    # كان محسوب صح. الفرونت إند بيرتب بـ queue_pos لكن كان الاعتماد الوحيد عليه؛ الأصح
    # إننا نضمن الترتيب من مصدره هنا في السيرفر بدل ما نسيبه بالكامل لطرف العميل.
    result = list(drivers.values())
    result.sort(key=lambda d: (
        0 if d.get("state") == "Waiting" else 1,
        d.get("queue_pos", 0) if isinstance(d.get("queue_pos"), (int, float)) and d.get("queue_pos", 0) > 0 else float("inf"),
        d.get("name", "")
    ))
    return result

async def repair_queue_drift():
    """
    بتصلح أي drift فعليًا في fleet:queue داخل Redis (مش بس وقت العرض) — أي مندوب
    حالته Waiting بس مش موجود في queue array بيترحّل لآخرها. من غير الدالة دي، كل
    ريفرش/تحديث فوري كان بيكشف نفس الـ drift القديم تاني (get_drivers_list كانت
    بتديله رقم احتياطي للعرض بس، من غير ما تصلح المصدر في Redis)، فكان بيبان للأدمن
    إن المندوب ده "بيرجع فوق" رغم إن حد رتّبه صح — لأن كل broadcast كان بيرجّع
    نفس الترتيب المبني على الـ drift القديم من غير أي تصليح حقيقي.
    ماينفعش تتنده من مكان ماسك queue_lock بالفعل (زي جوه change_driver_state أو
    الـ reorder handler) — دول عندهم نفس منطق التصليح مدمج فيهم أصلاً.
    """
    async with queue_lock:
        drivers = await get_drivers_from_redis()
        queue = await get_queue_from_redis()
        changed = False
        for name, d in drivers.items():
            if d.get("state") == "Waiting" and name not in queue:
                queue.append(name)
                changed = True
        if changed:
            await save_queue_to_redis(queue)

async def broadcast_state(event_type="update", reorder_seq=None, reorder_source_ws=None):
    drivers = await get_drivers_from_redis()
    queue = await get_queue_from_redis()
    drivers_list = get_drivers_list(drivers, queue)
    stats = get_dashboard_stats(drivers)
    auto_out = await get_auto_out_enabled()

    base_payload = {"type": event_type, "drivers": drivers_list, "stats": stats, "auto_out": auto_out}
    msg = json.dumps(base_payload)

    # reorder_seq بيتبعت بس لما الـ broadcast جاي من عملية reorder — وبيتبعت بس لصاحب
    # الطلب (reorder_source_ws) مش لكل الأدمنز. السبب: لو فيه أكتر من أدمن فاتحين
    # الداشبورد في نفس الوقت، كل واحد عنده _reorderSeqCounter مستقل في الفرونت إند.
    # لو بعتنا نفس reorder_seq (اللي جاي من client_seq بتاع أدمن معين) لكل الأدمنز،
    # أدمن تاني معندوش أي drag معلّق هيقارن الرقم ده بـ counter بتاعه هو (غالبًا 0 أو
    # رقم مختلف تمامًا) والمقارنة هتبقى بلا معنى — ممكن تسقط update شرعي أو تتقبل
    # واحد لازم يترفض. الحل: صاحب الطلب بس ياخد رسالة فيها reorder_seq (يقارنها مع
    # الـ counter بتاعه هو بالظبط)، وباقي الأدمنز ياخدوا نفس الـ update لكن من غير
    # reorder_seq خالص، فتتطبق عندهم عادي زي أي update تاني (join/kick/state change).
    reorder_msg = None
    if reorder_seq is not None:
        reorder_payload = dict(base_payload)
        reorder_payload["reorder_seq"] = reorder_seq
        reorder_msg = json.dumps(reorder_payload)

    dead_admins = []
    for ws in admin_connections:
        try:
            if reorder_msg is not None and ws is reorder_source_ws:
                await ws.send_text(reorder_msg)
            else:
                await ws.send_text(msg)
        except: dead_admins.append(ws)
    for ws in dead_admins: admin_connections.remove(ws)

    dead_drivers = []
    for d_name, d_ws in driver_connections.items():
        if d_name in drivers:
            avatar = await get_driver_avatar(d_name)
            me = dict(drivers[d_name])
            me["avatar"] = avatar
            try: await d_ws.send_text(json.dumps({"type": "sync", "me": me, "queue": queue}))
            except: dead_drivers.append(d_name)
    for d in dead_drivers: 
        if d in driver_connections: del driver_connections[d]
        driver_last_activity.pop(d, None)

async def broadcast_admin_event(event: str, driver: str, msg: str):
    """ توست بسيط للأدمن بس (join / out / return) — منفصل عن broadcast_state الكامل """
    payload = json.dumps({"type": "admin_event", "event": event, "driver": driver, "msg": msg})
    dead_admins = []
    for ws in admin_connections:
        try: await ws.send_text(payload)
        except: dead_admins.append(ws)
    for ws in dead_admins: admin_connections.remove(ws)

async def broadcast_driver_message_to_admins(driver: str, text: str):
    """ رسالة وصلت من مندوب للإدارة — بتتبعت للأدمنز كـ event مخصوص عشان يتعمل toast
        ويتفتح/يتحدث الـ modal بتاعه لو مفتوح دلوقتي """
    payload = json.dumps({"type": "driver_message", "driver": driver, "text": text, "ts": int(time.time())})
    dead_admins = []
    for ws in admin_connections:
        try: await ws.send_text(payload)
        except: dead_admins.append(ws)
    for ws in dead_admins: admin_connections.remove(ws)

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


async def connection_watchdog():
    """
    فحص دوري لكل اتصالات المناديب — لو اتصال فضل من غير أي نشاط (ping/location/battery)
    لمدة أطول من HEARTBEAT_DEAD_AFTER، نعتبره "زومبي" (السوكيت مفتوح على مستوى النظام
    بس النت فعليًا مقطوع عند المندوب) ونقفله يدويًا. ده بيخلي المندوب يعمل reconnect
    تلقائي بدل ما يفضل عالق (stuck) وشايف رقم مسافة/بطارية قديم من غير أي تحديث.

    نفس الحلقة كمان بتبعت FCM push استباقي للمندوب لو اتصاله فضل مقطوع (سواء اتقفل هنا
    كزومبي، أو اتقفل قبل كده بـ WebSocketDisconnect عادي — مثلاً لو الـ Android قتل الـ
    foreground Service بالكامل، وده بيحصل على شوية أجهزة لما المستخدم يسحب التطبيق من
    recent apps) ومعملش reconnect خلال DISCONNECT_ALERT_GRACE_SECS. الإشعار بيوصله حتى
    لو الـ Service مقفول خالص، لأنه بيوصل عن طريق نظام FCM نفسه مش عن طريق اتصالنا بيه.
    """
    while True:
        await asyncio.sleep(HEARTBEAT_CHECK_INTERVAL)
        now = time.time()

        # 1) زومبي: اتصال مفتوح بس مفيش نشاط عليه من زمان — نقفله يدويًا
        stale = [
            name for name, ws in list(driver_connections.items())
            if now - driver_last_activity.get(name, now) > HEARTBEAT_DEAD_AFTER
        ]
        for name in stale:
            ws = driver_connections.get(name)
            if ws:
                try:
                    await ws.close(code=1000)
                except Exception:
                    pass
            driver_connections.pop(name, None)
            driver_last_activity.pop(name, None)
            driver_disconnected_since.setdefault(name, now)

        # 2) أي مندوب معندوش اتصال مفتوح دلوقتي (سواء اتقفل فوق كزومبي، أو اتقفل قبل
        # كده بـ WebSocketDisconnect عادي) — لسه محسوبين "منقطعين". نبدأ نعدّلهم مهلة
        # DISCONNECT_ALERT_GRACE_SECS؛ لو رجعوا اتصلوا (join) هيتشالوا من الـ dict دي هناك.
        drivers_cache = None  # نجيبها مرة واحدة بس لو احتجناها فعلاً، مش لكل مندوب
        for name in list(driver_disconnected_since.keys()):
            if name in driver_connections:
                # رجع اتصل فعلاً — الغي أي حساب انقطاع قديم (احتياط، الأصل بيتشال في join)
                driver_disconnected_since.pop(name, None)
                continue

            disconnected_at = driver_disconnected_since[name]
            if now - disconnected_at < DISCONNECT_ALERT_GRACE_SECS:
                continue  # لسه جوه فترة السماح — ممكن يرجع لوحده

            last_alert = driver_last_disconnect_alert.get(name, 0)
            if now - last_alert < DISCONNECT_ALERT_COOLDOWN_SECS:
                continue  # اتبعتله إشعار قريب، منكررش

            # اتأكد إنه لسه "شغال" فعليًا (مش خلص شيفت ومش اتشال) قبل ما نزعجه بإشعار
            if drivers_cache is None:
                drivers_cache = await get_drivers_from_redis()
            if name not in drivers_cache:
                driver_disconnected_since.pop(name, None)
                driver_last_disconnect_alert.pop(name, None)
                continue

            driver_last_disconnect_alert[name] = now
            await send_fcm_to_driver(
                name, "connection_lost",
                "📡 الاتصال انقطع",
                "الاتصال بالسيرفر انقطع — افتح التطبيق عشان موقعك يرجع يتحدث"
            )

        # 3) مندوب متصل فعليًا (WebSocket شغال، ping بيوصل) بس آخر location وصل منه قديم —
        # الحالة دي بتحصل لو المستخدم قفل GPS/Location من إعدادات الجهاز، لأن الجافا مش
        # بتاخد أي إشارة من onProviderDisabled (فاضية) عشان تتصرف أو تبلّغنا. من غير الفحص
        # ده، المندوب ده هيفضل يبان "متصل" عند الأدمن بس بموقع مجمد — أخطر من انقطاع واضح.
        for name, ws in list(driver_connections.items()):
            last_loc = driver_last_location_msg.get(name)
            if last_loc is None:
                continue  # لسه ماوصلش أي location منه أصلاً (لحظات الـ join الأولى)، سيبه
            if now - last_loc < STALE_GPS_ALERT_AFTER:
                continue  # location لسه طازة

            last_alert = driver_last_stale_gps_alert.get(name, 0)
            if now - last_alert < STALE_GPS_ALERT_COOLDOWN_SECS:
                continue  # اتبعتله إشعار قريب، منكررش

            if drivers_cache is None:
                drivers_cache = await get_drivers_from_redis()
            if name not in drivers_cache:
                driver_last_location_msg.pop(name, None)
                driver_last_stale_gps_alert.pop(name, None)
                continue

            driver_last_stale_gps_alert[name] = now
            await send_fcm_to_driver(
                name, "stale_gps",
                "📍 الموقع مش بيتحدث",
                "موقعك واقف من فترة — اتأكد إن خدمة الـ GPS/Location مفعّلة في إعدادات الجهاز"
            )

async def change_driver_state(driver_name: str, new_state: str):
    drivers = await get_drivers_from_redis()

    if driver_name not in drivers: return

    prev_state = drivers[driver_name].get("state")
    drivers[driver_name]["state"] = new_state

    if new_state == "Waiting":
        drivers[driver_name]["break_end"] = None
        # الأوردر بيتحسب هنا — لما المندوب يرجع من Out (يعني خلص التوصيل)، مش لما يخرج
        if prev_state == "Out":
            drivers[driver_name]["orders"] += 1
            await bump_weekly_stat(driver_name, orders_delta=1)
    else:
        if new_state == "Out":
            drivers[driver_name]["out_since"] = int(time.time())
            drivers[driver_name]["break_end"] = None
        elif new_state == "Break":
            drivers[driver_name]["break_end"] = int(time.time()) + 3600

    await save_driver_to_redis(driver_name, drivers[driver_name])

    async with queue_lock:
        queue = await get_queue_from_redis()
        if new_state == "Waiting":
            # SAFETY NET — قبل ما نضيف driver_name نفسه، نتأكد الأول إن مفيش أي مندوب تاني
            # حالته Waiting فعلاً بس "drifted" برة الـ queue array (يعني عنده queue_pos=0
            # ضمنيًا رغم إنه فعليًا مستني). لو سبنا الحالة دي وأضفنا driver_name بـ append()،
            # هو هياخد مكان في queue قبل المندوب الـ drifted (لأنه هيبان بره الـ queue تمامًا)،
            # فيبان بصريًا إن الراجع من الأوردر "قفز" فوق مندوب كان مستني فعلاً من قبله —
            # وده بالظبط العرض اللي كان بيحصل. تصليح الـ drift هنا (جوه نفس الـ lock، قبل
            # append الحالي) بيضمن إن أصحاب الأولوية الأقدم يترتبوا صح قبل أي إضافة جديدة.
            all_drivers = await get_drivers_from_redis()
            for other_name, other_data in all_drivers.items():
                if (other_name != driver_name
                        and other_data.get("state") == "Waiting"
                        and other_name not in queue):
                    queue.append(other_name)
            if driver_name not in queue: queue.append(driver_name)
        else:
            if driver_name in queue: queue.remove(driver_name)
        await save_queue_to_redis(queue)
        # الـ broadcast لازم يحصل وهو لسه ماسك الـ lock، عشان لو مندوبين رجعوا من
        # أوردر في نفس اللحظة (زي auto-return بتاع اتنين قريبين من بعض)، كل واحد
        # بيبعت للأدمن snapshot كامل ومتسق (queue + drivers متزامنين مع بعض)، مش نص
        # تعديل. لو الـ broadcast كان بره الـ lock، ممكن يوصل broadcast لمندوب A
        # وهو لسه شايل نسخة الطابور القديمة لحظة ما B كان بيعدّل بره الـ lock —
        # فالأدمن كان بياخد رسالتين متتاليتين بترتيب متضارب وده اللي كان بيبان
        # كـ"لخبطة" في الترتيب لحظة رجوع مندوب من أوردر.
        await broadcast_state("update")

    # توست للأدمن بالتغيير اللي حصل — منفصل عن broadcast_state عشان يبقى event واضح
    # نستخدم prev_state عشان نميّز "رجع من أوردر" عن "رجع من بريك"
    if new_state == "Out":
        await broadcast_admin_event("out", driver_name, f"🚀 {driver_name} خرج لأوردر")
    elif new_state == "Waiting" and prev_state == "Out":
        await broadcast_admin_event("returned", driver_name, f"✅ {driver_name} رجع من الأوردر")
    elif new_state == "Waiting" and prev_state == "Break":
        await broadcast_admin_event("returned_break", driver_name, f"☕ {driver_name} رجع من البريك")

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
    driver_last_activity.clear()
    driver_disconnected_since.clear()
    driver_last_disconnect_alert.clear()
    driver_last_location_msg.clear()
    driver_last_stale_gps_alert.clear()

    await redis_client.delete("fleet:drivers")
    await redis_client.delete("fleet:queue")
    await redis_client.delete("fleet:fcm_tokens")  # امسح كل التوكنز عشان محدش ياخد إشعار وهو مش مسجل
    await broadcast_state("update")
    return {"ok": True}

async def do_force_refresh():
    # المنطق الفعلي لعمل "تحديث فوري" — مستخدم من الزرار اليدوي في شاشة الأدمن
    # وكمان من الدورة التلقائية كل AUTO_FORCE_REFRESH_INTERVAL
    ws_msg = json.dumps({"type": "force_refresh"})
    dead = []
    for d_name, d_ws in driver_connections.items():
        try: await d_ws.send_text(ws_msg)
        except: dead.append(d_name)
    for d in dead:
        if d in driver_connections: del driver_connections[d]
        driver_last_activity.pop(d, None)
    # ننتظر شوية عشان نديله فرصة حقيقية إن الموبايلات المتصلة تبعت مواقعها الجديدة
    # (لو المندوب بيبعت location_update عادي، ده هيوصل خلال الفترة دي ويحدث Redis)
    await asyncio.sleep(2.5)
    # نصلح أي drift في الطابور قبل الـ broadcast — عشان "تحديث فوري" ميكشفش drift قديم
    # تاني من غير ما يصلحه فعليًا في Redis (كان بيبان إن مندوب معين "بيرجع فوق" لوحده)
    await repair_queue_drift()
    # دلوقتي نعمل broadcast بأحدث داتا في Redis (بعد ما استنينا)
    await broadcast_state("update")
    return len(driver_connections)

async def auto_force_refresh_loop():
    """
    دورة تلقائية بتعمل force refresh لكل المناديب المتصلين كل AUTO_FORCE_REFRESH_INTERVAL
    (نفس فعل زرار "تحديث فوري" اليدوي)، عشان نضمن إن آخر GPS/بطارية محدثة بانتظام
    حتى لو محدش من الأدمنز دوس الزرار يدويًا.
    """
    while True:
        await asyncio.sleep(AUTO_FORCE_REFRESH_INTERVAL)
        try:
            await do_force_refresh()
        except Exception:
            pass

@app.post("/api/force_update")
async def force_update():
    # WebSocket بس — للمناديب اللي connected دلوقتي (من غير أي إشعار FCM يوصل للمندوب)
    connected_count = await do_force_refresh()
    return {"ok": True, "ws": connected_count}

class AvatarBody(BaseModel):
    name: str
    avatar: str  # base64 data URL (data:image/...;base64,...)

@app.post("/api/avatar")
async def upload_avatar(body: AvatarBody):
    if not body.name.strip() or not body.avatar.strip():
        return {"ok": False, "reason": "missing_fields"}
    await save_driver_avatar(body.name.strip(), body.avatar)
    # حدث الأدمن والمندوب نفسه فوراً
    await broadcast_state("update")
    return {"ok": True}

@app.get("/api/avatar/{name}")
async def fetch_avatar(name: str):
    avatar = await get_driver_avatar(name)
    return {"ok": True, "avatar": avatar}

@app.get("/api/weekly_stats/{name}")
async def weekly_stats(name: str):
    stats = await get_weekly_stats(name)
    return {"ok": True, "stats": stats}

class FcmTokenBody(BaseModel):
    name: str
    token: str

@app.post("/api/fcm_token")
async def save_fcm_token(body: FcmTokenBody):
    """السواق بيبعت الـ FCM token لما يفتح الـ App"""
    await redis_client.hset("fleet:fcm_tokens", body.name, body.token)
    # نفس تسجيل الـ join — احتياط إضافي عشان أي مندوب وصل الـ FCM token بتاعه يتسجل
    # في القائمة الدائمة حتى لو لسبب ما رسالة join نفسها فاتت
    await redis_client.hset("fleet:all_drivers_registry", body.name, int(time.time()))
    return {"ok": True}

class ChatBody(BaseModel):
    text: str

@app.post("/api/chat")
async def send_chat(body: ChatBody):
    if not body.text.strip():
        return {"ok": False, "reason": "empty"}
    msg = json.dumps({"type": "chat_message", "text": body.text.strip()})
    dead = []
    sent_ws = []
    for name, dws in driver_connections.items():
        try:
            await dws.send_text(msg)
            sent_ws.append(name)
        except:
            dead.append(name)
        await append_chat_history(name, "admin", body.text.strip())
    for d in dead:
        if d in driver_connections: del driver_connections[d]
        driver_last_activity.pop(d, None)
    sent_fcm = []
    if _FCM_SA_JSON:
        try:
            access_token = _get_fcm_access_token()
            all_tokens = await redis_client.hgetall("fleet:fcm_tokens")
            async with httpx.AsyncClient() as client:
                for name, token in all_tokens.items():
                    r = await client.post(
                        f"https://fcm.googleapis.com/v1/projects/{FIREBASE_PROJECT_ID}/messages:send",
                        json={"message": {"token": token, "data": {"type": "chat_message", "title": "📢 رسالة من الإدارة", "body": body.text.strip()}, "android": {"priority": "high"}}},
                        headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
                        timeout=10
                    )
                    if r.status_code == 200: sent_fcm.append(name)
        except: pass
    return {"ok": True, "ws": sent_ws, "fcm": sent_fcm}

class KickBody(BaseModel):
    driver: str

@app.post("/api/kick_driver")
async def kick_driver_http(body: KickBody):
    """نفس فعل kick_driver اللي بيتبعت عن طريق الـ WebSocket، لكن كـ HTTP endpoint —
       fallback لو اتصال الأدمن الـ WebSocket مش شغال وقت الضغط على الزرار."""
    d_name = body.driver.strip()
    if not d_name:
        return {"ok": False, "reason": "missing_driver"}
    drivers = await get_drivers_from_redis()
    was_present = d_name in drivers
    if was_present:
        await delete_driver_from_redis(d_name)
    async with queue_lock:
        queue = await get_queue_from_redis()
        if d_name in queue:
            queue.remove(d_name)
            await save_queue_to_redis(queue)
        await delete_fcm_token(d_name)

        notified_ws = False
        if d_name in driver_connections:
            try:
                await driver_connections[d_name].send_text(json.dumps({"type": "kicked"}))
                notified_ws = True
            except:
                pass
            del driver_connections[d_name]
        driver_last_activity.pop(d_name, None)
        driver_disconnected_since.pop(d_name, None)
        driver_last_disconnect_alert.pop(d_name, None)
        driver_last_location_msg.pop(d_name, None)
        driver_last_stale_gps_alert.pop(d_name, None)
        driver_rejoin_blocked_until[d_name.lower()] = time.time() + REJOIN_BLOCK_SECS
        await broadcast_state("update")
    return {"ok": True, "was_present": was_present, "notified_ws": notified_ws}

@app.get("/api/all_drivers")
async def all_drivers_endpoint():
    """
    كل اسم مندوب دخل النظام مرة على الأقل (من fleet:all_drivers_registry — سجل دائم
    مش بيتمسح مع end_shift/kick/clear_all)، مع علامة هل هو موجود حاليًا في الشيفت
    النشط (fleet:drivers) ولا لأ. ده الأساس لتاب "كل المناديب" في الأدمن.
    """
    registry = await redis_client.hgetall("fleet:all_drivers_registry")
    active = await get_drivers_from_redis()
    has_fcm = await redis_client.hkeys("fleet:fcm_tokens")
    has_fcm_set = set(has_fcm)
    result = []
    for name, first_seen in registry.items():
        result.append({
            "name": name,
            "online": name in active,
            "state": active.get(name, {}).get("state") if name in active else None,
            "has_fcm_token": name in has_fcm_set,  # لو False، زرار ADD مش هينفع يبعتله إشعار فعلي
            "first_seen": int(first_seen) if str(first_seen).isdigit() else None,
        })
    result.sort(key=lambda d: d["name"].lower())
    return {"ok": True, "drivers": result}

class AddDriverBody(BaseModel):
    driver: str

@app.post("/api/add_driver")
async def add_driver_endpoint(body: AddDriverBody):
    """
    زرار ADD في تاب "كل المناديب" — بيحط مندوب مش شغال دلوقتي في Waiting فورًا
    ويبعتله FCM يطلب منه يفتح التطبيق. الـ Service عند المندوب مش بيتشغّل عن بعد —
    ده بيحصل بس لما هو فعليًا يفتح التطبيق بنفسه (نفس مبدأ الموافقة الأصلية).
    """
    d_name = body.driver.strip()
    if not d_name:
        return {"ok": False, "reason": "missing_driver"}

    registry = await redis_client.hgetall("fleet:all_drivers_registry")
    if d_name not in registry:
        return {"ok": False, "reason": "not_registered"}  # مش من الأسماء اللي سبق دخلت النظام

    drivers = await get_drivers_from_redis()
    if d_name in drivers:
        return {"ok": False, "reason": "already_active"}  # شغال بالفعل، الزرار مالوش لازمة هنا

    now_ts = int(time.time())
    drivers[d_name] = {
        "name": d_name, "state": "Waiting",
        "orders": 0, "returns": 0, "misses": 0,
        "battery": None, "queue_pos": 0, "distance": None, "break_end": None,
        # مفيش lat/lng حقيقية لسه — المندوب مش متصل، هتتظبط أول ما يفتح التطبيق
        # ويبعت أول رسالة location فعلية. مش بنحط قيمة وهمية عشان ميخربش حسابات
        # الكيلومترات/المسافة (implied_kmh هيتحسب غلط لو بدأنا من نقطة مش حقيقية)
        "lat": None, "lng": None,
        "speed": 0, "heading": 0, "last_seen": now_ts,
        "shift_km": 0.0, "_last_gps": None,
        "added_by_admin": True,  # علامة للتفرقة عن دخول عادي، لو احتجنا نميزها في الواجهة لاحقًا
    }
    await save_driver_to_redis(d_name, drivers[d_name])

    async with queue_lock:
        queue = await get_queue_from_redis()
        if d_name not in queue:
            queue.append(d_name)
            await save_queue_to_redis(queue)
        await broadcast_state("update")

    await broadcast_admin_event("added_by_admin", d_name, f"🟢 {d_name} اتضاف للطابور من الإدارة")

    fcm_sent = await send_fcm_to_driver(
        d_name, "added_to_queue",
        "🟢 دخلت الدور",
        "الإدارة ضافتك في الطابور — افتح التطبيق عشان تبدأ تستقبل الأوردرات"
    )
    return {"ok": True, "fcm_sent": fcm_sent}

@app.get("/api/chat_history/{driver}")
async def chat_history_endpoint(driver: str):
    """
    كل المحادثة (إدارة + مندوب) بترتيب زمني — بيستخدمها الـ modal عند الأدمن
    (لما يدوس على مندوب) وعند المندوب (زرار "رسالة من الإدارة").
    """
    history = await get_chat_history(driver.strip())
    return {"ok": True, "driver": driver.strip(), "history": history}

@app.get("/api/debug_connections")
async def debug_connections():
    """للتشخيص بس: بيوري مين فعليًا متسجل كـ WebSocket نشط عند السيرفر دلوقتي،
       عشان نتأكد إن اسم المندوب اللي الأدمن بيبعته مطابق تمامًا للاسم المسجل هنا."""
    return {
        "connected_driver_names": list(driver_connections.keys()),
        "count": len(driver_connections)
    }

class ChatDriverBody(BaseModel):
    driver: str
    text: str

@app.post("/api/chat_driver")
async def send_chat_to_driver(body: ChatDriverBody):
    """رسالة خاصة لمندوب واحد بس (مش broadcast) — بتظهر عنده هو بس في الـ App"""
    driver_name = body.driver.strip()
    text = body.text.strip()
    if not driver_name or not text:
        return {"ok": False, "reason": "missing_fields"}

    await append_chat_history(driver_name, "admin", text)
    msg = json.dumps({"type": "chat_message", "text": text})

    sent_ws = False
    ws_existed = driver_name in driver_connections
    dws = driver_connections.get(driver_name)
    if dws:
        try:
            await dws.send_text(msg)
            sent_ws = True
        except:
            if driver_name in driver_connections: del driver_connections[driver_name]
            driver_last_activity.pop(driver_name, None)

    sent_fcm = False
    had_token = False
    if _FCM_SA_JSON:
        try:
            token = await redis_client.hget("fleet:fcm_tokens", driver_name)
            had_token = bool(token)
            if token:
                access_token = _get_fcm_access_token()
                async with httpx.AsyncClient() as client:
                    r = await client.post(
                        f"https://fcm.googleapis.com/v1/projects/{FIREBASE_PROJECT_ID}/messages:send",
                        json={"message": {"token": token, "data": {"type": "chat_message", "title": "📢 رسالة من الإدارة", "body": text}, "android": {"priority": "high"}}},
                        headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
                        timeout=10
                    )
                    if r.status_code == 200: sent_fcm = True
        except:
            pass

    if not sent_ws and not sent_fcm:
        return {"ok": False, "reason": "driver_unreachable", "ws_existed": ws_existed, "had_token": had_token}
    return {"ok": True, "ws": sent_ws, "fcm": sent_fcm, "ws_existed": ws_existed}

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

@app.get("/download")
async def download_page(): return no_cache_html("download.html")

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

    # نصلح أي drift في الطابور أول ما الأدمن يفتح/يعمل ريفرش للصفحة — عشان الريفرش
    # العادي (F5) برضه يصلح المشكلة فورًا، مش بس "تحديث فوري"/الدورة التلقائية
    await repair_queue_drift()

    drivers = await get_drivers_from_redis()
    queue = await get_queue_from_redis()
    await ws.send_text(json.dumps({
        "type": "update", 
        "drivers": get_drivers_list(drivers, queue), 
        "stats": get_dashboard_stats(drivers),
        "auto_out": await get_auto_out_enabled()
    }))
    
    try:
        while True:
            raw = await ws.receive_text()
            data = json.loads(raw)
            if data["type"] == "reorder":
                async with queue_lock:
                    current_queue = await get_queue_from_redis()
                    # SAFETY NET (نفس فكرة change_driver_state): قبل ما نقارن مع new_queue،
                    # نتأكد إن current_queue متضمن أي مندوب Waiting فعلاً بس drifted برة الـ
                    # array (يعني set(new_queue) مش هيتطابق أبدًا وهيترفض السحب اليدوي بصمت،
                    # فيبان للأدمن إنه رتّب بس الترتيب "رجع لوحده" — ده كان سبب المشكلة).
                    all_drivers = await get_drivers_from_redis()
                    for other_name, other_data in all_drivers.items():
                        if other_data.get("state") == "Waiting" and other_name not in current_queue:
                            current_queue.append(other_name)
                    new_queue = data["new_queue"]
                    # لازم new_queue يبقى نفس أعضاء الطابور الحالي بالظبط (نفس الـ set) —
                    # مجرد إعادة ترتيب، مش استبدال. لو الأدمن كان بيسحب وقت ما مندوب
                    # اتضاف/اتشال من الطابور من عملية تانية (join / state change) في نفس اللحظة،
                    # الـ new_queue اللي جاي من الـ DOM القديم ممكن يكون ناقص أو زيادة —
                    # وقتها كنا بنستبدل الطابور بالكامل ونمسح مناديب بالغلط.
                    # الحل: لو الـ set مش متطابق، نرفض ونسيب الطابور الحالي زي ما هو
                    # (وبنعمل broadcast بالحالة الصح عشان الأدمن ياخد آخر تحديث فورًا).
                    if set(new_queue) == set(current_queue):
                        await save_queue_to_redis(new_queue)
                    else:
                        # مفيش تطابق حتى بعد تصليح الـ drift — نحفظ current_queue المُصلّح
                        # على الأقل (بدل ما نسيب الـ drift زي ما هو من غير أي تصليح)
                        await save_queue_to_redis(current_queue)
                    # لو الأدمن بعت أكتر من drag بسرعة، ردود reorder ممكن توصل بترتيب
                    # مختلف عن ترتيب الإرسال (استقبال/معالجة async، تأخير شبكة). بنرجّع
                    # client_seq اللي الأدمن بعته مع الطلب عشان العميل يعرف يتجاهل أي رد
                    # لعملية reorder أقدم لو وصل بعد رد لعملية أحدث. ده اللي كان بيسبب
                    # "الرقم #N بييجي من عملية، وترتيب الكارت في القايمة بييجي من عملية تانية".
                    await broadcast_state("update", reorder_seq=data.get("client_seq"), reorder_source_ws=ws)
            elif data["type"] == "admin_change_state":
                await change_driver_state(data["driver"], data["state"])
            elif data["type"] == "set_auto_out":
                await set_auto_out_enabled(bool(data.get("enabled")))
                await broadcast_state("update")
            elif data["type"] == "ping":
                # رد بسيط عشان الـ watchdog في المتصفح يعرف إن الاتصال لسه حي فعليًا،
                # حتى لو مفيش أي تغيير في البيانات نفسها
                await ws.send_text(json.dumps({"type": "pong"}))
            elif data["type"] == "kick_driver":
                d_name = data["driver"]
                drivers = await get_drivers_from_redis()
                if d_name in drivers: await delete_driver_from_redis(d_name)
                async with queue_lock:
                    queue = await get_queue_from_redis()
                    if d_name in queue: queue.remove(d_name)
                    await save_queue_to_redis(queue)
                    await delete_fcm_token(d_name)  # امسح التوكن عشان ميوصلوش إشعارات وهو مش شغال

                    if d_name in driver_connections:
                        try: await driver_connections[d_name].send_text(json.dumps({"type": "kicked"}))
                        except: pass
                        del driver_connections[d_name]
                    driver_last_activity.pop(d_name, None)
                    driver_disconnected_since.pop(d_name, None)
                    driver_last_disconnect_alert.pop(d_name, None)
                    driver_last_location_msg.pop(d_name, None)
                    driver_last_stale_gps_alert.pop(d_name, None)
                    driver_rejoin_blocked_until[d_name.lower()] = time.time() + REJOIN_BLOCK_SECS
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

            if driver_name:
                driver_last_activity[driver_name] = time.time()

            if data["type"] == "join":
                driver_name = data["name"].strip()

                # ── منع rejoin تلقائي بعد kick/end_shift مباشرة ──
                # الـ Android Service الحقيقي عند المندوب (لو الطرد/الإنهاء كان بسبب
                # مشكلة في التطبيق أو ضغطة غلط) ممكن يرجع يبعت join تلقائي خلال ثواني
                # حتى لو الواجهة رجعت لشاشة اللوجين — لأن الإيقاف الفعلي بيحصل بس على
                # مستوى الـ WebView JS مش على مستوى الـ Service. نرفض أي محاولة هنا
                # لحد ما نافذة المنع تخلص.
                block_key = driver_name.lower()
                blocked_until = driver_rejoin_blocked_until.get(block_key)
                if blocked_until and time.time() < blocked_until:
                    remaining = int(blocked_until - time.time())
                    await ws.send_text(json.dumps({
                        "type": "join_rejected",
                        "reason": "recently_removed",
                        "retry_after": remaining,
                        "msg": f"❌ تم إنهاء شيفتك للتو — حاول تاني بعد {remaining} ثانية"
                    }))
                    driver_name = None
                    continue

                drivers = await get_drivers_from_redis()

                is_reconnect = driver_name in drivers

                # ── سجل دائم لكل اسم مندوب دخل النظام مرة، مش بيتمسح أبدًا (حتى مع
                # end_shift/kick/clear_all) — الأساس اللي عليه تاب "كل المناديب" في
                # الأدمن، عشان نقدر نضيف مندوب من غير ما يكون شغال دلوقتي.
                await redis_client.hset("fleet:all_drivers_registry", driver_name, int(time.time()))

                # ── Proximity check (فقط للـ join الجديد مش reconnect) ──
                is_gps_exempt = driver_name.lower() == "bankai225"
                if not is_reconnect and not is_gps_exempt:
                    join_lat = data.get("lat")
                    join_lng = data.get("lng")
                    if join_lat is None or join_lng is None:
                        # السواق ما بعتش موقعه — ارفض
                        await ws.send_text(json.dumps({
                            "type": "join_rejected",
                            "reason": "no_location",
                            "msg": "❌ لازم تسمح بالـ GPS قبل ما تبدأ الشيفت"
                        }))
                        continue
                    dist_to_branch = haversine(join_lat, join_lng, BRANCH_LAT, BRANCH_LNG)
                    if dist_to_branch > 50:
                        await ws.send_text(json.dumps({
                            "type": "join_rejected",
                            "reason": "too_far",
                            "dist": dist_to_branch,
                            "msg": f"❌ أنت بعيد عن الفرع ({dist_to_branch}م) — لازم تكون في نطاق 50م"
                        }))
                        continue
                # ── ✅ مقبول ──

                # لو في connection قديم لنفس الدرايفر (reconnect)، بنبدّله بالجديد بدون أي noise
                driver_connections[driver_name] = ws
                driver_last_activity[driver_name] = time.time()
                # اتصل تاني فعليًا — نلغي أي عداد انقطاع معلّق ونصفّر الـ cooldown، عشان لو
                # حصل انقطاع حقيقي تاني بعد كده نقدر نبعت إشعار جديد وميتحجبش بسبب الوقت القديم
                driver_disconnected_since.pop(driver_name, None)
                driver_last_disconnect_alert.pop(driver_name, None)
                # نفس الفكرة لتتبع الـ GPS — join/reconnect دايمًا بيجيب lat/lng، فده يعتبر
                # "location طازة" لحد ما نشوف عكس ذلك، ومنفضلش شايلين تنبيه GPS قديم من قبل
                driver_last_location_msg[driver_name] = time.time()
                driver_last_stale_gps_alert.pop(driver_name, None)

                if driver_name not in drivers:
                    # دخول جديد خالص - نسجله من الأول
                    drivers[driver_name] = {
                        "name": driver_name, "state": "Waiting", 
                        "orders": 0, "returns": 0, "misses": 0,
                        "battery": "100%", "queue_pos": 0, "distance": None, "break_end": None,
                        "lat": data.get("lat"), "lng": data.get("lng"),
                        "speed": 0, "heading": 0, "last_seen": int(time.time()),
                        "shift_km": 0.0, "_last_gps": {"lat": data.get("lat"), "lng": data.get("lng"), "ts": int(time.time())}
                    }
                    await save_driver_to_redis(driver_name, drivers[driver_name])

                # كل قراءة-تعديل-كتابة على الطابور بتحصل هنا جوه الـ lock كوحدة واحدة،
                # عشان مفيش طلب تاني (state change / drag reorder / join تاني) يقرا نسخة
                # قديمة من الطابور ويكتب فوق التعديل ده (lost update).
                async with queue_lock:
                    queue = await get_queue_from_redis()
                    if not is_reconnect:
                        # دخول جديد خالص بس — نضيفه آخر الطابور
                        if driver_name not in queue:
                            queue.append(driver_name)
                            await save_queue_to_redis(queue)
                    elif driver_name not in queue and drivers[driver_name]["state"] == "Waiting":
                        # حالة استثنائية: مندوب موجود أصلاً وحالته Waiting بس مش موجود في الطابور
                        # (يعني حصل خلل ما وسبب فقدانه من الـ queue list) — نرجّعه تاني كمعالجة أمان،
                        # لكن ده مش المفروض يحصل في الـ reconnect العادي
                        queue.append(driver_name)
                        await save_queue_to_redis(queue)

                    # broadcast عشان الادمن يشوف إن الدرايفر اتوصل تاني — جوه الـ lock عشان
                    # لو مندوبين بيعملوا join في نفس اللحظة، كل broadcast يبقى متسق مع آخر تعديل مؤكد
                    await broadcast_state("update")

                if is_reconnect:
                    # reconnect - بنبعتله state الحالي فوراً عشان يعرف هو فين (بعد أي تعديل فوق)
                    queue = await get_queue_from_redis()
                    avatar = await get_driver_avatar(driver_name)
                    me = dict(drivers[driver_name])
                    me["avatar"] = avatar
                    await ws.send_text(json.dumps({"type": "sync", "me": me, "queue": queue}))

                # توست للأدمن بس لو دخول جديد فعلي (مش reconnect) — عشان الريلود ماتعملش توست كل مرة
                if not is_reconnect:
                    await broadcast_admin_event("joined", driver_name, f"🟢 {driver_name} سجّل ودخل الطابور")
            
            elif data["type"] == "location" and driver_name:
                driver_last_location_msg[driver_name] = time.time()
                now_ts = int(time.time())
                
                # HGET لمندوب واحد بس بدل HGETALL لكل الأسطول — الـ handler ده بينده
                # كل كام ثواني لكل مندوب متصل (GPS ping)، فمع 30-100 مندوب سوا كان
                # ده أكبر عنق زجاجة في السيستم.
                driver = await get_one_driver_from_redis(driver_name)
                if driver is not None:
                    # حساب الكيلومترات التراكمية من آخر نقطة GPS معروفة (تصفيته من نويز الـ GPS الثابت)
                    last_gps = driver.get("_last_gps")
                    shift_km = driver.get("shift_km", 0.0)
                    implied_kmh = None
                    if last_gps and last_gps.get("lat") is not None and last_gps.get("lng") is not None:
                        step_m = haversine(last_gps["lat"], last_gps["lng"], data["lat"], data["lng"])
                        dt = max(1, now_ts - last_gps.get("ts", now_ts))
                        implied_kmh = (step_m / dt) * 3.6

                    # فحص إضافي ضد "قفزة مزدوجة" (double-jump): قفزة GPS وهمية لمكان بعيد ممكن
                    # تفلت من فحص implied_kmh العادي لو الفرق الزمني بينها وبين النقطة اللي قبلها
                    # كان كبير كفاية (implied_kmh يطلع معقول رغم إن النقلة وهمية). نتأكد كمان إن
                    # النقطة الجديدة قريبة من آخر موقع "معروض" (مفلتر) مش بس من آخر نقطة خام —
                    # لو بعيدة عن الاتنين مع بعض بسرعة عالية، يبقى تأكيد أقوى إنها outlier فعلاً.
                    prev_display_lat = driver.get("display_lat", driver.get("lat"))
                    prev_display_lng = driver.get("display_lng", driver.get("lng"))
                    step_from_display_m = None
                    if prev_display_lat is not None and prev_display_lng is not None:
                        step_from_display_m = haversine(prev_display_lat, prev_display_lng, data["lat"], data["lng"])

                    # نتجاهل: jitter وهو واقف (<5م) + قفزات GPS الوهمية (>300م لحظيًا)
                    # + أي نقلة سرعتها المضمنة أعلى من 120 كم/س (يعني الموبايل قفل شوية وفتح في مكان تاني بره النطاق ده)
                    # + أي فجوة زمنية كبيرة (>30 ثانية) بين آخر نقطة والنقطة دي — دي علامة على reconnect/انقطاع نت
                    #   مش سير فعلي، فلو حسبناها هتضيف كيلومترات وهمية بسبب GPS drift وقت الانقطاع
                    if implied_kmh is not None and 5 <= step_m <= 300 and dt <= 30 and implied_kmh <= 120:
                        shift_km = round(shift_km + step_m / 1000, 3)

                    # نقطة outlier = سرعة ضمنية شاذة من آخر نقطة خام، أو بعيدة جدًا عن آخر موقع
                    # معروض بسرعة ضمنية شاذة برضه (يمسك القفزة المزدوجة اللي بتفلت من الفحص التاني)
                    point_is_outlier = (implied_kmh is not None and implied_kmh > 120)
                    if not point_is_outlier and step_from_display_m is not None and dt > 0:
                        implied_kmh_from_display = (step_from_display_m / dt) * 3.6
                        point_is_outlier = step_from_display_m > 300 and implied_kmh_from_display > 120

                    # المسافة من الفرع (auto-out/auto-return) بتتحسب من النقطة المعروضة/المفلترة
                    # مش من data الخام مباشرة — عشان قفزة GPS وهمية ما تأثرش على قرارات تشغيلية
                    if point_is_outlier and prev_display_lat is not None:
                        display_lat, display_lng = prev_display_lat, prev_display_lng
                    else:
                        display_lat, display_lng = data["lat"], data["lng"]
                    dist = haversine(display_lat, display_lng, BRANCH_LAT, BRANCH_LNG)

                    # تنعيم (EMA) للمسافة المعروضة بس — عشان الرقم اللي بيشوفه الأدمن مايرقصش
                    # كل تحديث بسبب jitter الـ GPS الطبيعي وهو واقف مكانه ثابت.
                    prev_display_dist = driver.get("distance")

                    if point_is_outlier and prev_display_dist is not None:
                        # نتجاهل القفزة الشاذة بالكامل: نفضل على آخر رقم معروض صحيح
                        display_dist = prev_display_dist
                    elif prev_display_dist is not None:
                        # لو الفرق كبير (>15م) نعتبرها حركة فعلية ونتبعها بسرعة أكبر (alpha أعلى)
                        # لو الفرق صغير (jitter) نمهّد أكتر (alpha أقل) عشان الرقم يفضل مستقر
                        jump = abs(dist - prev_display_dist)
                        alpha = 0.6 if jump > 15 else 0.25
                        display_dist = round(prev_display_dist + alpha * (dist - prev_display_dist))
                    else:
                        display_dist = dist

                    driver.update({
                        "distance": display_dist,
                        "raw_distance": dist,
                        # الماركر المعروض للأدمن — بيفضل على آخر نقطة صحيحة لو دي outlier،
                        # عشان الماركر ميقفزش لحظيًا لمكان غلط ثم يرجع
                        "lat": display_lat,
                        "lng": display_lng,
                        "display_lat": display_lat,
                        "display_lng": display_lng,
                        "speed": data.get("speed", 0),
                        "heading": data.get("heading", 0),
                        "last_seen": now_ts,
                        "shift_km": shift_km,
                        # _last_gps بتسجل النقطة الخام دايمًا (مش المفلترة) — لازم نفضل نقيس من
                        # الإحداثية الحقيقية اللي جاية من الموبايل عشان implied_kmh يفضل دقيق
                        # للمقارنة القادمة، حتى لو النقطة دي اتصنّفت outlier ومتتجاهلش في العرض
                        "_last_gps": {"lat": data["lat"], "lng": data["lng"], "ts": now_ts}
                    })
                    await save_driver_to_redis(driver_name, driver)
                    
                    # إرسال التحديث الصغير للآدمنز فوراً بدون تحميل كامل الداتا
                    await broadcast_location_update(driver)
                    
                    # تحديث المندوب بحالته + الكيلومترات لايف + هل هو داخل نطاق الفرع وعداد الرجوع التلقائي شغال
                    out_since_val = driver.get("out_since", 0)
                    auto_return_secs_left = None
                    if driver["state"] == "Out" and dist <= 100 and out_since_val:
                        auto_return_secs_left = max(0, 480 - (now_ts - out_since_val))
                    await ws.send_text(json.dumps({
                        "type": "distance", "meters": dist, "shift_km": shift_km,
                        "auto_return_secs_left": auto_return_secs_left
                    }))

                    # AUTO-RETURN: لو Out فأكتر من 8 دقايق وراجع للفرع (≤50m) → Waiting تلقائي
                    out_since = driver.get("out_since", 0)
                    two_mins_passed = (now_ts - out_since) >= 480
                    if driver["state"] == "Out" and dist <= 100 and two_mins_passed:
                        await change_driver_state(driver_name, "Waiting")
                        # بلّغ السواق إنه رجع في الطابور
                        try:
                            await ws.send_text(json.dumps({
                                "type": "auto_returned",
                                "msg": "🏠 رجعت للفرع — اتضفت في الطابور تلقائياً"
                            }))
                        except: pass
                        # FCM notification لو الـ app في الخلفية
                        try:
                            fcm_token = await redis_client.hget("fleet:fcm_tokens", driver_name)
                            if fcm_token and _FCM_SA_JSON:
                                access_token = _get_fcm_access_token()
                                fcm_payload = {
                                    "message": {
                                        "token": fcm_token,
                                        "data": {
                                            "type": "auto_returned",
                                            "title": "🏠 رجعت للفرع",
                                            "body": "اتضفت في الطابور تلقائياً"
                                        },
                                        "android": {"priority": "high"}
                                    }
                                }
                                async with httpx.AsyncClient() as client:
                                    await client.post(
                                        f"https://fcm.googleapis.com/v1/projects/{FIREBASE_PROJECT_ID}/messages:send",
                                        json=fcm_payload,
                                        headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
                                        timeout=10
                                    )
                        except: pass

                    # AUTO-OUT: لو وضع Auto مفعّل من الأدمن، أي مندوب Waiting وبعيد عن الفرع
                    # أكتر من AUTO_OUT_DISTANCE_M بيتحول Out تلقائي (مفيد لو المندوب طلع من غير ما يدوس "Send")
                    #
                    # ملاحظة مهمة: بنستخدم dist الخام (مش المنعّم) عشان القرار يبقى سريع لو المندوب
                    # فعلاً خرج، لكن عشان نتجنب إن نقطة GPS واحدة كاذبة/قفزة وهمية (وهو أصلاً واقف
                    # ثابت جوه الفرع) تحوله Out غلط، بنتجاهل النقطة لو طلعت outlier (implied_kmh > 120،
                    # نفس الفحص المستخدم فوق للمسافة المعروضة)، وكمان بنستنى إنه يفضل بره النطاق
                    # لمدة AUTO_OUT_CONFIRM_SECS متواصلة قبل ما نأكد التحويل — مش أول نقطة بعيدة بنشوفها.
                    if point_is_outlier:
                        # نقطة GPS مشكوك فيها — منستخدمهاش نهائي في قرار auto-out، ونصفّر عداد التأكيد
                        # عشان لو كانت بداية سلسلة حقيقية هنعيد التأكد من الصفر بنقط سليمة
                        driver["_out_of_range_since"] = None
                        await save_driver_to_redis(driver_name, driver)
                    elif driver["state"] == "Waiting" and dist > AUTO_OUT_DISTANCE_M:
                        out_of_range_since = driver.get("_out_of_range_since")
                        if not out_of_range_since:
                            out_of_range_since = now_ts
                            driver["_out_of_range_since"] = out_of_range_since
                            await save_driver_to_redis(driver_name, driver)

                        if (now_ts - out_of_range_since) >= AUTO_OUT_CONFIRM_SECS:
                            if await get_auto_out_enabled():
                                await change_driver_state(driver_name, "Out")
                                try:
                                    await ws.send_text(json.dumps({
                                        "type": "auto_out",
                                        "msg": "🚀 خرجت بره نطاق الفرع — اتسجلت Out تلقائياً"
                                    }))
                                except: pass
                    elif driver["state"] == "Waiting" and driver.get("_out_of_range_since"):
                        # رجع جوه النطاق قبل ما ياخد تأكيد — نصفّر العداد
                        driver["_out_of_range_since"] = None
                        await save_driver_to_redis(driver_name, driver)

            elif data["type"] == "ping":
                await ws.send_text(json.dumps({"type": "pong"}))

            elif data["type"] == "driver_message" and driver_name:
                # المندوب بعت رسالة للإدارة — نسجلها في التاريخ ونبلّغ كل الأدمنز فورًا
                msg_text = (data.get("text") or "").strip()
                if msg_text:
                    await append_chat_history(driver_name, "driver", msg_text)
                    await broadcast_driver_message_to_admins(driver_name, msg_text)

            elif data["type"] == "resync" and driver_name:
                # المندوب طلب مزامنة فورية (مثلاً بعد ما التطبيق رجع من الخلفية)
                drivers = await get_drivers_from_redis()
                if driver_name in drivers:
                    queue = await get_queue_from_redis()
                    avatar = await get_driver_avatar(driver_name)
                    me = dict(drivers[driver_name])
                    me["avatar"] = avatar
                    await ws.send_text(json.dumps({"type": "sync", "me": me, "queue": queue}))

            elif data["type"] == "change_state" and driver_name:
                if data["state"] == "Waiting":
                    drivers = await get_drivers_from_redis()
                    current_state = drivers.get(driver_name, {}).get("state")
                    # المندوب مسموح له يرجّع نفسه بس من Break — لو في Out لازم الأدمن يقفل الأوردر
                    if current_state == "Break":
                        await change_driver_state(driver_name, "Waiting")

            elif data["type"] == "end_shift" and driver_name:
                # المندوب دوس "إنهاء الشيفت" — لازم نمسحه فعليًا من Redis والطابور
                # مش بس نقفل الـ WebSocket، عشان الأدمن يشوفه اتشال فورًا
                ended_name = driver_name
                drivers = await get_drivers_from_redis()
                if driver_name in drivers:
                    await delete_driver_from_redis(driver_name)
                async with queue_lock:
                    queue = await get_queue_from_redis()
                    if driver_name in queue:
                        queue.remove(driver_name)
                        await save_queue_to_redis(queue)
                    await delete_fcm_token(driver_name)
                    if driver_connections.get(driver_name) is ws:
                        del driver_connections[driver_name]
                    driver_last_activity.pop(driver_name, None)
                    driver_disconnected_since.pop(driver_name, None)
                    driver_last_disconnect_alert.pop(driver_name, None)
                    driver_last_location_msg.pop(driver_name, None)
                    driver_last_stale_gps_alert.pop(driver_name, None)
                    driver_rejoin_blocked_until[driver_name.lower()] = time.time() + REJOIN_BLOCK_SECS
                    await broadcast_state("update")
                await broadcast_admin_event("ended_shift", ended_name, f"⚪ {ended_name} أنهى الشيفت")
                driver_name = None  # عشان الـ WebSocketDisconnect بعد كده متعملش حاجة تاني عليه

            elif data["type"] == "battery" and driver_name:
                driver = await get_one_driver_from_redis(driver_name)
                if driver is not None:
                    driver["battery"] = f"{int(data['level'] * 100)}%"
                    driver["last_seen"] = int(time.time())  # نحدث last_seen مع كل بطارية
                    await save_driver_to_redis(driver_name, driver)
                    # delta update للـ admins فقط بدل full board re-render
                    battery_msg = json.dumps({
                        "type": "battery_update",
                        "driver": driver_name,
                        "battery": driver["battery"]
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
        if driver_name:
            driver_last_activity.pop(driver_name, None)
            # نعلّم وقت الانقطاع (لو معلمناهوش قبل كده) — الـ watchdog هو اللي هيقرر لاحقًا
            # لو المدة عدّت من غير reconnect يبعتله FCM. مش بنبعت الإشعار من هنا مباشرة
            # عشان ده بيحصل بشكل طبيعي جدًا (تطبيق راح خلفية، رجرشة نت لحظية) ومعظمه
            # بيتصلح لوحده خلال ثواني بـ reconnect.
            driver_disconnected_since.setdefault(driver_name, time.time())
