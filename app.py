import os, json, sqlite3, base64, csv, io, time, urllib.parse
from pathlib import Path
from flask import (
    Flask, request, jsonify, render_template_string,
    send_from_directory, redirect, url_for, Response
)
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Timezone formatting (GMT+2 with AM/PM) ..---
from datetime import datetime, timezone, timedelta
TZ_GMT2 = timezone(timedelta(hours=2))
def fmt_gmt2(iso_str: str) -> str:
    try:
        dt = datetime.fromisoformat(iso_str)  # stored as naive UTC ISO
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt2 = dt.astimezone(TZ_GMT2)
        return dt2.strftime("%Y-%m-%d %I:%M %p GMT+2")
    except Exception:
        return iso_str or ""

# -------- Config (env) --------
VERIFY_TOKEN = os.getenv("WHATSAPP_VERIFY_TOKEN", "dev-verify")
WA_PNID      = os.getenv("WA_PNID")
WA_TOKEN     = os.getenv("WA_TOKEN")
GRAPH_BASE   = "https://graph.facebook.com/v21.0"

# Allow sending template components (e.g., Flow buttons) when explicitly enabled.
ALLOW_TEMPLATE_COMPONENTS = os.getenv("WA_ALLOW_TEMPLATE_COMPONENTS", "0") == "1"
print("DEBUG: ALLOW_TEMPLATE_COMPONENTS =", ALLOW_TEMPLATE_COMPONENTS, flush=True)

INBOX_USER = os.getenv("INBOX_USER", "admin")
INBOX_PASS = os.getenv("INBOX_PASS")              # enable auth when set
PROTECT_MEDIA = os.getenv("PROTECT_MEDIA", "0") == "1"

# DB (Postgres on Render, SQLite locally)
DATABASE_URL = os.getenv("DATABASE_URL") or ""  # database url on render

# Bulk defaults
BULK_CONCURRENCY_DEFAULT = int(os.getenv("BULK_CONCURRENCY", "5"))
BULK_SLEEP_DEFAULT = float(os.getenv("BULK_PER_CALL_SLEEP", "0.1"))  # 0.1s
BULK_MAX_RETRIES = int(os.getenv("BULK_MAX_RETRIES", "2"))

APP_DIR = Path(__file__).resolve().parent
DATA_DIR = APP_DIR / "data"; DATA_DIR.mkdir(exist_ok=True)
DB_PATH  = DATA_DIR / "messages.db"
STATIC_DIR = APP_DIR / "static"; STATIC_DIR.mkdir(exist_ok=True)

# -------- App --------
app = Flask(__name__, static_folder=str(STATIC_DIR))
app.url_map.strict_slashes = False

# -------- DB (SQLite locally, Postgres when DATABASE_URL is set) --------

def get_conn():
    """
    If DATABASE_URL is set -> Postgres (Render).
    Otherwise -> local SQLite (messages.db).
    """
    if DATABASE_URL:
        import psycopg2
        try:
            conn = psycopg2.connect(DATABASE_URL, connect_timeout=5)
            conn.autocommit = True
            return conn
        except Exception as e:
            print("⚠️ Postgres connection failed:", e, flush=True)
            raise
    return sqlite3.connect(DB_PATH)


def init_db():
    """
    Create the messages table on first run.
    """
    with get_conn() as c:
        cur = c.cursor()
        if DATABASE_URL:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS messages (
              id SERIAL PRIMARY KEY,
              created_at TEXT,
              direction TEXT,
              wa_from TEXT, wa_to TEXT, wa_id TEXT,
              name TEXT, type TEXT, body TEXT,
              status TEXT,
              conversation_id TEXT, conversation_category TEXT
            )
            """)
        else:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS messages (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              created_at TEXT,
              direction TEXT,          -- 'in' | 'out' | 'status' | 'unknown'
              wa_from TEXT, wa_to TEXT, wa_id TEXT,
              name TEXT, type TEXT, body TEXT,
              status TEXT,
              conversation_id TEXT, conversation_category TEXT
            )
            """)

def store_message(**kw):
    fields = (
        "created_at","direction","wa_from","wa_to","wa_id","name",
        "type","body","status","conversation_id","conversation_category"
    )
    values = [kw.get("created_at") or datetime.utcnow().isoformat()] + [
        kw.get(f) for f in fields[1:]
    ]
    with get_conn() as c:
        cur = c.cursor()
        if DATABASE_URL:
            placeholders = ",".join(["%s"] * len(fields))
        else:
            placeholders = ",".join(["?"] * len(fields))
        cur.execute(
            f"INSERT INTO messages ({','.join(fields)}) VALUES ({placeholders})",
            values
        )

def fetch_messages(limit=200, direction=None, since_id=None):
    sql = """
      SELECT
        id, created_at, direction, wa_from, wa_to, wa_id,
        name, type, body, status, conversation_id, conversation_category
      FROM messages
    """
    params = []
    where = []
    if direction in {"in","out"}:
        where.append("direction = %s" if DATABASE_URL else "direction = ?")
        params.append(direction)
    if since_id:
        where.append("id > %s" if DATABASE_URL else "id > ?")
        params.append(int(since_id))
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY id DESC"
    sql += " LIMIT " + ("%s" if DATABASE_URL else "?")
    params.append(limit)

    with get_conn() as c:
        if DATABASE_URL:
            import psycopg2.extras
            cur = c.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(sql, params)
            rows = cur.fetchall()
            return [dict(r) for r in rows]
        else:
            c.row_factory = sqlite3.Row
            cur = c.execute(sql, tuple(params))
            return [dict(r) for r in cur.fetchall()]


# Initialize DB at import
try:
    init_db()
    print(f"DB backend: {'Postgres' if DATABASE_URL else 'SQLite'}", flush=True)
except Exception as e:
    print("❌ init_db failed:", e, flush=True)
    raise

# -------- Minimal HTTP Basic auth --------
def _unauth():
    return Response("Auth required", 401, {"WWW-Authenticate": 'Basic realm="Inbox"'})

def require_basic_auth(fn):
    if not INBOX_PASS:
        return fn
    def _wrap(*args, **kwargs):
        auth = request.headers.get("Authorization", "")
        if auth.startswith("Basic "):
            try:
                raw = base64.b64decode(auth.split(" ", 1)[1]).decode("utf-8")
                user, pw = raw.split(":", 1)
                if user == INBOX_USER and pw == INBOX_PASS:
                    return fn(*args, **kwargs)
            except Exception:
                pass
        return _unauth()
    _wrap.__name__ = fn.__name__
    return _wrap

# -------- Helpers --------
def graph_post(path, payload):
    """
    Final sending helper to WhatsApp Graph API.
    """
    if not WA_PNID or not WA_TOKEN:
        raise RuntimeError("WA_PNID/WA_TOKEN not configured.")

    # Strip components unless explicitly allowed
    if isinstance(payload, dict) and not ALLOW_TEMPLATE_COMPONENTS:
        try:
            if payload.get("type") == "template":
                tpl = payload.get("template")
                if isinstance(tpl, dict):
                    tpl = dict(tpl)
                    tpl.pop("components", None)
                    payload = dict(payload)
                    payload["template"] = tpl
        except Exception:
            pass

    url = f"{GRAPH_BASE}/{path.lstrip('/')}"
    print("WA OUT:", json.dumps(payload, ensure_ascii=False), flush=True)

    r = requests.post(
        url,
        headers={"Authorization": f"Bearer {WA_TOKEN}", "Content-Type":"application/json"},
        data=json.dumps(payload),
        timeout=30
    )
    if not r.ok:
        raise RuntimeError(f"POST {url} -> {r.status_code} {r.text}")
    return r.json()

def graph_get(path):
    if not WA_TOKEN:
        raise RuntimeError("WA_TOKEN not configured.")
    url = f"{GRAPH_BASE}/{path.lstrip('/')}"
    r = requests.get(url, headers={"Authorization": f"Bearer {WA_TOKEN}"}, timeout=30)
    if not r.ok:
        raise RuntimeError(f"GET {url} -> {r.status_code} {r.text}")
    return r.json()

def badge_class(status_text: str) -> str:
    s = (status_text or "").lower()
    if s == "read":
        return "badge green"
    if s == "delivered":
        return "badge teal"
    if s == "sent":
        return "badge blue"
    if s in {"failed", "error"}:
        return "badge red"
    return "badge gray"

# ---- Dynamic ACK builders (with optional profile_name) ----
def build_ack_message(profile_name=None):
    name_part = f" {profile_name}" if profile_name else ""
    return (
        f"Thank you{name_part} for contacting Al-Khawarizmi Group, your request is being processed "
        f"and we will contact you shortly after.\n\n"
        f"{name_part} شكراً\n"
        f"لتواصلكم مع مجموعة الخوارزمي، جارٍ معالجة طلبكم وسنتواصل معكم قريباً\n"
    )

def build_ack_message_encoded(profile_name=None):
    return urllib.parse.quote(build_ack_message(profile_name))


# ---------- FLOW PREVIEW HELPER (FULL submission, multi-line) ----------
def build_flow_preview(body):
    """
    Build a FULL human-readable preview for any Flow (nfm_reply) payload.

    - Decodes Arabic properly.
    - Shows each key/value on its own line.
    - Does NOT truncate (except a very high safety cap).
    """
    if body is None:
        return None

    # Normalize to dict
    if isinstance(body, str):
        try:
            obj = json.loads(body)
        except Exception:
            # fallback: just show raw string (capped)
            return body[:4000]
    elif isinstance(body, dict):
        obj = body
    else:
        return str(body)[:4000]

    nfm = obj.get("nfm_reply") or obj
    parsed = obj.get("parsed_response") or {}

    # If parsed_response missing, decode response_json directly
    if not parsed:
        rj = nfm.get("response_json")
        if isinstance(rj, str):
            try:
                parsed = json.loads(rj) if rj.strip() else {}
            except Exception:
                parsed = {"raw_response_json": rj}
        elif isinstance(rj, dict):
            parsed = rj

    status = (nfm.get("body") or nfm.get("name") or "Flow reply").strip()

    # Build multi-line text
    if isinstance(parsed, dict) and parsed:
        lines = []
        for k, v in parsed.items():
            if k == "flow_token":
                continue
            lines.append(f"{k}: {v}")
        full_text = "\n".join(lines)
        return f"FLOW ({status}):\n{full_text}"[:4000]
    else:
        return f"FLOW ({status}): {json.dumps(parsed, ensure_ascii=False)}"[:4000]


def _massage_messages(rows, base_url):
    out = []
    base = (base_url or "").rstrip("/")
    for m in rows:
        m = dict(m)
        m["created_fmt"] = fmt_gmt2(m.get("created_at", ""))
        m["preview"] = m.get("body") or ""
        m["media_link"] = None
        m["status_class"] = badge_class(m.get("status"))

        profile_name = m.get("name")
        ack_msg = build_ack_message(profile_name)
        m["ack_msg"] = ack_msg
        m["ack_msg_enc"] = build_ack_message_encoded(profile_name)

        raw_body = m.get("body")
        t = (m.get("type") or "").lower()

        # FLOW: build full multi-line preview
        flow_preview = None
        try:
            if t == "nfm_reply" or (isinstance(raw_body, str) and '"nfm_reply"' in raw_body):
                flow_preview = build_flow_preview(raw_body)
        except Exception:
            flow_preview = None

        if flow_preview:
            m["preview"] = flow_preview

        # Media messages
        elif t in {"image","audio","video","document","sticker"} and raw_body:
            try:
                obj = json.loads(raw_body) if isinstance(raw_body, str) else (raw_body or {})
                payload = obj.get(t) if isinstance(obj, dict) and isinstance(obj.get(t), dict) else obj
                mid = (payload or {}).get("id")
                caption = (payload or {}).get("caption") if t == "image" else None
                if mid and base:
                    m["media_link"] = f"{base}/wa/media/{mid}"
                m["preview"] = (caption or f"{t.title()} • media_id={mid or 'n/a'}")
            except Exception:
                if isinstance(m["preview"], str) and len(m["preview"]) > 1500:
                    m["preview"] = m["preview"][:1500] + "…"
        else:
            # Plain text / template body JSON etc.
            if isinstance(m["preview"], str) and len(m["preview"]) > 1500:
                m["preview"] = m["preview"][:1500] + "…"

        out.append(m)
    return out

# Core send logic
def _lang_code_from(value, default="en"):
    if isinstance(value, dict):
        value = value.get("code") or value.get("language")
    if not value:
        return default
    return str(value).strip().lower() or default

def do_send(to, kind="text", text="", template=None):
    if not to:
        raise RuntimeError("missing 'to'")

    if kind == "text":
        out = {
            "messaging_product": "whatsapp",
            "to": to,
            "type": "text",
            "text": {"body": text or ""}
        }
        resp = graph_post(f"{WA_PNID}/messages", out)

    elif kind == "template":
        tpl = template or {}
        name = tpl.get("name")
        if not name:
            raise RuntimeError("template.name required")

        lang_code = _lang_code_from(tpl.get("language"))
        lang_value = tpl.get("language")

        if isinstance(lang_value, str):
            lang_value = {"code": _lang_code_from(lang_value, default=lang_code)}
        elif isinstance(lang_value, dict):
            c = _lang_code_from(
                lang_value.get("code") or lang_value.get("language") or lang_code,
                default=lang_code
            )
            lang_value = {"code": c}
        else:
            lang_value = {"code": lang_code}

        if ALLOW_TEMPLATE_COMPONENTS:
            t = dict(tpl)
            t["language"] = lang_value
        else:
            if tpl.get("components"):
                raise RuntimeError(
                    "Template components blocked: set WA_ALLOW_TEMPLATE_COMPONENTS=1 to send interactive/Flow templates."
                )
            t = {
                "name": name,
                "language": lang_value,
            }

        out = {
            "messaging_product": "whatsapp",
            "to": to,
            "type": "template",
            "template": t
        }
        resp = graph_post(f"{WA_PNID}/messages", out)

    else:
        raise RuntimeError("unsupported kind")

    wa_id = (resp.get("messages") or [{}])[0].get("id")
    conv = resp.get("conversation") or {}
    store_message(
        direction="out",
        wa_from=None, wa_to=to, wa_id=wa_id, name=None,
        type=out["type"], body=json.dumps(out, ensure_ascii=False), status="sent",
        conversation_id=conv.get("id"), conversation_category=conv.get("category"),
    )
    return resp

# ---------- BULK SEND ----------
def do_send_safe(number, kind="template", text="", template=None):
    try:
        return {"to": number, "ok": True, "resp": do_send(number, kind=kind, text=text, template=template)}
    except Exception as e:
        return {"to": number, "ok": False, "error": str(e)}

def bulk_send(numbers, kind="template", text="", template=None, concurrency=None, per_call_sleep=None, max_retries=BULK_MAX_RETRIES):
    numbers = [n.strip() for n in numbers if n and n.strip()]
    if not numbers:
        return []
    if concurrency is None:
        concurrency = BULK_CONCURRENCY_DEFAULT
    if per_call_sleep is None:
        per_call_sleep = BULK_SLEEP_DEFAULT

    results = []
    def worker(n):
        attempt = 0
        while True:
            r = do_send_safe(n, kind=kind, text=text, template=template)
            if r["ok"]:
                return r
            transient = any(tok in (r.get("error","")) for tok in [" 429 ", "Rate", "rate", "temporar", " 5"])
            if not transient or attempt >= max_retries:
                return r
            time.sleep(1.5 * (attempt + 1))
            attempt += 1

    with ThreadPoolExecutor(max_workers=max(1, int(concurrency))) as ex:
        futs = []
        for n in numbers:
            futs.append(ex.submit(worker, n))
            if per_call_sleep:
                time.sleep(per_call_sleep)
        for f in as_completed(futs):
            results.append(f.result())
    return results

# -------- Webhook --------
@app.get("/webhook")
def webhook_verify():
    if (request.args.get("hub.mode") == "subscribe"
        and request.args.get("hub.verify_token") == VERIFY_TOKEN):
        return request.args.get("hub.challenge", ""), 200
    return "forbidden", 403

# ------- CHATBOT IMPLEMENTATION --------------

AUTO_REPLY_ENABLED = os.getenv("AUTO_REPLY_ENABLED", "0") == "1"

def auto_reply_for_text(text, profile_name=None):
    if not AUTO_REPLY_ENABLED:
        return None
    if not text:
        return None

    t = text.strip().lower()

    if any(g in t for g in ["hi", "hello", "helo", "مرحبا", "salam", "سلامات", "سلام" ]):
        name_part = f" {profile_name}" if profile_name else ""
        return (
            f"Hello{name_part} 👋\n"
            "Welcome to Al-Khawarizmi Group.\n\n"
            "Type:\n"
            "1️ for Construction & Contracting\n"
            "2️ for Solar / PV Systems\n"
            "3️ to talk to a representative."
        )

    if t in ["1", "construction", "contracting", "انشاءات", "مقاولات"]:
        return (
            "*Construction & Contracting*\n\n"
            "We handle:\n"
            "• Concrete RC & formwork\n"
            "• Villas & buildings\n"
            "• Renovation & finishing\n\n"
            "Send your project location + any drawings (if available) and we’ll follow up."
        )

    if t in ["2", "solar", "pv", "طاقة", "طاقة شمسية"]:
        return (
            "*Solar / PV Systems*\n\n"
            "Please share:\n"
            "• Your area / village\n"
            "• Average monthly bill\n"
            "• Number of AC / main loads\n\n"
            "Our team will estimate a suitable system and contact you."
        )

    if t in ["3", "agent", "support", "موظف", "انسان"]:
        return (
            "A representative will take over this conversation shortly.\n"
            "Thank you for your patience."
        )

    return build_ack_message(profile_name)


@app.post("/webhook")
def webhook_inbound():
    data = request.get_json(force=True, silent=True) or {}
    print("WEBHOOK:", json.dumps(data, ensure_ascii=False), flush=True)

    if data.get("object") != "whatsapp_business_account":
        return jsonify(status="ignored"), 200

    try:
        for entry in (data.get("entry") or []):
            for ch in (entry.get("changes") or []):
                value = ch.get("value") or {}

                # -------- Status updates --------
                for st in (value.get("statuses") or []):
                    conv = st.get("conversation") or {}
                    store_message(
                        direction="status",
                        wa_from=None,
                        wa_to=st.get("recipient_id"),
                        wa_id=st.get("id"),
                        name=None,
                        type="status",
                        body=json.dumps(st, ensure_ascii=False),
                        status=st.get("status"),
                        conversation_id=conv.get("id"),
                        conversation_category=conv.get("category"),
                    )

                # -------- Inbound messages --------
                contacts = value.get("contacts") or [{}]
                profile_name = (contacts[0].get("profile") or {}).get("name") if contacts else None
                meta = value.get("metadata") or {}

                for msg in (value.get("messages") or []):
                    mtype = msg.get("type")

                    inbound_text = None
                    body = ""

                    if mtype == "text":
                        inbound_text = (msg.get("text") or {}).get("body", "")
                        body = inbound_text or ""

                    elif mtype == "nfm_reply":
                        nfm = msg.get("nfm_reply") or {}
                        rj = nfm.get("response_json")

                        parsed_response = {}
                        if isinstance(rj, str):
                            try:
                                parsed_response = json.loads(rj) if rj.strip() else {}
                            except Exception:
                                parsed_response = {"raw_response_json": rj}
                        elif isinstance(rj, dict):
                            parsed_response = rj

                        flow_data = {
                            "nfm_reply": nfm,
                            "parsed_response": parsed_response,
                        }

                        print("FLOW SUBMISSION:", json.dumps(flow_data, ensure_ascii=False), flush=True)
                        body = json.dumps(flow_data, ensure_ascii=False)

                    else:
                        body = json.dumps(msg.get(mtype, {}) or {}, ensure_ascii=False)

                    store_message(
                        direction="in",
                        wa_from=msg.get("from"),
                        wa_to=meta.get("display_phone_number"),
                        wa_id=msg.get("id"),
                        name=profile_name,
                        type=mtype,
                        body=body,
                        status="received",
                        conversation_id=(msg.get("context") or {}).get("id"),
                        conversation_category=None,
                    )

                    try:
                        if inbound_text:
                            reply_text = auto_reply_for_text(inbound_text, profile_name=profile_name)
                            if reply_text:
                                do_send(msg.get("from"), kind="text", text=reply_text)
                    except Exception as e:
                        print("Auto-reply failed:", e, flush=True)

    except Exception as e:
        print("Webhook parse error:", e, flush=True)

    return jsonify(status="ok"), 200

    
@app.get("/api/contacts")
@require_basic_auth
def api_contacts():
    base = request.url_root
    rows = _massage_messages(fetch_messages(500, direction=None), base)

    contacts_map = {}
    for m in rows:
        direction = m.get("direction")
        if direction not in ("in", "out"):
            continue
        phone = m.get("wa_from") if direction == "in" else m.get("wa_to")
        if not phone:
            continue

        existing = contacts_map.get(phone)
        if (not existing) or (m["id"] > existing["last_id"]):
            contacts_map[phone] = {
                "phone": phone,
                "name": m.get("name") or "",
                "last_preview": m.get("preview") or "",
                "last_time": m.get("created_fmt") or m.get("created_at") or "",
                "last_id": m["id"],
                "last_direction": direction,
            }

    contacts = list(contacts_map.values())
    contacts.sort(key=lambda c: c["last_id"], reverse=True)
    return jsonify(contacts)


@app.get("/api/chat")
@require_basic_auth
def api_chat():
    phone = request.args.get("phone")
    if not phone:
        return jsonify([])

    base = request.url_root
    rows = _massage_messages(fetch_messages(500, direction=None), base)

    msgs = []
    for m in rows:
        direction = m.get("direction")
        if direction not in ("in", "out"):
            continue
        peer = m.get("wa_from") if direction == "in" else m.get("wa_to")
        if peer == phone:
            msgs.append(m)

    msgs.sort(key=lambda x: x["id"])
    return jsonify(msgs)


# -------- UI (WhatsApp theme + tabs + scroll + badges + quick actions + BULK) --------
INBOX_HTML = """
<!doctype html>
<html lang="en" data-theme="dark">
<head>
<meta charset="utf-8">
<title>WhatsApp API Inbox - Al-Khawarizmi Group</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<link rel="icon" href="/favicon.ico">
<style>
 :root{
   --wa-green:#25D366;
   --wa-dark:#075E54;
   --wa-light:#DCF8C6;

   --page-bg:#0b141a;
   --app-bg:#111b21;
   --sidebar-bg:#111b21;
   --sidebar-header-bg:#202c33;
   --chat-bg:#0b141a;
   --chat-panel-bg:#202c33;
   --chat-messages-bg:#0a1014;
   --bulk-panel-bg:#111b21;
   --bulk-inner-bg:#111827;
   --input-bg:#111827;
   --border-strong:#202c33;
   --border-soft:#1f2937;
   --input-border:#374151;

   --wa-text:#e9edef;
   --wa-text-soft:#8696a0;
   --blue:#3b82f6;
   --red:#ef4444;
 }

 [data-theme="light"]{
   --wa-dark:#008069;
   --wa-light:#e7ffdb;

   --page-bg:#e5ddd5;
   --app-bg:#ffffff;
   --sidebar-bg:#ffffff;
   --sidebar-header-bg:#f0f2f5;
   --chat-bg:#e5ddd5;
   --chat-panel-bg:#f0f2f5;
   --chat-messages-bg:#efeae2;
   --bulk-panel-bg:#f0f2f5;
   --bulk-inner-bg:#ffffff;
   --input-bg:#ffffff;
   --border-strong:#d1d5db;
   --border-soft:#e5e7eb;
   --input-border:#d1d5db;

   --wa-text:#111827;
   --wa-text-soft:#6b7280;
 }

 *{box-sizing:border-box}
 body{
   margin:0;
   font-family:system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Arial,sans-serif;
   background:var(--page-bg);
   color:var(--wa-text);
 }
 .topbar{
   background:var(--wa-dark);
   color:#fff;
   padding:10px 16px;
   display:flex;
   align-items:center;
   justify-content:space-between;
   gap:10px;
 }
 .topbar-title{
   font-weight:600;
   font-size:14px;
   display:flex;
   align-items:center;
   gap:8px;
 }
 .topbar-title span.logo-dot{
   width:8px;height:8px;border-radius:999px;background:var(--wa-green);
 }
 .topbar-actions{
   display:flex;
   align-items:center;
   gap:8px;
 }
 .pill-btn{
   border-radius:999px;
   padding:6px 10px;
   border:none;
   cursor:pointer;
   font-size:12px;
   display:inline-flex;
   align-items:center;
   gap:6px;
 }
 .pill-toggle{
   background:#16a34a;
   color:#fff;
 }
 .pill-toggle.off{
   background:#6b7280;
 }
 .pill-export{
   background:#0d6efd;
   color:#fff;
   text-decoration:none;
 }
 .pill-theme{
   background:rgba(15,23,42,0.2);
   color:#e5e7eb;
   border:1px solid rgba(15,23,42,0.4);
 }
 .pill-theme:hover{
   filter:brightness(1.1);
 }

 .outer-wrap{
   height:calc(100vh - 46px);
   padding:12px;
   display:flex;
 }
 .app{
   width:100%;
   height:100%;
   background:var(--app-bg);
   border-radius:8px;
   overflow:hidden;
   display:flex;
   border:1px solid var(--border-soft);
 }

 .sidebar{
   width:32%;
   min-width:260px;
   background:var(--sidebar-bg);
   border-right:1px solid var(--border-strong);
   display:flex;
   flex-direction:column;
 }
 .sidebar-header{
   padding:8px;
   background:var(--sidebar-header-bg);
   display:flex;
   flex-direction:column;
   gap:6px;
 }
 .sidebar-header-title{
   font-size:13px;
   font-weight:600;
 }
 .sidebar-search{
   position:relative;
 }
 .sidebar-search input{
   width:100%;
   padding:6px 26px 6px 10px;
   border-radius:6px;
   border:none;
   outline:none;
   font-size:12px;
   background:var(--input-bg);
   color:var(--wa-text);
 }
 .sidebar-search input::placeholder{
   color:var(--wa-text-soft);
 }
 .sidebar-search span.icon{
   position:absolute;
   right:8px;
   top:50%;
   transform:translateY(-50%);
   font-size:12px;
   color:var(--wa-text-soft);
 }
 .contact-list{
   flex:1;
   overflow-y:auto;
   background:var(--sidebar-bg);
 }
 .contact{
   padding:8px 10px;
   display:flex;
   gap:10px;
   cursor:pointer;
   border-bottom:1px solid var(--border-strong);
 }
 .contact:hover{
   background:var(--sidebar-header-bg);
 }
 .contact.active{
   background:var(--sidebar-header-bg);
 }
 .contact-avatar{
   width:32px;height:32px;border-radius:50%;
   background:var(--sidebar-header-bg);
   display:flex;align-items:center;justify-content:center;
   font-size:14px;
   color:var(--wa-text-soft);
 }
 .contact-main{
   flex:1;
   min-width:0;
 }
 .contact-row1{
   display:flex;
   justify-content:space-between;
   gap:6px;
   font-size:12px;
 }
 .contact-name{
   font-weight:600;
   white-space:nowrap;
   overflow:hidden;
   text-overflow:ellipsis;
 }
 .contact-time{
   font-size:11px;
   color:var(--wa-text-soft);
 }
 .contact-row2{
   margin-top:2px;
   font-size:11px;
   color:var(--wa-text-soft);
   white-space:nowrap;overflow:hidden;text-overflow:ellipsis;
 }
 .chat{
   flex:1;
   display:flex;
   flex-direction:column;
   background:var(--chat-bg);
 }
 .chat-header{
   padding:10px 12px;
   background:var(--chat-panel-bg);
   border-bottom:1px solid var(--border-strong);
   display:flex;
   justify-content:space-between;
   align-items:center;
   font-size:13px;
 }
 .chat-header-main{
   display:flex;
   flex-direction:column;
 }
 .chat-title{
   font-weight:600;
 }
 .chat-subtitle{
   font-size:11px;
   color:var(--wa-text-soft);
 }
 .chat-empty{
   flex:1;
   display:flex;
   align-items:center;
   justify-content:center;
   font-size:13px;
   color:var(--wa-text-soft);
   text-align:center;
   padding:16px;
 }
 .chat-messages{
   flex:1;
   padding:12px;
   background:var(--chat-messages-bg);
   background-size:400px;
   overflow-y:auto;
   display:flex;
   flex-direction:column;
   gap:4px;
 }
 .msg-row{
   display:flex;
   width:100%;
   margin-bottom:2px;
 }
 .msg.in{
   margin-right:auto;
   background:var(--sidebar-header-bg);
   color:var(--wa-text);
 }
 .msg.out{
   margin-left:auto;
   background:var(--wa-light);
   color:#111827;
 }
 .msg{
   max-width:min(70%, 640px);
   width:fit-content;
   padding:6px 8px;
   border-radius:10px;
   font-size:13px;
   position:relative;
   white-space:pre-wrap;
   word-break:break-word;
   overflow-wrap:anywhere;
 }
 .msg-time{
   font-size:10px;
   opacity:.7;
   margin-top:2px;
   text-align:right;
 }
 .msg-media a{
   color:#0ea5e9;
   text-decoration:none;
   font-size:12px;
 }
 .msg-media a:hover{text-decoration:underline;}
 .chat-compose{
   padding:8px;
   background:var(--chat-panel-bg);
   border-top:1px solid var(--border-strong);
 }
 .chat-compose form{
   display:flex;
   flex-direction:column;
   gap:6px;
 }
 .compose-row{
   display:flex;
   gap:6px;
   flex-wrap:wrap;
 }
 .compose-row > *{
   font-size:12px;
 }
 .compose-row select,
 .compose-row input{
   padding:6px 8px;
   border-radius:6px;
   border:1px solid var(--input-border);
   outline:none;
   background:var(--input-bg);
   color:var(--wa-text);
 }
 .compose-row select:focus,
 .compose-row input:focus,
 .chat-textarea:focus{
   border-color:var(--wa-green);
 }
 .chat-textarea{
   width:100%;
   padding:6px 8px;
   border-radius:6px;
   border:1px solid var(--input-border);
   background:var(--input-bg);
   color:var(--wa-text);
   font-size:12px;
   min-height:60px;
   resize:vertical;
 }
 .send-btn{
   align-self:flex-end;
   background:var(--wa-green);
   border:none;
   color:#022c22;
   padding:6px 14px;
   border-radius:999px;
   font-weight:600;
   cursor:pointer;
   font-size:12px;
 }
 .send-btn:hover{
   filter:brightness(.95);
 }
 .small{font-size:11px;color:var(--wa-text-soft);}
 .bulk-panel{
   background:var(--bulk-panel-bg);
   border-top:1px solid var(--border-strong);
   padding:10px 12px;
   font-size:12px;
 }
 details.bulk{
   background:var(--bulk-inner-bg);
   border-radius:6px;
   padding:8px 10px;
   border:1px solid var(--border-soft);
 }
 details.bulk summary{
   list-style:none;
   cursor:pointer;
   display:flex;
   justify-content:space-between;
   align-items:center;
   font-size:12px;
 }
 details.bulk summary::-webkit-details-marker{display:none;}
 .bulk form{
   margin-top:8px;
   display:flex;
   flex-direction:column;
   gap:6px;
 }
 .bulk-row{
   display:flex;
   gap:8px;
   flex-wrap:wrap;
 }
 .bulk-row textarea{
   flex:1;
   min-width:220px;
   min-height:80px;
 }
 .bulk-row textarea,
 .bulk input,
 .bulk select,
 .bulk textarea{
   padding:6px 8px;
   border-radius:6px;
   border:1px solid var(--input-border);
   background:var(--input-bg);
   color:var(--wa-text);
   font-size:12px;
 }
 .bulk button{
   background:#22c55e;
   border:none;
   color:#022c22;
   padding:6px 10px;
   border-radius:999px;
   font-weight:600;
   cursor:pointer;
   font-size:12px;
   align-self:flex-start;
 }
 .bulk button:hover{
   filter:brightness(.96);
 }
 #toast{
   position:fixed;
   right:16px;
   bottom:16px;
   padding:8px 12px;
   border-radius:8px;
   font-size:12px;
   background:#16a34a;
   color:#fff;
   display:none;
   z-index:9999;
 }
 #toast.error{background:#dc2626;}
 @media(max-width:900px){
   .app{flex-direction:column;}
   .sidebar{width:100%;height:40%;border-right:none;border-bottom:1px solid var(--border-strong);}
   .chat{height:60%;}
 }
</style>
</head>
<body>

<div class="topbar">
  <div class="topbar-title">
    <span class="logo-dot"></span>
    <span>Al-Khawarizmi WhatsApp Inbox - Elite Dev. 2025</span>
  </div>
  <div class="topbar-actions">
    <form method="post" action="/toggle-autoreply?dir={{active_dir}}" style="margin:0">
      <input type="hidden" name="enabled" value="{{ '0' if auto_reply_enabled else '1' }}">
      <button type="submit"
              class="pill-btn pill-toggle {% if not auto_reply_enabled %}off{% endif %}">
        Auto-Reply: {{ 'ON' if auto_reply_enabled else 'OFF' }}
      </button>
    </form>
    <a href="/export.csv?dir=in" class="pill-btn pill-export" title="Export inbox as CSV">
      Export CSV
    </a>
    <button type="button" id="themeToggle" class="pill-btn pill-theme">
      🌙 Dark
    </button>
  </div>
</div>

<div class="outer-wrap">
  <div class="app">
    <!-- Sidebar / chat list -->
    <div class="sidebar">
      <div class="sidebar-header">
        <div class="sidebar-header-title">Chats</div>
        <div class="sidebar-search">
          <input id="searchInput" placeholder="Search name or number">
          <span class="icon">🔍</span>
        </div>
      </div>
      <div id="contactList" class="contact-list"></div>
    </div>

    <!-- Chat panel -->
    <div class="chat">
      <div class="chat-header">
        <div class="chat-header-main">
          <div id="chatTitle" class="chat-title">Select a chat</div>
          <div id="chatSubtitle" class="chat-subtitle">Incoming messages will appear here</div>
        </div>
      </div>

      <div id="chatEmpty" class="chat-empty">
        Select a contact from the left panel to start chatting.<br>
        You can still use Bulk Send at the bottom for campaigns.
      </div>
      <div id="chatMessages" class="chat-messages" style="display:none;"></div>

      <div class="chat-compose">
        <form id="chatSendForm">
          <input type="hidden" id="chatPhone">
          <div class="compose-row">
            <select id="chatKind">
              <option value="text">Text</option>
              <option value="template">Template</option>
            </select>
            <input id="tplName" placeholder="template name (if template)">
            <select id="tplLang" style="max-width:80px">
              <option value="en" selected>EN</option>
              <option value="ar">AR</option>
            </select>
            <select id="headerType">
              <option value="">Header: none</option>
              <option value="image">Header: image URL</option>
              <option value="video">Header: video URL</option>
            </select>
            <input id="headerUrl" placeholder="https:// header media URL" style="flex:1;min-width:120px">
            <input id="flowId" placeholder="Flow ID (optional)" style="max-width:180px">
            <label class="small" style="display:flex;align-items:center;gap:4px;">
              <input type="checkbox" id="flowEnable">
              Flow button
            </label>
          </div>
          <textarea id="chatText" class="chat-textarea"
            placeholder="Type a message (for text) OR JSON components for advanced templates."></textarea>
          <div class="small">
            For templates: you can paste components JSON, or just use Header / Flow controls above.
          </div>
          <button type="submit" class="send-btn">Send</button>
        </form>
      </div>

      <!-- Bulk panel -->
      <div class="bulk-panel">
        <details class="bulk">
          <summary>
            <span>Bulk Send (templates / text)</span>
            <span class="small">Click to expand</span>
          </summary>
          <form method="post" action="/inbox/bulk">
            <div class="bulk-row">
              <textarea name="numbers" placeholder="One number per line (e.g. +9617xxxxxx)" required></textarea>
              <div style="min-width:220px;display:flex;flex-direction:column;gap:4px;">
                <label class="small">Kind</label>
                <select name="kind">
                  <option value="template" selected>Template (recommended)</option>
                  <option value="text">Text (24h window)</option>
                </select>
                <label class="small">Template name</label>
                <input name="tpl_name" placeholder="hello_world1">
                <label class="small">Language</label>
                <select name="tpl_lang">
                  <option value="en" selected>EN</option>
                  <option value="ar">AR</option>
                </select>
                <label class="small">Header type</label>
                <select name="header_type">
                  <option value="">Header: none</option>
                  <option value="image">Header: image URL</option>
                  <option value="video">Header: video URL</option>
                </select>
                <label class="small">Header URL</label>
                <input name="header_url" placeholder="https://...">
                <label class="small">Flow ID (optional)</label>
                <input name="flow_id" placeholder="Flow ID">
                <label class="small">
                  <input type="checkbox" name="flow_enable" value="1"> Flow button
                </label>
                <label class="small">Concurrency</label>
                <input name="concurrency" value="5" type="number" min="1" max="20">
                <label class="small">Per-call sleep (sec)</label>
                <input name="sleep" value="0.1" type="number" step="0.01" min="0">
              </div>
            </div>
            <textarea name="payload"
              placeholder='For templates: optional JSON components body. For text: message body.'></textarea>
            <button type="submit">Send Bulk</button>
            <div class="small">Note: Marketing/out-of-session must use approved templates.</div>
          </form>
        </details>
      </div>
    </div>
  </div>
</div>

<div id="toast"></div>

<script>
(function(){
  let contacts = [];
  let activePhone = null;

  const contactListEl = document.getElementById('contactList');
  const searchInput = document.getElementById('searchInput');
  const chatTitleEl = document.getElementById('chatTitle');
  const chatSubtitleEl = document.getElementById('chatSubtitle');
  const chatEmptyEl = document.getElementById('chatEmpty');
  const chatMessagesEl = document.getElementById('chatMessages');

  const chatPhoneInput = document.getElementById('chatPhone');
  const chatKind = document.getElementById('chatKind');
  const tplName = document.getElementById('tplName');
  const tplLang = document.getElementById('tplLang');
  const headerType = document.getElementById('headerType');
  const headerUrl = document.getElementById('headerUrl');
  const flowIdInput = document.getElementById('flowId');
  const flowEnable = document.getElementById('flowEnable');
  const chatText = document.getElementById('chatText');
  const chatSendForm = document.getElementById('chatSendForm');

  const toast = document.getElementById('toast');
  function showToast(msg, isErr){
    toast.textContent = msg;
    toast.className = isErr ? 'error' : '';
    toast.style.display = 'block';
    setTimeout(()=>{ toast.style.display='none'; },1800);
  }

  async function fetchJSON(url){
    const r = await fetch(url);
    if(!r.ok) throw new Error('HTTP '+r.status);
    return await r.json();
  }

  function renderContacts(list){
    contactListEl.innerHTML = '';
    list.forEach(c=>{
      const div = document.createElement('div');
      div.className = 'contact';
      div.dataset.phone = c.phone;

      const avatar = document.createElement('div');
      avatar.className = 'contact-avatar';
      const initials = (c.name || c.phone || '?').toString().trim()[0] || '?';
      avatar.textContent = initials.toUpperCase();

      const main = document.createElement('div');
      main.className = 'contact-main';

      const row1 = document.createElement('div');
      row1.className = 'contact-row1';
      const name = document.createElement('div');
      name.className = 'contact-name';
      name.textContent = c.name || c.phone;
      const time = document.createElement('div');
      time.className = 'contact-time';
      time.textContent = c.last_time || '';
      row1.appendChild(name);
      row1.appendChild(time);

      const row2 = document.createElement('div');
      row2.className = 'contact-row2';
      row2.textContent = c.last_preview || '';

      main.appendChild(row1);
      main.appendChild(row2);

      div.appendChild(avatar);
      div.appendChild(main);

      div.addEventListener('click', ()=>{
        setActiveContact(c.phone, c.name, div);
      });

      contactListEl.appendChild(div);
    });
  }

  function setActiveContact(phone, name, element){
    activePhone = phone;
    chatPhoneInput.value = phone;

    Array.from(contactListEl.querySelectorAll('.contact')).forEach(el=>{
      el.classList.toggle('active', el === element);
    });

    chatTitleEl.textContent = name || phone;
    chatSubtitleEl.textContent = phone;
    loadChat(phone);
  }

  async function loadContacts(){
    try{
      const data = await fetchJSON('/api/contacts');
      contacts = data || [];
      applyFilter();
      if(contacts.length && !activePhone){
        const first = contacts[0];
        const firstEl = contactListEl.querySelector('.contact');
        if(first && firstEl){
          setActiveContact(first.phone, first.name, firstEl);
        }
      }
    }catch(e){
      console.error('contacts error', e);
    }
  }

  function applyFilter(){
    const q = (searchInput.value || '').toLowerCase();
    if(!q){
      renderContacts(contacts);
      return;
    }
    const filtered = contacts.filter(c=>{
      const txt = (c.name || '') + ' ' + (c.phone || '');
      return txt.toLowerCase().includes(q);
    });
    renderContacts(filtered);
  }

  searchInput.addEventListener('input', applyFilter);

  function hasArabic(text){
    return /[\\u0600-\\u06FF]/.test(text || '');
  }

  async function loadChat(phone){
    if(!phone) return;
    try{
      const msgs = await fetchJSON('/api/chat?phone=' + encodeURIComponent(phone));
      chatMessagesEl.innerHTML = '';
      if(!msgs.length){
        chatEmptyEl.style.display = 'flex';
        chatMessagesEl.style.display = 'none';
        return;
      }
      chatEmptyEl.style.display = 'none';
      chatMessagesEl.style.display = 'flex';

      msgs.forEach(m=>{
        const row = document.createElement('div');
        row.className = 'msg-row';

        const bubble = document.createElement('div');
        const dir = m.direction === 'out' ? 'out' : 'in';
        bubble.className = 'msg ' + dir;

        if(m.media_link){
          const mediaDiv = document.createElement('div');
          mediaDiv.className = 'msg-media';
          mediaDiv.innerHTML =
            '<a href="'+m.media_link+'" target="_blank" rel="noopener">Download ' +
            (m.type || 'media') + '</a>';
          if(m.preview){
            const caption = document.createElement('div');
            caption.textContent = m.preview;
            caption.style.marginTop = '2px';
            caption.setAttribute('dir', hasArabic(m.preview) ? 'rtl' : 'ltr');
            mediaDiv.appendChild(caption);
          }
          bubble.appendChild(mediaDiv);
        }else{
          const text = m.preview || '';
          bubble.textContent = text;
          bubble.setAttribute('dir', hasArabic(text) ? 'rtl' : 'ltr');
        }

        const time = document.createElement('div');
        time.className = 'msg-time';
        time.textContent = m.created_fmt || m.created_at || '';
        bubble.appendChild(time);

        row.appendChild(bubble);
        chatMessagesEl.appendChild(row);
      });

      chatMessagesEl.scrollTop = chatMessagesEl.scrollHeight;
    }catch(e){
      console.error('chat error', e);
    }
  }

  chatSendForm.addEventListener('submit', async (ev)=>{
    ev.preventDefault();
    if(!activePhone){
      showToast('Select a chat first', true);
      return;
    }
    const kind = chatKind.value || 'text';
    const text = (chatText.value || '').trim();
    const name = tplName.value.trim();
    const lang = tplLang.value || 'en';
    const hType = (headerType.value || '').trim().toLowerCase();
    const hUrl = (headerUrl.value || '').trim();
    const flowId = (flowIdInput.value || '').trim();
    const flowOn = flowEnable.checked;

    let payload = { to: activePhone, kind: kind };

    if(kind === 'text'){
      if(!text){
        showToast('Enter a message', true);
        return;
      }
      payload.text = text;
    }else{
      if(!name){
        showToast('Template name is required', true);
        return;
      }
      let template = { name: name, language: lang };
      let components = null;

      // If user pasted components JSON, keep it
      if(text){
        try{
          const parsed = JSON.parse(text);
          if(Array.isArray(parsed)){
            components = parsed;
          }else if(parsed && typeof parsed === 'object'){
            components = [parsed];
          }
        }catch(e){
        }
      }

      if(!components){
        components = [];
      }

      // Optional header component
      if(hType && hUrl){
        components.push({
          type:'header',
          parameters:[
            {
              type:hType,
              [hType]:{ link:hUrl }
            }
          ]
        });
      }

      // Optional Flow button component
      if(flowOn && flowId){
        components.push({
          type:'button',
          sub_type:'flow',
          index:'0',
          parameters:[
            {
              type:'flow',
              flow_id: flowId
            }
          ]
        });
      }

      if(components.length === 0){
        components = null;
      }

      if(components){
        template.components = components;
      }
      payload.template = template;
    }

    try{
      const res = await fetch('/send', {
        method:'POST',
        headers:{'Content-Type':'application/json'},
        body:JSON.stringify(payload)
      });
      const data = await res.json();
      if(!res.ok || data.error){
        throw new Error(data.error || ('HTTP '+res.status));
      }
      if(kind === 'text'){
        chatText.value = '';
      }
      showToast('Message sent ✓', false);
      setTimeout(()=>{ loadChat(activePhone); loadContacts(); },400);
    }catch(e){
      console.error('send error', e);
      showToast('Failed: ' + e.message, true);
    }
  });

  const themeToggleBtn = document.getElementById('themeToggle');
  function applyTheme(theme){
    document.documentElement.setAttribute('data-theme', theme);
    if(theme === 'light'){
      themeToggleBtn.textContent = ' Light';
    }else{
      themeToggleBtn.textContent = ' Dark';
    }
  }
  const savedTheme = localStorage.getItem('waTheme') || 'dark';
  applyTheme(savedTheme);

  themeToggleBtn.addEventListener('click', ()=>{
    const current = document.documentElement.getAttribute('data-theme') || 'dark';
    const next = current === 'dark' ? 'light' : 'dark';
    applyTheme(next);
    localStorage.setItem('waTheme', next);
  });

  loadContacts();
  setInterval(()=>{
    loadContacts();
    if (activePhone) {
      loadChat(activePhone);
    }
  }, 5000);
})();
</script>


</body>
</html>
"""

# ---- Routes for Inbox UI ----
@app.get("/inbox")
@require_basic_auth
def inbox():
    active_dir = request.args.get("dir") or "in"
    if active_dir not in {"in","out"}: active_dir = "in"
    base = request.url_root
    rows = _massage_messages(fetch_messages(200, direction=active_dir), base)
    return render_template_string(
        INBOX_HTML,
        messages=rows,
        active_dir=active_dir,
        auto_reply_enabled=AUTO_REPLY_ENABLED,
    )

@app.post("/inbox/send")
@require_basic_auth
def inbox_send():
    to = request.form.get("to")
    kind = request.form.get("kind", "text")
    raw_text = (request.form.get("text") or "").strip()
    tpl_name = request.form.get("tpl_name") or ""
    tpl_lang = request.form.get("tpl_lang") or "en"
    header_type = (request.form.get("header_type") or "").strip().lower()
    header_url = (request.form.get("header_url") or "").strip()
    flow_id = (request.form.get("flow_id") or "").strip()
    flow_enable = request.form.get("flow_enable") == "1"

    try:
        if kind == "text":
            do_send(to, kind="text", text=raw_text)
        else:
            components = None

            if raw_text:
                try:
                    loaded = json.loads(raw_text)
                    if isinstance(loaded, list):
                        components = loaded
                    elif isinstance(loaded, dict):
                        components = [loaded]
                except Exception:
                    components = None

            if components is None:
                components = []

            if header_type in ("image", "video") and header_url:
                components.append(
                    {
                        "type": "header",
                        "parameters": [
                            {
                                "type": header_type,
                                header_type: {
                                    "link": header_url
                                }
                            }
                        ]
                    }
                )

            if flow_enable and flow_id:
                components.append(
                    {
                        "type": "button",
                        "sub_type": "flow",
                        "index": "0",
                        "parameters": [
                            {"type": "flow", "flow_id": flow_id}
                        ]
                    }
                )

            if not components:
                components = None

            tpl = {"name": tpl_name, "language": tpl_lang}
            if components:
                tpl["components"] = components

            do_send(to, kind="template", template=tpl)

        return redirect(url_for('inbox', dir='out', sent='1'))
    except Exception as e:
        return redirect(url_for('inbox', dir='out', err=str(e)))

# ---- Bulk UI handler ----
@app.post("/inbox/bulk")
@require_basic_auth
def inbox_bulk():
    raw = (request.form.get("numbers") or "").strip()
    numbers = [n.strip() for n in raw.splitlines() if n.strip()]
    kind = request.form.get("kind","template")
    tpl_name = request.form.get("tpl_name") or ""
    tpl_lang = request.form.get("tpl_lang") or "en"
    sleep = request.form.get("sleep")
    conc = request.form.get("concurrency")
    header_type = (request.form.get("header_type") or "").strip().lower()
    header_url = (request.form.get("header_url") or "").strip()
    flow_id = (request.form.get("flow_id") or "").strip()
    flow_enable = request.form.get("flow_enable") == "1"

    try:
        if kind == "text":
            text = (request.form.get("payload") or "").strip()
            results = bulk_send(
                numbers,
                kind="text",
                text=text,
                concurrency=int(conc or BULK_CONCURRENCY_DEFAULT),
                per_call_sleep=float(sleep or BULK_SLEEP_DEFAULT)
            )
        else:
            payload_str = (request.form.get("payload") or "").strip()
            components = None

            if payload_str:
                try:
                    loaded = json.loads(payload_str)
                    if isinstance(loaded, list):
                        components = loaded
                    elif isinstance(loaded, dict):
                        components = [loaded]
                except Exception:
                    components = None

            if components is None:
                components = []

            if header_type in ("image", "video") and header_url:
                components.append(
                    {
                        "type": "header",
                        "parameters": [
                            {
                                "type": header_type,
                                header_type: {
                                    "link": header_url
                                }
                            }
                        ]
                    }
                )

            if flow_enable and flow_id:
                components.append(
                    {
                        "type": "button",
                        "sub_type": "flow",
                        "index": "0",
                        "parameters": [
                            {"type": "flow", "flow_id": flow_id}
                        ]
                    }
                )

            if not components:
                components = None

            tpl = {"name": tpl_name, "language": tpl_lang}
            if components:
                tpl["components"] = components

            results = bulk_send(
                numbers,
                kind="template",
                template=tpl,
                concurrency=int(conc or BULK_CONCURRENCY_DEFAULT),
                per_call_sleep=float(sleep or BULK_SLEEP_DEFAULT)
            )
        ok = sum(1 for r in results if r.get("ok"))
        failed = len(results) - ok
        return redirect(url_for('inbox', dir='out', sent='1' if ok else None, err=None if failed==0 else f"{failed} failed"))
    except Exception as e:
        return redirect(url_for('inbox', dir='out', err=str(e)))

# ---- Bulk API (JSON) ----
@app.post("/bulk")
@require_basic_auth
def bulk_api():
    """
    JSON:
    {
      "to": ["+9617xxxxxx", "+9613yyyyyy"],
      "kind": "template",
      "text": "Hi",                               # when kind=text
      "template": {"name":"hello_world1","language":"en","components":[...]},
      "concurrency": 5,
      "per_call_sleep": 0.1
    }
    """
    p = request.get_json(force=True, silent=False)
    numbers = p.get("to") or []
    if not isinstance(numbers, list) or not numbers:
        return jsonify(error="provide 'to' as a non-empty list"), 400

    kind = p.get("kind", "template")
    if kind not in ("text","template"):
        return jsonify(error="kind must be 'text' or 'template'"), 400

    if kind == "text":
        text = p.get("text","")
        results = bulk_send(
            numbers, kind="text", text=text,
            concurrency=int(p.get("concurrency", BULK_CONCURRENCY_DEFAULT)),
            per_call_sleep=float(p.get("per_call_sleep", BULK_SLEEP_DEFAULT))
        )
    else:
        tpl = p.get("template") or {}
        if not tpl.get("name"):
            return jsonify(error="template.name required for kind=template"), 400
        results = bulk_send(
            numbers, kind="template", template=tpl,
            concurrency=int(p.get("concurrency", BULK_CONCURRENCY_DEFAULT)),
            per_call_sleep=float(p.get("per_call_sleep", BULK_SLEEP_DEFAULT))
        )
    ok = sum(1 for r in results if r.get("ok"))
    failed = [r for r in results if not r.get("ok")]
    return jsonify(sent=ok, failed=len(failed), failures=failed), 200

# ---- Quick actions ----
@app.get("/quick")
@require_basic_auth
def quick():
    to = request.args.get("to")
    raw_msg = request.args.get("msg", "👍")
    msg = urllib.parse.unquote(raw_msg)
    redir = request.args.get("redir", "in")
    try:
        do_send(to, kind="text", text=msg)
        return redirect(url_for('inbox', dir=redir, sent='1'))
    except Exception as e:
        return redirect(url_for('inbox', dir=redir, err=str(e)))

# ---- Auto-reply toggle route ----
@app.post("/toggle-autoreply")
@require_basic_auth
def toggle_autoreply():
    global AUTO_REPLY_ENABLED
    enabled = request.form.get("enabled", "")
    AUTO_REPLY_ENABLED = (enabled == "1")
    redir_dir = request.args.get("dir") or "in"
    if redir_dir not in {"in","out"}:
        redir_dir = "in"
    return redirect(url_for("inbox", dir=redir_dir))

# ---- Favicon ----
@app.route("/favicon.ico")
def favicon():
    return send_from_directory(
        app.static_folder,
        "favicon.ico",
        mimetype="image/x-icon"
    )

# ---- Privacy ----
@app.get("/privacy")
def privacy():
    p = STATIC_DIR / "privacy.html"
    if p.exists(): return send_from_directory(app.static_folder, "privacy.html")
    return "No privacy.html uploaded", 200

# ---- Health ----
@app.get("/")
def health():
    return jsonify(ok=True, pnid=bool(WA_PNID))

# ---- Send API (JSON) ----
@app.post("/send")
@require_basic_auth
def send_api():
    """
    JSON body:
      { "to": "9617xxxxxx", "kind": "text", "text": "hi" }
      or
      { "to": "9617xxxxxx", "kind": "template", "template": { "name":"xyz", "language":"en", ... } }
    """
    p = request.get_json(force=True, silent=False)
    to = p.get("to")
    kind = p.get("kind","text")
    try:
        if kind == "text":
            resp = do_send(to, kind="text", text=p.get("text",""))
        else:
            resp = do_send(to, kind="template", template=p.get("template") or {})
        return jsonify(resp), 200
    except Exception as e:
        return jsonify(error=str(e)), 400

# ---- Media download ----
@app.get("/wa/media/<media_id>")
def wa_media(media_id):
    if PROTECT_MEDIA and INBOX_PASS:
        wrapped = require_basic_auth(lambda: Response("ok",200))()
        if isinstance(wrapped, Response) and wrapped.status_code == 401:
            return wrapped
    meta = graph_get(media_id)
    url = meta.get("url")
    if not url: return jsonify(error="no url for media"), 404
    r = requests.get(url, headers={"Authorization": f"Bearer {WA_TOKEN}"}, timeout=25)
    if not r.ok:
        return (r.text, r.status_code, {"Content-Type": "application/json"})
    ctype = r.headers.get("Content-Type", "application/octet-stream")
    return (r.content, 200, {"Content-Type": ctype})

# ---- CSV export ----
@app.get("/export.csv")
@require_basic_auth
def export_csv():
    direction = request.args.get("dir")
    if direction not in {"in", "out"}:
        direction = None

    rows = fetch_messages(2000, direction=direction)

    buf = io.StringIO()
    # UTF-8 BOM so Excel sees Arabic correctly
    buf.write("\ufeff")

    writer = csv.writer(buf)
    writer.writerow([
        "id",
        "created_at(GMT+2)",
        "direction",
        "wa_from",
        "wa_to",
        "wa_id",
        "name",
        "type",
        "status",
        "conversation_id",
        "conversation_category",
        "body",
    ])

    for m in rows:
        body = (m.get("body") or "").replace("\n", " ").replace("\r", " ")
        writer.writerow([
            m.get("id"),
            fmt_gmt2(m.get("created_at", "")),
            m.get("direction"),
            m.get("wa_from"),
            m.get("wa_to"),
            m.get("wa_id"),
            m.get("name"),
            m.get("type"),
            m.get("status"),
            m.get("conversation_id"),
            m.get("conversation_category"),
            body,
        ])

    data = buf.getvalue()
    resp = Response(
        data,
        mimetype="text/csv; charset=utf-8",
    )
    resp.headers["Content-Disposition"] = (
        f'attachment; filename="messages_{direction or "all"}.csv"'
    )
    return resp

# ---- Live feed API (polling) ----
@app.get("/api/messages")
@require_basic_auth
def api_messages():
    direction = request.args.get("dir")
    if direction not in {"in","out"}: direction = None
    since_id = request.args.get("since_id")
    base = request.url_root
    rows = _massage_messages(fetch_messages(200, direction=direction, since_id=since_id), base)
    return jsonify(rows)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8000")), debug=True)
