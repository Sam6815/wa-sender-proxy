import os, json, sqlite3, base64, csv, io, time, urllib.parse
from pathlib import Path
from flask import (
    Flask, request, jsonify, render_template_string,
    send_from_directory, redirect, url_for, Response
)
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Timezone formatting (GMT+2 with AM/PM) ---
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

INBOX_USER = os.getenv("INBOX_USER", "admin")
INBOX_PASS = os.getenv("INBOX_PASS")              # enable auth when set
PROTECT_MEDIA = os.getenv("PROTECT_MEDIA", "0") == "1"

# DB (Postgres on Render, SQLite locally)
DATABASE_URL = os.getenv("DATABASE_URL") or ""  # e.g. postgres://user:pass@host:5432/dbname

# Bulk defaults
BULK_CONCURRENCY_DEFAULT = int(os.getenv("BULK_CONCURRENCY", "5"))
BULK_SLEEP_DEFAULT = float(os.getenv("BULK_PER_CALL_SLEEP", "0.1"))  # 0.1s per your request
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

    We add connect_timeout to avoid hanging the deploy if Postgres is misconfigured.
    """
    if DATABASE_URL:
        import psycopg2
        try:
            # Use DSN directly so sslmode and other params in the URL are respected.
            conn = psycopg2.connect(DATABASE_URL, connect_timeout=5)
            conn.autocommit = True
            return conn
        except Exception as e:
            print("⚠️ Postgres connection failed:", e, flush=True)
            # Fail fast so Render shows a clear error instead of a generic timeout
            raise

    # Fallback: SQLite (used locally when DATABASE_URL is not set)
    return sqlite3.connect(DB_PATH)


def init_db():
    """
    Create the messages table on first run.
    Uses Postgres DDL when DATABASE_URL is present, otherwise SQLite.
    """
    with get_conn() as c:
        cur = c.cursor()
        if DATABASE_URL:
            # Postgres
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
            # SQLite
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
    # store as UTC naive ISO for consistency
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
    # If this prints in Render logs, fix DATABASE_URL or temporarily unset it.
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
    if not WA_PNID or not WA_TOKEN:
        raise RuntimeError("WA_PNID/WA_TOKEN not configured.")
    url = f"{GRAPH_BASE}/{path.lstrip('/')}"
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
    """
    Build the bilingual ACK message, optionally including the profile name.
    """
    name_part = f" {profile_name}" if profile_name else ""
    return (
        f"Thank you{name_part} for contacting Al-Khawarizmi Group, your request is being processed "
        f"and we will contact you shortly after. "
        f".شكراً{name_part} لتواصلكم مع مجموعة الخوارزمي، جارٍ معالجة طلبكم وسنتواصل معكم قريباً"
    )

def build_ack_message_encoded(profile_name=None):
    """
    URL-encoded version of the ACK (for quick-action links).
    """
    return urllib.parse.quote(build_ack_message(profile_name))

def _massage_messages(rows, base_url):
    out = []
    base = (base_url or "").rstrip("/")
    for m in rows:
        m = dict(m)
        m["created_fmt"] = fmt_gmt2(m.get("created_at", ""))
        m["preview"] = m.get("body") or ""
        m["media_link"] = None
        m["status_class"] = badge_class(m.get("status"))

        # Per-row ACK (personalized)
        profile_name = m.get("name")
        ack_msg = build_ack_message(profile_name)
        m["ack_msg"] = ack_msg
        m["ack_msg_enc"] = build_ack_message_encoded(profile_name)

        t = (m.get("type") or "").lower()
        if t in {"image","audio","video","document","sticker"} and m.get("body"):
            try:
                obj = json.loads(m["body"]) if isinstance(m["body"], str) else (m["body"] or {})
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
            if isinstance(m["preview"], str) and len(m["preview"]) > 1500:
                m["preview"] = m["preview"][:1500] + "…"
        out.append(m)
    return out

# Core send logic
def do_send(to, kind="text", text="", template=None):
    if not to:
        raise RuntimeError("missing 'to'")
    if kind == "text":
        out = {"messaging_product":"whatsapp","to":to,"type":"text","text":{"body": text or ""}}
    elif kind == "template":
        tpl = template or {}
        name = tpl.get("name"); lang = tpl.get("language") or "en"
        if not name: raise RuntimeError("template.name required")
        t = {"name": name, "language": {"code": lang}}
        if tpl.get("components"): t["components"] = tpl["components"]
        out = {"messaging_product":"whatsapp","to":to,"type":"template","template":t}
    else:
        raise RuntimeError("unsupported kind")
    resp = graph_post(f"{WA_PNID}/messages", out)
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

# ---- Auto-reply toggle (global in-memory flag) ----
AUTO_REPLY_ENABLED = os.getenv("AUTO_REPLY_ENABLED", "1") == "1"

def auto_reply_for_text(text, profile_name=None):
    """
    Given the inbound text, return a reply string or None to skip auto-reply.
    Very simple rule-based example.
    """
    # Respect global toggle
    if not AUTO_REPLY_ENABLED:
        return None

    if not text:
        return None

    t = text.strip().lower()

    # Greeting
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

    # Menu options
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

    # Fallback to standard AUTO-REPLY (personalized ACK)
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
                # Status updates
                for st in (value.get("statuses") or []):
                    conv = st.get("conversation") or {}
                    store_message(
                        direction="status", wa_from=None, wa_to=st.get("recipient_id"),
                        wa_id=st.get("id"), name=None, type="status",
                        body=json.dumps(st, ensure_ascii=False), status=st.get("status"),
                        conversation_id=conv.get("id"), conversation_category=conv.get("category"),
                    )
                # Inbound messages
                contacts = value.get("contacts") or [{}]
                profile_name = (contacts[0].get("profile") or {}).get("name") if contacts else None
                meta = value.get("metadata") or {}
                
                for msg in (value.get("messages") or []):
                    mtype = msg.get("type")

                    inbound_text = None
                    if mtype == "text":
                        inbound_text = (msg.get("text") or {}).get("body", "")
                        body = inbound_text
                    else:
                        body = json.dumps(msg.get(mtype, {}) or {}, ensure_ascii=False)

                    # Store inbound message
                    store_message(
                        direction="in", wa_from=msg.get("from"),
                        wa_to=meta.get("display_phone_number"), wa_id=msg.get("id"),
                        name=profile_name, type=mtype, body=body, status="received",
                        conversation_id=(msg.get("context") or {}).get("id"),
                        conversation_category=None,
                    )

                    # --- AUTO REPLY LOGIC ---
                    try:
                        # Only auto-reply to text messages for now
                        if inbound_text:
                            reply_text = auto_reply_for_text(inbound_text, profile_name=profile_name)
                            if reply_text:
                                # msg["from"] is the user's WhatsApp number
                                do_send(msg.get("from"), kind="text", text=reply_text)
                    except Exception as e:
                        print("Auto-reply failed:", e, flush=True)
    except Exception as e:
        print("Webhook parse error:", e, flush=True)
    return jsonify(status="ok"), 200

# -------- UI (WhatsApp theme + tabs + scroll + badges + quick actions + BULK) --------
INBOX_HTML = """
<!doctype html><html lang="en"><head><meta charset="utf-8">
<title>WhatsApp API Inbox - By Elite Dev.</title><meta name="viewport" content="width=device-width,initial-scale=1">
<link rel="icon" href="/favicon.ico">
<style>
 :root{ --wa-green:#25D366; --wa-dark:#075E54; --wa-light:#DCF8C6; --wa-bg:#f6f7f9; --text:#0f172a;
        --blue:#3b82f6; --teal:#14b8a6; --green:#22c55e; --red:#ef4444; --gray:#64748b; }
 body{font-family:system-ui,Segoe UI,Roboto,Arial,sans-serif;margin:0;background:var(--wa-bg);color:var(--text)}
 .wrap{max-width:1200px;margin:0 auto;padding:20px}
 .card{background:#fff;border-radius:12px;box-shadow:0 10px 24px rgba(0,0,0,.06);overflow:hidden;border:1px solid #eef2f7}
 .topbar{background:var(--wa-dark);color:#fff;padding:14px 18px;display:flex;align-items:center;gap:12px;justify-content:space-between}
 .pill{display:inline-flex;align-items:center;gap:8px;padding:8px 12px;border-radius:999px;background:#fff1;color:#fff;font-weight:600;letter-spacing:.2px}
 /* Optional: different color for the button pill */
 .pill-export {background: #0d6efd; color: #fff; text-decoration: none;}
 .pill-export:hover {opacity: 0.9;}
 .tabs{display:flex;gap:8px;padding:10px;background:#fff;border-bottom:1px solid #eef2f7}
 .tab{padding:8px 14px;border-radius:10px;border:1px solid #e5e7eb;color:#334155;text-decoration:none}
 .tab.active{background:var(--wa-green);color:#fff;border-color:var(--wa-green)}
 .tab:hover{border-color:#cbd5e1}
 .righttools a{margin-left:10px;color:#334155;text-decoration:none}
 .righttools a:hover{color:#000}
 .compose{padding:16px;display:flex;flex-direction:column;gap:10px;background:#fff}
 .row{display:flex;gap:8px;flex-wrap:wrap}
 input,select,textarea,button{padding:.6rem .7rem;font:inherit;border:1px solid #d1d5db;border-radius:10px;outline:none}
 input:focus,select:focus,textarea:focus{border-color:var(--wa-green);box-shadow:0 0 0 3px rgba(37,211,102,.18)}
 button{background:var(--wa-green);color:#fff;border:1px solid var(--wa-green);cursor:pointer}
 button:hover{filter:brightness(.95)}
 textarea{width:100%;height:80px}
 .scrollwrap{max-height:70vh;overflow:auto;background:#fff}
 table{border-collapse:collapse;width:100%}
 th,td{border-bottom:1px solid #eef2f7;padding:10px 8px;text-align:left;vertical-align:top}
 thead th{position:sticky;top:0;background:#fff}
 .badge{display:inline-block;padding:.12rem .5rem;border-radius:999px;border:1px solid #c9eec2;background:var(--wa-light)}
 .badge.blue{background:#eff6ff;border-color:#bfdbfe;color:#1d4ed8}
 .badge.teal{background:#f0fdfa;border-color:#99f6e4;color:#0f766e}
 .badge.green{background:#ecfdf5;border-color:#a7f3d0;color:#065f46}
 .badge.red{background:#fef2f2;border-color:#fecaca;color:#991b1b}
 .badge.gray{background:#f1f5f9;border-color:#cbd5e1;color:#334155}
 .mono{font-family:ui-monospace,Menlo,Consolas,monospace}
 .qa a{display:inline-block;margin-right:8px;padding:.25rem .55rem;border-radius:8px;border:1px solid #e2e8f0;text-decoration:none;color:#0f172a;font-size:.9rem}
 .qa a:hover{background:#f8fafc}
 #toast{position:fixed;right:16px;bottom:16px;z-index:9999;display:none;background:#16a34a;color:#fff;padding:.6rem .8rem;border-radius:10px;box-shadow:0 6px 18px rgba(0,0,0,.2)}
 #toast.error{background:#dc2626}

 /* Bulk panel */
 .bulk{padding:16px;border-top:1px dashed #e5e7eb;background:#fff}
 .bulk h3{margin:.2rem 0 10px 0;font-size:1rem;color:#0f172a}
 .bulk .row > *{flex:1 1 220px}
 .small{font-size:.85rem;color:#64748b}
</style></head>

<div class="topbar">
  <div class="pill">WhatsApp API Inbox - Al-Khawarizmi Group/developed by Elite Dev.</div>
  <div class="righttools">
    <form method="post" action="/toggle-autoreply?dir={{active_dir}}" style="display:inline">
      <input type="hidden" name="enabled" value="{{ '0' if auto_reply_enabled else '1' }}">
      <button type="submit" class="pill"
              style="border:none; cursor:pointer;
                     background: {{ '#16a34a' if auto_reply_enabled else '#6b7280' }};
                     color:#fff;">
        Auto-Reply: {{ 'ON' if auto_reply_enabled else 'OFF' }}
      </button>
    </form>
    <a href="/export.csv?dir={{active_dir}}" 
       class="pill pill-export" 
       title="Export CSV">
      Export CSV
    </a>
  </div>
</div>


<div class="wrap">
  <div class="card">
    <div class="tabs">
      <a class="tab {% if active_dir == 'in' %}active{% endif %}" href="/inbox?dir=in">Inbox</a>
      <a class="tab {% if active_dir == 'out' %}active{% endif %}" href="/inbox?dir=out">Sent</a>
    </div>

    <!-- Single send -->
    <form class="compose" method="post" action="/inbox/send">
      <div class="row" style="width:100%">
        <input name="to" placeholder="+9617xxxxxx" required style="min-width:220px">
        <select name="kind">
          <option value="text" selected>Text</option>
          <option value="template">Template</option>
        </select>
        <input name="tpl_name" placeholder="template name (if template)">
        <input name="tpl_lang" placeholder="en" value="en" style="width:72px">
      </div>
      <div class="row" style="width:100%">
        <select name="header_type">
          <option value="">Header: none</option>
          <option value="image">Header: image URL</option>
          <option value="video">Header: video URL</option>
        </select>
        <input name="header_url" placeholder="Header image/video URL (https://...)" style="flex:1">
        <button type="submit">Send</button>
      </div>
      <textarea name="text" placeholder="Text body (for text) OR JSON components/parameters (advanced templates)"></textarea>
      <div class="small">
        For templates: choose header type + paste URL above for media headers. The textarea is optional for body variables / advanced JSON.
      </div>
    </form>

    <!-- Bulk send -->
    <div class="bulk">
      <h3>Bulk Send</h3>
      <form method="post" action="/inbox/bulk">
        <div class="row" style="width:100%">
          <textarea name="numbers" placeholder="One number per line (e.g. +9617xxxxxx)" style="height:120px" required></textarea>
          <div style="min-width:280px">
            <label class="small">Kind</label>
            <select name="kind" style="width:100%">
              <option value="template" selected>Template (recommended)</option>
              <option value="text">Text (24h service window)</option>
            </select>
            <label class="small">Template name</label>
            <input name="tpl_name" placeholder="hello_world1">
            <label class="small">Language</label>
            <input name="tpl_lang" value="en">
            <label class="small">Header type</label>
            <select name="header_type" style="width:100%">
              <option value="">Header: none</option>
              <option value="image">Header: image URL</option>
              <option value="video">Header: video URL</option>
            </select>
            <label class="small">Header URL (image/video)</label>
            <input name="header_url" placeholder="https://...">
            <label class="small">Concurrency</label>
            <input name="concurrency" value="5" type="number" min="1" max="20">
            <label class="small">Per-call sleep (sec)</label>
            <input name="sleep" value="0.1" type="number" step="0.01" min="0">
          </div>
        </div>
        <textarea name="payload" placeholder='For templates: optional JSON body parameters / full components. For text: message body.'></textarea>
        <button type="submit">Send Bulk</button>
        <div class="small">Note: Marketing/out-of-session must use approved templates.</div>
      </form>
    </div>

    <div id="toast"></div>

    <div class="scrollwrap">
      <table id="tbl">
        <thead>
          <tr>
            <th>When</th><th>Dir</th><th>From</th><th>To</th><th>Name</th>
            <th>Type</th><th>Status</th><th>Body / Media</th><th>Quick</th>
          </tr>
        </thead>
        <tbody id="tbody">
        {% for m in messages %}
          <tr data-id="{{m.id}}">
            <td>{{m.created_fmt}}</td>
            <td><span class="badge">{{m.direction}}</span></td>
            <td>{{m.wa_from or ""}}</td>
            <td>{{m.wa_to or ""}}</td>
            <td>{{m.name or ""}}</td>
            <td>{{m.type}}</td>
            <td><span class="{{m.status_class}}">{{m.status or "—"}}</span></td>
            <td class="mono" style="max-width:520px;white-space:pre-wrap">
              {% if m.media_link %}
                <a href="{{m.media_link}}" target="_blank" rel="noopener">Download {{m.type}}</a>
                <div style="opacity:.75">{{m.preview}}</div>
              {% else %}
                {{m.preview}}
              {% endif %}
            </td>
            <td class="qa">
              {% if m.direction == 'in' and m.wa_from %}
                <a href="/quick?to={{m.wa_from}}&msg=%F0%9F%91%8D&redir={{active_dir}}">👍</a>
                <a href="/quick?to={{m.wa_from}}&msg={{m.ack_msg_enc}}&redir={{active_dir}}">Auto-Reply</a>
              {% elif m.direction == 'out' and m.wa_to %}
                <a href="/quick?to={{m.wa_to}}&msg=Resending%20this.&redir={{active_dir}}">Resend</a>
              {% endif %}
            </td>
          </tr>
        {% endfor %}
        </tbody>
      </table>
    </div>
  </div>
</div>

<script>
(function(){
  const params = new URLSearchParams(location.search);
  const ok = params.get('sent'); const err = params.get('err');
  const toast = document.getElementById('toast');
  function show(msg, isErr){
    toast.textContent = msg; toast.className = isErr ? 'error' : '';
    toast.style.display = 'block'; setTimeout(()=>{ toast.style.display='none'; }, 1800);
  }
  if(ok==='1'){ show('Message sent ✓', false); }
  if(err){ show('Failed: ' + err, true); }

  // live polling every ~5s
  const dir = (new URLSearchParams(location.search).get('dir')) || 'in';
  const tbody = document.getElementById('tbody');
  function currentTopId(){
    const tr = tbody.querySelector('tr[data-id]');
    return tr ? parseInt(tr.getAttribute('data-id')) : 0;
  }
  async function poll(){
    try{
      const since = currentTopId();
      const res = await fetch(`/api/messages?dir=${encodeURIComponent(dir)}&since_id=${since}`);
      if(!res.ok) return;
      const items = await res.json(); // newest first
      if(items.length){
        for(const m of items){
          const tr = document.createElement('tr');
          tr.setAttribute('data-id', m.id);
          tr.innerHTML = `
            <td>${m.created_fmt || m.created_at || ''}</td>
            <td><span class="badge">${m.direction||''}</span></td>
            <td>${m.wa_from||''}</td>
            <td>${m.wa_to||''}</td>
            <td>${m.name||''}</td>
            <td>${m.type||''}</td>
            <td><span class="${m.status_class||'badge'}">${m.status||'—'}</span></td>
            <td class="mono" style="max-width:520px;white-space:pre-wrap">
              ${
                m.media_link
                  ? `<a href="${m.media_link}" target="_blank" rel="noopener">Download ${m.type}</a><div style="opacity:.75">${m.preview||''}</div>`
                  : (m.preview||'')
              }
            </td>
            <td class="qa">
              ${
                m.direction==='in' && m.wa_from
                ? `<a href="/quick?to=${encodeURIComponent(m.wa_from)}&msg=%F0%9F%91%8D&redir=${dir}">👍</a>
                   <a href="/quick?to=${encodeURIComponent(m.wa_from)}&msg=${m.ack_msg_enc || ''}&redir=${dir}">Auto-Reply</a>`
                : (m.direction==='out' && m.wa_to
                   ? `<a href="/quick?to=${encodeURIComponent(m.wa_to)}&msg=Resending%20this.&redir=${dir}">Resend</a>`
                   : ``)
              }
            </td>`;
          tbody.insertBefore(tr, tbody.firstChild);
        }
        show(`+${items.length} new`, false);
      }
    }catch(e){ /* silent */ }
  }
  setInterval(poll, 5000);
})();
</script>
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

    try:
        if kind == "text":
            # Simple text message
            do_send(to, kind="text", text=raw_text)
        else:
            # TEMPLATE
            components = None

            # 1) Advanced mode: user pasted full components JSON in textarea
            if raw_text:
                try:
                    loaded = json.loads(raw_text)
                    if isinstance(loaded, list):
                        components = loaded
                    elif isinstance(loaded, dict):
                        # allow a single component object
                        components = [loaded]
                except Exception:
                    components = None

            # 2) Simple mode: header_type + header_url → build header component
            if components is None and header_type in ("image", "video") and header_url:
                components = [
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
                ]
                # (Body variables can be added later via extra UI if you want.)

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
            # TEMPLATE
            payload_str = (request.form.get("payload") or "").strip()
            components = None

            # 1) Advanced mode: full components JSON in payload textarea
            if payload_str:
                try:
                    loaded = json.loads(payload_str)
                    if isinstance(loaded, list):
                        components = loaded
                    elif isinstance(loaded, dict):
                        components = [loaded]
                except Exception:
                    components = None

            # 2) Simple mode: header_type + URL
            if components is None and header_type in ("image", "video") and header_url:
                components = [
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
                ]

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
        # summarize
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
    # Decode URL-encoded message (so 👍 and ACK render correctly)
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
    p = request.get_json(force=True, silent=False)
    to = p.get("to"); kind = p.get("kind","text")
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
    # Write UTF-8 BOM so Excel correctly detects encoding (Arabic, etc.)
    buf.write("\\ufeff")

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
        body = (m.get("body") or "").replace("\\n", " ").replace("\\r", " ")
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
