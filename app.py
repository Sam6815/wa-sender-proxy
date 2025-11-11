import os, json, sqlite3, datetime
from pathlib import Path
from flask import Flask, request, jsonify, render_template_string, send_from_directory, redirect, url_for
import requests

# -------- Config (env) --------
VERIFY_TOKEN = os.getenv("WHATSAPP_VERIFY_TOKEN", "dev-verify")
WA_PNID      = os.getenv("WA_PNID")      # e.g. 886670621191094
WA_TOKEN     = os.getenv("WA_TOKEN")     # long-lived system user token
GRAPH_BASE   = "https://graph.facebook.com/v21.0"

APP_DIR = Path(__file__).resolve().parent
DATA_DIR = APP_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)
DB_PATH  = DATA_DIR / "messages.db"
STATIC_DIR = APP_DIR / "static"
STATIC_DIR.mkdir(exist_ok=True)

# -------- App --------
app = Flask(__name__, static_folder=str(STATIC_DIR))
app.url_map.strict_slashes = False  # accept /webhook and /webhook/

# -------- DB --------
def init_db():
    with sqlite3.connect(DB_PATH) as c:
        c.execute("""
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
        c.commit()

def store_message(**kw):
    fields = ("created_at","direction","wa_from","wa_to","wa_id","name",
              "type","body","status","conversation_id","conversation_category")
    values = [kw.get("created_at") or datetime.datetime.utcnow().isoformat()] + [kw.get(f) for f in fields[1:]]
    with sqlite3.connect(DB_PATH) as c:
        c.execute(f"INSERT INTO messages ({','.join(fields)}) VALUES ({','.join(['?']*len(fields))})", values)
        c.commit()

def fetch_messages(limit=200, direction=None):
    """
    Fetch last N messages. If direction in {'in','out'} is provided, filter in SQL.
    """
    with sqlite3.connect(DB_PATH) as c:
        c.row_factory = sqlite3.Row
        if direction in {"in","out"}:
            cur = c.execute(
                "SELECT * FROM messages WHERE direction = ? ORDER BY id DESC LIMIT ?",
                (direction, limit),
            )
        else:
            cur = c.execute("SELECT * FROM messages ORDER BY id DESC LIMIT ?", (limit,))
        return [dict(r) for r in cur.fetchall()]

init_db()

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

def _massage_messages(rows, base_url):
    """Enrich rows with media_link (when possible) and a clean preview.
       Supports both shapes:
       A) {"image": {"id": "...", "caption": "..."}}
       B) {"id": "...", "mime_type": "...", "caption": "..."} (inner object stored)
    """
    out = []
    base = (base_url or "").rstrip("/")
    for m in rows:
        m = dict(m)
        m["preview"] = m.get("body") or ""
        m["media_link"] = None
        t = (m.get("type") or "").lower()

        if t in {"image","audio","video","document","sticker"} and m.get("body"):
            try:
                obj = json.loads(m["body"]) if isinstance(m["body"], str) else (m["body"] or {})
                payload = None
                if isinstance(obj, dict):
                    payload = obj.get(t) if t in obj and isinstance(obj.get(t), dict) else obj
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

# Core send logic (used by both /send API and /inbox/send form)
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
    elif kind == "template":
        tpl = template or {}
        name = tpl.get("name")
        lang = tpl.get("language") or "en"
        if not name:
            raise RuntimeError("template.name required")
        t = {"name": name, "language": {"code": lang}}
        if tpl.get("components"):
            t["components"] = tpl["components"]
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

# -------- Webhook (GET verify + POST events) --------
@app.get("/webhook")
def webhook_verify():
    if (request.args.get("hub.mode") == "subscribe"
        and request.args.get("hub.verify_token") == VERIFY_TOKEN):
        return request.args.get("hub.challenge", ""), 200
    return "forbidden", 403

@app.post("/webhook")
def webhook_inbound():
    data = request.get_json(force=True, silent=True) or {}
    print("WEBHOOK:", json.dumps(data, ensure_ascii=False), flush=True)

    # Only handle WhatsApp events
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

                # Inbound messages
                contacts = value.get("contacts") or [{}]
                profile_name = (contacts[0].get("profile") or {}).get("name") if contacts else None
                meta = value.get("metadata") or {}
                for msg in (value.get("messages") or []):
                    mtype = msg.get("type")
                    if mtype == "text":
                        body = (msg.get("text") or {}).get("body", "")
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
    except Exception as e:
        print("Webhook parse error:", e, flush=True)

    return jsonify(status="ok"), 200

# -------- Send API --------
@app.post("/send")
def send_api():
    """
    JSON:
    {
      "to": "+9617xxxxxx",
      "kind": "text" | "template",
      "text": "Hi",                      # when kind=text
      "template": {                      # when kind=template
        "name": "hello_world1",
        "language": "en",
        "components": [ ... ]            # optional
      }
    }
    """
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

# -------- Media download (works on Render) --------
@app.get("/wa/media/<media_id>")
def wa_media(media_id):
    # Step 1: resolve the media URL
    meta = graph_get(media_id)    # -> {"url": "..."}
    url = meta.get("url")
    if not url:
        return jsonify(error="no url for media"), 404
    # Step 2: download bytes from lookaside
    r = requests.get(url, headers={"Authorization": f"Bearer {WA_TOKEN}"}, timeout=25)
    if not r.ok:
        return (r.text, r.status_code, {"Content-Type": "application/json"})
    ctype = r.headers.get("Content-Type", "application/octet-stream")
    return (r.content, 200, {"Content-Type": ctype})

# -------- Inbox UI (WhatsApp-ish theme + Tabs + Scrollable) --------
INBOX_HTML = """
<!doctype html><html lang="en"><meta charset="utf-8">
<title>WhatsApp Inbox</title><meta name="viewport" content="width=device-width,initial-scale=1">
<style>
 :root{
   --wa-green:#25D366;       /* WhatsApp green */
   --wa-dark:#075E54;        /* WhatsApp dark header */
   --wa-light:#DCF8C6;       /* WhatsApp light bubble */
   --wa-bg:#f6f7f9;
   --text:#0f172a;
 }
 body{font-family:system-ui,Segoe UI,Roboto,Arial,sans-serif;margin:0;background:var(--wa-bg);color:var(--text)}
 .wrap{max-width:1200px;margin:0 auto;padding:20px}
 .card{background:#fff;border-radius:12px;box-shadow:0 10px 24px rgba(0,0,0,.06);overflow:hidden;border:1px solid #eef2f7}

 .topbar{background:var(--wa-dark);color:#fff;padding:14px 18px;display:flex;align-items:center;gap:12px}
 .pill{display:inline-flex;align-items:center;gap:8px;padding:8px 12px;border-radius:999px;
       background:#fff1; color:#fff; font-weight:600; letter-spacing:.2px}

 .tabs{display:flex;gap:8px;padding:10px;background:#fff;border-bottom:1px solid #eef2f7}
 .tab{padding:8px 14px;border-radius:10px;border:1px solid #e5e7eb;color:#334155;text-decoration:none}
 .tab.active{background:var(--wa-green);color:#fff;border-color:var(--wa-green)}
 .tab:hover{border-color:#cbd5e1}

 .compose{padding:16px;display:flex;flex-direction:column;gap:10px;background:#fff}
 .row{display:flex;gap:8px;flex-wrap:wrap}
 input,select,textarea,button{
   padding:.6rem .7rem;font:inherit;border:1px solid #d1d5db;border-radius:10px;outline:none
 }
 input:focus,select:focus,textarea:focus{border-color:var(--wa-green);box-shadow:0 0 0 3px rgba(37,211,102,.18)}
 button{background:var(--wa-green);color:#fff;border:1px solid var(--wa-green);cursor:pointer}
 button:hover{filter:brightness(.95)}
 textarea{width:100%;height:80px}

 .scrollwrap{max-height:70vh;overflow:auto;background:#fff}
 table{border-collapse:collapse;width:100%}
 th,td{border-bottom:1px solid #eef2f7;padding:10px 8px;text-align:left;vertical-align:top}
 thead th{position:sticky;top:0;background:#fff}
 .badge{display:inline-block;padding:.12rem .5rem;border-radius:999px;background:var(--wa-light);border:1px solid #c9eec2}
 .mono{font-family:ui-monospace,Menlo,Consolas,monospace}

 /* Toast */
 #toast{position:fixed;right:16px;bottom:16px;z-index:9999;display:none;
        background:#16a34a;color:#fff;padding:.6rem .8rem;border-radius:10px;box-shadow:0 6px 18px rgba(0,0,0,.2)}
 #toast.error{background:#dc2626}
</style>

<div class="topbar">
  <div class="pill">WhatsApp Inbox</div>
</div>

<div class="wrap">
  <div class="card">
    <div class="tabs">
      <a class="tab {% if active_dir == 'in' %}active{% endif %}" href="/inbox?dir=in">Inbox</a>
      <a class="tab {% if active_dir == 'out' %}active{% endif %}" href="/inbox?dir=out">Sent</a>
    </div>

    <form class="compose" method="post" action="/inbox/send">
      <div class="row" style="width:100%">
        <input name="to" placeholder="+9617xxxxxx" required style="min-width:220px">
        <select name="kind">
          <option value="text" selected>Text</option>
          <option value="template">Template</option>
        </select>
        <input name="tpl_name" placeholder="template name (if template)">
        <input name="tpl_lang" placeholder="en" value="en" style="width:72px">
        <button type="submit">Send</button>
      </div>
      <textarea name="text" placeholder="Text body OR JSON array of template components"></textarea>
    </form>

    <div id="toast"></div>

    <div class="scrollwrap">
      <table>
        <thead>
          <tr>
            <th>When</th><th>Dir</th><th>From</th><th>To</th><th>Name</th>
            <th>Type</th><th>Status</th><th>Body / Media</th>
          </tr>
        </thead>
        <tbody>
        {% for m in messages %}
          <tr>
            <td>{{m.created_at}}</td>
            <td><span class="badge">{{m.direction}}</span></td>
            <td>{{m.wa_from or ""}}</td>
            <td>{{m.wa_to or ""}}</td>
            <td>{{m.name or ""}}</td>
            <td>{{m.type}}</td>
            <td>{{m.status or ""}}</td>
            <td class="mono" style="max-width:640px;white-space:pre-wrap">
              {% if m.media_link %}
                <a href="{{m.media_link}}" target="_blank" rel="noopener">Download {{m.type}}</a>
                <div style="opacity:.75">{{m.preview}}</div>
              {% else %}
                {{m.preview}}
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
  // Toast from URL params
  const params = new URLSearchParams(location.search);
  const ok = params.get('sent');
  const err = params.get('err');
  const toast = document.getElementById('toast');
  function show(msg, isErr){
    toast.textContent = msg;
    toast.className = isErr ? 'error' : '';
    toast.style.display = 'block';
    setTimeout(()=>{ toast.style.display='none'; }, 2200);
  }
  if(ok==='1'){ show('Message sent ✓', false); }
  if(err){ show('Failed: ' + err, true); }
})();
</script>
</html>
"""

# ---- Routes for Inbox UI (with dir tabs) ----
@app.get("/inbox")
def inbox():
    active_dir = request.args.get("dir") or "in"  # default to Inbox
    if active_dir not in {"in","out"}:
        active_dir = "in"
    base = request.url_root
    rows = _massage_messages(fetch_messages(200, direction=active_dir), base)
    return render_template_string(INBOX_HTML, messages=rows, active_dir=active_dir)

@app.post("/inbox/send")
def inbox_send():
    to = request.form.get("to")
    kind = request.form.get("kind", "text")
    text = (request.form.get("text") or "").strip()
    tpl_name = request.form.get("tpl_name") or ""
    tpl_lang = request.form.get("tpl_lang") or "en"
    try:
        if kind == "text":
            do_send(to, kind="text", text=text)
        else:
            comps = None
            if text:
                try: comps = json.loads(text)
                except: comps = None
            tpl = {"name": tpl_name, "language": tpl_lang}
            if comps: tpl["components"] = comps
            do_send(to, kind="template", template=tpl)
        # Redirect to Sent tab with success toast
        return redirect(url_for('inbox', dir='out', sent='1'))
    except Exception as e:
        return redirect(url_for('inbox', dir='out', err=str(e)))

# -------- Privacy (optional) --------
@app.get("/privacy")
def privacy():
    p = STATIC_DIR / "privacy.html"
    if p.exists():
        return send_from_directory(app.static_folder, "privacy.html")
    return "No privacy.html uploaded", 200

# -------- Health --------
@app.get("/")
def health():
    return jsonify(ok=True, pnid=bool(WA_PNID))

# -------- Send API (JSON) --------
@app.post("/send")
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

# -------- Media download --------
@app.get("/wa/media/<media_id>")
def wa_media(media_id):
    meta = graph_get(media_id)    # -> {"url": "..."}
    url = meta.get("url")
    if not url:
        return jsonify(error="no url for media"), 404
    r = requests.get(url, headers={"Authorization": f"Bearer {WA_TOKEN}"}, timeout=25)
    if not r.ok:
        return (r.text, r.status_code, {"Content-Type": "application/json"})
    ctype = r.headers.get("Content-Type", "application/octet-stream")
    return (r.content, 200, {"Content-Type": ctype})

if __name__ == "__main__":
    # For local dev; Render uses Gunicorn (Procfile)
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8000")), debug=True)
