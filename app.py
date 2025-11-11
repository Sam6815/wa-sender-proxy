import os, json, sqlite3, datetime
from pathlib import Path
from flask import Flask, request, jsonify, render_template_string, send_from_directory
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
        )""")
        c.commit()

def store_message(**kw):
    fields = ("created_at","direction","wa_from","wa_to","wa_id","name",
              "type","body","status","conversation_id","conversation_category")
    values = [kw.get("created_at") or datetime.datetime.utcnow().isoformat()] + [kw.get(f) for f in fields[1:]]
    with sqlite3.connect(DB_PATH) as c:
        c.execute(f"INSERT INTO messages ({','.join(fields)}) VALUES ({','.join(['?']*len(fields))})", values)
        c.commit()

def fetch_messages(limit=200):
    with sqlite3.connect(DB_PATH) as c:
        c.row_factory = sqlite3.Row
        cur = c.execute("SELECT * FROM messages ORDER BY id DESC LIMIT ?", (limit,))
        return [dict(r) for r in cur.fetchall()]

init_db()

# -------- Helpers --------
def graph_post(path, payload):
    if not WA_PNID or not WA_TOKEN:
        raise RuntimeError("WA_PNID/WA_TOKEN not configured.")
    url = f"{GRAPH_BASE}/{path.lstrip('/')}"
    r = requests.post(url,
                      headers={"Authorization": f"Bearer {WA_TOKEN}", "Content-Type":"application/json"},
                      data=json.dumps(payload),
                      timeout=15)
    if not r.ok:
        raise RuntimeError(f"POST {url} -> {r.status_code} {r.text}")
    return r.json()

def graph_get(path):
    if not WA_TOKEN:
        raise RuntimeError("WA_TOKEN not configured.")
    url = f"{GRAPH_BASE}/{path.lstrip('/')}"
    r = requests.get(url, headers={"Authorization": f"Bearer {WA_TOKEN}"}, timeout=15)
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
                    # if outer keyed by type, use it; else assume inner object
                    payload = obj.get(t) if t in obj and isinstance(obj.get(t), dict) else obj
                mid = (payload or {}).get("id")
                caption = (payload or {}).get("caption") if t == "image" else None

                if mid and base:
                    m["media_link"] = f"{base}/wa/media/{mid}"
                m["preview"] = (caption or f"{t.title()} • media_id={mid or 'n/a'}")
            except Exception:
                # fallback: keep trimmed raw
                if isinstance(m["preview"], str) and len(m["preview"]) > 1500:
                    m["preview"] = m["preview"][:1500] + "…"
        else:
            if isinstance(m["preview"], str) and len(m["preview"]) > 1500:
                m["preview"] = m["preview"][:1500] + "…"

        out.append(m)
    return out

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
def send():
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
    if not to: return jsonify(error="missing 'to'"), 400

    if kind == "text":
        out = {"messaging_product":"whatsapp","to":to,"type":"text","text":{"body":p.get("text","")}}
    elif kind == "template":
        tpl = p.get("template") or {}
        name = tpl.get("name"); lang = tpl.get("language") or "en"
        if not name: return jsonify(error="template.name required"), 400
        t = {"name": name, "language": {"code": lang}}
        if tpl.get("components"): t["components"] = tpl["components"]
        out = {"messaging_product":"whatsapp","to":to,"type":"template","template":t}
    else:
        return jsonify(error="unsupported kind"), 400

    try:
        resp = graph_post(f"{WA_PNID}/messages", out)
        wa_id = (resp.get("messages") or [{}])[0].get("id")
        conv = resp.get("conversation") or {}
        store_message(
            direction="out",
            wa_from=None, wa_to=to, wa_id=wa_id, name=None,
            type=out["type"], body=json.dumps(out, ensure_ascii=False), status="sent",
            conversation_id=conv.get("id"), conversation_category=conv.get("category"),
        )
        return jsonify(resp), 200
    except Exception as e:
        return jsonify(error=str(e)), 500

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

# -------- Inbox UI --------
INBOX_HTML = """
<!doctype html><html lang="en"><meta charset="utf-8">
<title>WhatsApp Inbox</title><meta name="viewport" content="width=device-width,initial-scale=1">
<style>
 body{font-family:system-ui,Segoe UI,Roboto,Arial,sans-serif;margin:24px;max-width:1080px}
 h1{margin:0 0 16px}
 table{border-collapse:collapse;width:100%}
 th,td{border-bottom:1px solid #eee;padding:8px 6px;text-align:left;vertical-align:top}
 .badge{display:inline-block;padding:.1rem .45rem;border-radius:8px;background:#eef}
 form{display:flex;gap:8px;margin:16px 0;flex-direction:column}
 input,select,textarea,button{padding:.55rem .7rem;font:inherit}
 textarea{width:100%;height:80px}
 .row{display:flex;gap:8px;flex-wrap:wrap}
 .mono{font-family:ui-monospace,Menlo,Consolas,monospace}
</style>
<h1>WhatsApp Inbox</h1>

<form method="post" action="/inbox/send">
  <div class="row" style="width:100%">
    <input name="to" placeholder="+9617xxxxxx" required style="min-width:220px">
    <select name="kind"><option value="text" selected>Text</option><option value="template">Template</option></select>
    <input name="tpl_name" placeholder="template name (if template)">
    <input name="tpl_lang" placeholder="en" value="en" style="width:72px">
    <button type="submit">Send</button>
  </div>
  <textarea name="text" placeholder="Text body OR JSON array of template components"></textarea>
</form>

<table>
  <thead>
    <tr><th>When</th><th>Dir</th><th>From</th><th>To</th><th>Name</th><th>Type</th><th>Status</th><th>Body / Media</th></tr>
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
      <td class="mono" style="max-width:600px;white-space:pre-wrap">
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
</html>
"""

@app.get("/inbox")
def inbox():
    base = request.url_root
    rows = _massage_messages(fetch_messages(200), base)
    return render_template_string(INBOX_HTML, messages=rows)

@app.post("/inbox/send")
def inbox_send():
    to = request.form.get("to")
    kind = request.form.get("kind", "text")
    text = (request.form.get("text") or "").strip()
    tpl_name = request.form.get("tpl_name") or ""
    tpl_lang = request.form.get("tpl_lang") or "en"

    body = {"to": to, "kind": kind}
    if kind == "text":
        body["text"] = text
    else:
        comps = None
        if text:
            try: comps = json.loads(text)
            except: comps = None
        body["template"] = {"name": tpl_name, "language": tpl_lang}
        if comps: body["template"]["components"] = comps

    r = requests.post(request.url_root.rstrip("/") + "/send",
                      headers={"Content-Type": "application/json"},
                      data=json.dumps(body))
    if not r.ok:
        return f"Send failed: {r.status_code} {r.text}", 400
    return ("<script>location.href='/inbox'</script>", 200, {"Content-Type": "text/html"})

# -------- Privacy (optional) --------
@app.get("/privacy")
def privacy():
    # put a privacy.html into /static if you want to expose one
    p = STATIC_DIR / "privacy.html"
    if p.exists():
        return send_from_directory(app.static_folder, "privacy.html")
    return "No privacy.html uploaded", 200

# -------- Health --------
@app.get("/")
def health():
    return jsonify(ok=True, pnid=bool(WA_PNID))

if __name__ == "__main__":
    # For local dev; Render uses Gunicorn (Procfile)
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8000")), debug=True)

