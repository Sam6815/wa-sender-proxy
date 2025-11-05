import os, json, requests
from flask import Flask, request, jsonify

GRAPH = "https://graph.facebook.com/v21.0"
WA_PNID   = os.getenv("WA_PNID")          # e.g. 886670621191094
WA_TOKEN  = os.getenv("WA_TOKEN")         # long-lived system user token
PROXY_KEY = os.getenv("PROXY_KEY")        # shared secret with PythonAnywhere

app = Flask(__name__)

@app.post("/wa/send")
def wa_send():
    if PROXY_KEY and request.headers.get("X-Proxy-Key") != PROXY_KEY:
        return jsonify(error="unauthorized"), 401
    payload = request.get_json(force=True)
    r = requests.post(
        f"{GRAPH}/{WA_PNID}/messages",
        headers={"Authorization": f"Bearer {WA_TOKEN}", "Content-Type": "application/json"},
        data=json.dumps(payload),
        timeout=12
    )
    return (r.text, r.status_code, {"Content-Type":"application/json"})

@app.get("/")
def ok():
    return jsonify(ok=True)
