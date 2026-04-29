import os
import re
import uuid
import time
import json
import shutil
import select
import signal
import logging
import subprocess
import threading
from urllib.parse import urlparse

import requests
import redis
import yt_dlp
from flask import Flask, request, jsonify, Response, stream_with_context, g, has_request_context
from flask_cors import CORS
from flask_limiter import Limiter
from cachetools import TTLCache

# --- VERIFY SYSTEM ---
def verify_binaries():
    """Render check: Ensures FFmpeg and yt-dlp are installed via the build command."""
    for tool in ["yt-dlp", "ffmpeg"]:
        if not shutil.which(tool):
            # This will show up in Render's build/service logs
            print(f"CRITICAL: {tool} missing. Check Build Command.")

verify_binaries()

# --- CONFIG ---
class Config:
    REDIS_URL = os.environ.get("REDIS_URL") # Render provides this
    PO_TOKEN = os.environ.get("YT_PO_TOKEN")
    CHUNK_SIZE = 262144
    PORT = int(os.environ.get("PORT", 10000))
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0',
        'Accept-Encoding': 'identity',
        'Connection': 'keep-alive'
    }

app = Flask(__name__)
CORS(app)
session = requests.Session()

# --- REDIS ---
try:
    if Config.REDIS_URL:
        r_client = redis.Redis.from_url(Config.REDIS_URL, decode_responses=True)
        r_client.ping()
    else:
        r_client = None
except:
    r_client = None

l1_cache = TTLCache(maxsize=200, ttl=3600)
lock = threading.Lock()

# --- CIRCUIT BREAKER ---
class CircuitBreaker:
    def __init__(self, threshold=5, recovery_time=60):
        self.failures = 0
        self.threshold = threshold
        self.recovery_time = recovery_time
        self.last_failure_time = 0

    def is_open(self):
        if self.failures >= self.threshold:
            if time.time() - self.last_failure_time > self.recovery_time:
                self.failures = 0
                return False
            return True
        return False

    def record_failure(self):
        self.failures += 1
        self.last_failure_time = time.time()

    def record_success(self):
        self.failures = 0

cb = CircuitBreaker()
cb_lock = threading.Lock()

# --- LOGGING ---
@app.before_request
def start_trace():
    g.start_time = time.time()
    # Use Render's request ID if available
    g.req_id = request.headers.get("X-Render-Request-Id", str(uuid.uuid4()))

logging.basicConfig(level=logging.INFO, format='[%(req_id)s] %(levelname)s: %(message)s')
logger = logging.getLogger("gateway")

class ContextFilter(logging.Filter):
    def filter(self, record):
        if has_request_context():
            record.req_id = getattr(g, 'req_id', 'N/A')
        else:
            record.req_id = 'SYSTEM'
        return True

logger.addFilter(ContextFilter())

# --- RATE LIMIT ---
def get_ip():
    # Render uses Cloudflare/Load Balancers. Real IP is in X-Forwarded-For.
    fwd = request.headers.get("X-Forwarded-For")
    return fwd.split(",")[0] if fwd else request.remote_addr

limiter = Limiter(
    key_func=get_ip,
    app=app,
    storage_uri=Config.REDIS_URL if Config.REDIS_URL else "memory://",
    default_limits=["500/day", "100/hour"]
)

# --- META ENGINE ---
def get_meta(url, fid):
    if cb.is_open():
        raise RuntimeError("Circuit breaker active")

    key = f"meta:{url}:{fid}"

    with lock:
        if key in l1_cache: return l1_cache[key]

    if r_client:
        cached = r_client.get(key)
        if cached:
            data = json.loads(cached)
            with lock: l1_cache[key] = data
            return data

    try:
        opts = {
            'quiet': True,
            'format': fid,
            'http_headers': Config.HEADERS,
            'extractor_args': {'youtube': {'player_client': ['android', 'web']}}
        }
        if Config.PO_TOKEN:
            opts['extractor_args']['youtube']['po_token'] = [f'web+{Config.PO_TOKEN}']

        with yt_dlp.YoutubeDL(opts) as ydl:
            raw = ydl.extract_info(url, download=False)

        with cb_lock: cb.record_success()

        meta = {
            "url": raw.get("url"),
            "title": re.sub(r'[\\/*?:"<>|]', "", raw.get("title") or "video"),
            "size": raw.get("filesize") or raw.get("filesize_approx"),
            "ext": raw.get("ext", "mp4"),
            "is_combined": raw.get('vcodec') != 'none' and raw.get('acodec') != 'none'
        }

        with lock: l1_cache[key] = meta
        if r_client:
            r_client.setex(key, 10800, json.dumps(meta))
        
        return meta

    except Exception as e:
        with cb_lock: cb.record_failure()
        raise e

# --- MUX ENGINE ---
def mux_stream(url, fid):
    fmt = f"{fid}+bestaudio/best" if fid != "best" else "best"
    cmd = [
        "yt-dlp", "-f", fmt, 
        "--merge-output-format", "mp4", 
        "-o", "-", 
        "--quiet", "--no-warnings", 
        url
    ]

    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # 15 Minute timeout for long videos
    timer = threading.Timer(900, p.kill)
    timer.start()

    try:
        while True:
            # Check if subprocess died
            if p.poll() is not None: break

            # Non-blocking read
            ready, _, _ = select.select([p.stdout], [], [], 30)
            if not ready: break

            chunk = p.stdout.read(Config.CHUNK_SIZE)
            if not chunk: break
            yield chunk
    finally:
        if p.poll() is None:
            p.terminate()
            p.wait()
        timer.cancel()

# --- ROUTES ---
@app.route('/health')
def health():
    return jsonify({
        "status": "online",
        "latency_ms": round((time.time() - g.start_time) * 1000, 2),
        "circuit": "open" if cb.is_open() else "closed",
        "redis": "connected" if r_client else "disconnected"
    })

@app.route('/api/proxy', methods=['GET', 'HEAD'])
@limiter.limit("20/minute")
def proxy():
    url = request.args.get("url", "")
    fid = request.args.get("format_id", "best")

    if not re.match(r'^[\w+.-]+$', fid):
        return "Invalid format", 400

    try:
        parsed = urlparse(url)
        host = parsed.netloc.lower()
        if not (host == "youtube.com" or host.endswith(".youtube.com") or host == "youtu.be"):
            return "Forbidden", 403
    except:
        return "Invalid URL", 400

    try:
        meta = get_meta(url, fid)
        headers = {
            "Content-Disposition": f'attachment; filename="{meta["title"]}.{meta["ext"]}"',
            "Content-Type": f"video/{meta['ext']}",
            "Accept-Ranges": "bytes",
            "X-Content-Type-Options": "nosniff"
        }

        if meta["is_combined"] and meta["size"]:
            headers["Content-Length"] = str(meta["size"])

        if request.method == "HEAD":
            return Response(None, headers=headers)

        def generate():
            success = False
            if meta["is_combined"] and meta["url"]:
                sent = 0
                for attempt in range(2):
                    try:
                        h = Config.HEADERS.copy()
                        if sent: h["Range"] = f"bytes={sent}-"

                        with session.get(meta["url"], stream=True, headers=h, timeout=(10, 30)) as r:
                            if sent and r.status_code != 206: break
                            r.raise_for_status()

                            for chunk in r.iter_content(Config.CHUNK_SIZE):
                                if not chunk: break
                                yield chunk
                                sent += len(chunk)
                        success = True
                        break
                    except Exception as e:
                        logger.warning(f"Fast path attempt {attempt} failed. Retrying...")

            if not success:
                logger.info("Falling back to Mux-Stream")
                yield from mux_stream(url, fid)

        return Response(stream_with_context(generate()), headers=headers)

    except Exception as e:
        logger.error(f"Request failed: {e}")
        return "Service Unavailable", 503

# --- RUN ---
if __name__ == "__main__":
    # Render uses the PORT env var
    app.run(host="0.0.0.0", port=Config.PORT)
