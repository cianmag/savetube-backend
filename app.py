import os
import re
import uuid
import time
import json
import shutil
import logging
import threading
from urllib.parse import urlparse

import redis
import yt_dlp
from flask import Flask, request, jsonify, g, has_request_context
from flask_cors import CORS
from flask_limiter import Limiter
from cachetools import TTLCache

# ============================================================
# STARTUP CHECKS
# ============================================================
def verify_binaries():
    results = {}
    for tool in ["yt-dlp", "ffmpeg"]:
        path = shutil.which(tool)
        results[tool] = path
        status = f"OK at {path}" if path else "MISSING - Check build.sh"
        print(f"[STARTUP] {tool}: {status}")
    return results

BINARY_STATUS = verify_binaries()

# ============================================================
# CONFIG
# ============================================================
class Config:
    REDIS_URL      = os.environ.get("REDIS_URL")
    PO_TOKEN       = os.environ.get("YT_PO_TOKEN")
    PORT           = int(os.environ.get("PORT", 10000))
    CACHE_TTL      = 3600        # 1 hour (URLs expire ~6hrs)
    CACHE_MAX      = 500
    REDIS_KEY_TTL  = 3600

    # yt-dlp base options
 YDL_OPTS = {
    'quiet': True,
    'no_warnings': True,
    'extractor_args': {
        'youtube': {
            # ONLY android (remove 'web')
            'player_client': ['android'],
            'player_skip': ['webpage', 'configs']
        }
    },
    'http_headers': {
        'User-Agent': (
            'com.google.android.youtube/17.36.4 '
            '(Linux; U; Android 12; GB) gzip'
        )
    }
}

# ============================================================
# FLASK APP
# ============================================================
app = Flask(__name__)

# Allow all origins - tighten this to your frontend domain in production
# e.g. CORS(app, origins=["https://yoursite.github.io"])
CORS(app, origins="*")

# ============================================================
# REDIS
# ============================================================
def create_redis_client():
    if not Config.REDIS_URL:
        print("[STARTUP] REDIS_URL not set - using memory cache only")
        return None
    try:
        client = redis.Redis.from_url(
            Config.REDIS_URL,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5
        )
        client.ping()
        print("[STARTUP] Redis connected successfully")
        return client
    except Exception as e:
        print(f"[STARTUP] Redis failed: {e} - using memory cache only")
        return None

r_client = create_redis_client()

# L1 in-memory cache (always available)
l1_cache  = TTLCache(maxsize=Config.CACHE_MAX, ttl=Config.CACHE_TTL)
cache_lock = threading.Lock()

# ============================================================
# LOGGING
# ============================================================
class RequestContextFilter(logging.Filter):
    def filter(self, record):
        try:
            record.req_id = getattr(g, 'req_id', 'SYSTEM') \
                if has_request_context() else 'SYSTEM'
        except RuntimeError:
            record.req_id = 'SYSTEM'
        return True

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(req_id)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("resolver")
logger.addFilter(RequestContextFilter())

# ============================================================
# REQUEST LIFECYCLE
# ============================================================
@app.before_request
def start_trace():
    g.start_time = time.time()
    g.req_id     = request.headers.get(
        "X-Render-Request-Id",
        str(uuid.uuid4())[:8]
    )

@app.after_request
def log_request(response):
    elapsed = round((time.time() - g.start_time) * 1000, 2)
    logger.info(
        f"{request.method} {request.path} "
        f"→ {response.status_code} [{elapsed}ms]"
    )
    return response

# ============================================================
# RATE LIMITING
# ============================================================
def get_client_ip():
    forwarded = request.headers.get("X-Forwarded-For", "")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.remote_addr or "unknown"

limiter = Limiter(
    key_func=get_client_ip,
    app=app,
    storage_uri=Config.REDIS_URL if Config.REDIS_URL else "memory://",
    default_limits=["200/day", "50/hour"]
)

# ============================================================
# URL VALIDATION
# ============================================================
ALLOWED_HOSTS = {
    "youtube.com",
    "www.youtube.com",
    "m.youtube.com",
    "youtu.be",
    "music.youtube.com"
}

def is_valid_youtube_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            return False
        return parsed.netloc.lower().strip() in ALLOWED_HOSTS
    except Exception:
        return False

def extract_video_id(url: str) -> str | None:
    """Extract YouTube video ID for cache keying."""
    patterns = [
        r'(?:v=|youtu\.be/)([a-zA-Z0-9_-]{11})',
        r'(?:embed/|v/)([a-zA-Z0-9_-]{11})',
        r'shorts/([a-zA-Z0-9_-]{11})'
    ]
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    return None

# ============================================================
# FORMAT HELPERS
# ============================================================
def sanitize_title(raw: str) -> str:
    """Remove characters unsafe for filenames."""
    safe = re.sub(r'[\\/*?:"<>|]', "", raw or "video")
    return safe.strip() or "video"

def classify_format(fmt: dict) -> str:
    """
    Classify format for frontend display.
    
    Returns: 'video+audio', 'video-only', 'audio-only', 'unknown'
    """
    has_video = fmt.get('vcodec', 'none') not in ('none', None, '')
    has_audio = fmt.get('acodec', 'none') not in ('none', None, '')

    if has_video and has_audio:
        return 'video+audio'
    elif has_video:
        return 'video-only'
    elif has_audio:
        return 'audio-only'
    return 'unknown'

def format_filesize(size_bytes) -> str | None:
    """Human-readable file size."""
    if not size_bytes:
        return None
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} GB"

def build_format_entry(fmt: dict) -> dict:
    """Build a clean format object from raw yt-dlp format data."""
    ftype      = classify_format(fmt)
    height     = fmt.get('height')
    width      = fmt.get('width')
    fps        = fmt.get('fps')
    vcodec     = fmt.get('vcodec') or 'none'
    acodec     = fmt.get('acodec') or 'none'
    abr        = fmt.get('abr')       # Audio bitrate kbps
    vbr        = fmt.get('vbr')       # Video bitrate kbps
    tbr        = fmt.get('tbr')       # Total bitrate kbps
    filesize   = fmt.get('filesize') or fmt.get('filesize_approx')

    # Build human-readable label
    if ftype == 'video+audio' and height:
        label = f"{height}p (with audio)"
    elif ftype == 'video-only' and height:
        fps_str = f" {int(fps)}fps" if fps and fps > 30 else ""
        label   = f"{height}p{fps_str} video only"
    elif ftype == 'audio-only':
        bitrate = f" {int(abr)}kbps" if abr else ""
        codec   = acodec.split('.')[0]   # 'opus', 'mp4a', etc.
        label   = f"Audio{bitrate} ({codec})"
    else:
        label = fmt.get('format_note') or fmt.get('format_id', 'unknown')

    return {
        # Core identification
        'format_id':    fmt.get('format_id'),
        'label':        label,
        'type':         ftype,

        # Video info
        'height':       height,
        'width':        width,
        'fps':          round(fps) if fps else None,
        'vcodec':       vcodec,
        'vbr_kbps':     round(vbr) if vbr else None,

        # Audio info
        'acodec':       acodec,
        'abr_kbps':     round(abr) if abr else None,

        # General
        'ext':          fmt.get('ext'),
        'tbr_kbps':     round(tbr) if tbr else None,
        'filesize_bytes': filesize,
        'filesize_human': format_filesize(filesize),
        'protocol':     fmt.get('protocol'),
        'url':          fmt.get('url'),   # Direct CDN URL

        # Flags for frontend logic
        'has_video':    ftype in ('video+audio', 'video-only'),
        'has_audio':    ftype in ('video+audio', 'audio-only'),
        'needs_mux':    ftype == 'video-only',  # Client needs audio too
    }

# ============================================================
# CORE RESOLVER
# ============================================================
def resolve_formats(url: str) -> dict:
    """
    Main resolution function.
    Returns structured format data with caching.
    """
    video_id  = extract_video_id(url)
    cache_key = f"resolve:{video_id}" if video_id else f"resolve:{url}"

    # --- L1 Cache ---
    with cache_lock:
        if cache_key in l1_cache:
            logger.info(f"L1 hit: {cache_key}")
            return l1_cache[cache_key]

    # --- L2 Cache (Redis) ---
    if r_client:
        try:
            cached = r_client.get(cache_key)
            if cached:
                data = json.loads(cached)
                with cache_lock:
                    l1_cache[cache_key] = data
                logger.info(f"L2 hit: {cache_key}")
                return data
        except Exception as e:
            logger.warning(f"Redis read error: {e}")

    # --- Fetch from YouTube ---
    logger.info(f"Resolving: {url}")

    ydl_opts = {**Config.YDL_OPTS}

    if Config.PO_TOKEN:
        ydl_opts['extractor_args']['youtube']['po_token'] = [
            f'web+{Config.PO_TOKEN}'
        ]

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
    except yt_dlp.utils.DownloadError as e:
        err = str(e)
        if 'Sign in' in err or 'bot' in err.lower():
            raise PermissionError("YouTube bot detection triggered")
        elif 'Private video' in err:
            raise PermissionError("Video is private")
        elif 'not available' in err.lower():
            raise ValueError("Video not available in your region or deleted")
        raise RuntimeError(f"yt-dlp error: {err}")

    # --- Process Formats ---
    raw_formats = info.get('formats', [])

    # Build clean format list
    formats = [build_format_entry(f) for f in raw_formats]

    # Sort: video+audio first, then video-only by quality, then audio
    def sort_key(f):
        type_order = {'video+audio': 0, 'video-only': 1, 'audio-only': 2, 'unknown': 3}
        return (
            type_order.get(f['type'], 9),
            -(f['height'] or 0),
            -(f['fps'] or 0),
            -(f['tbr_kbps'] or 0)
        )

    formats.sort(key=sort_key)

    # --- Build Best Format Suggestions ---
    # Best combined (for simple downloads)
    best_combined = next(
        (f for f in formats if f['type'] == 'video+audio'),
        None
    )

    # Best 4K video (needs muxing)
    best_4k = next(
        (f for f in formats if f['has_video'] and (f['height'] or 0) >= 2160),
        None
    )

    # Best 1080p video (needs muxing)
    best_1080 = next(
        (f for f in formats if f['has_video'] and (f['height'] or 0) >= 1080),
        None
    )

    # Best audio only
    best_audio = next(
        (f for f in formats if f['type'] == 'audio-only'),
        None
    )

    # --- Final Response ---
    result = {
        'video_id':   video_id,
        'title':      sanitize_title(info.get('title', '')),
        'channel':    info.get('channel') or info.get('uploader'),
        'duration_s': info.get('duration'),
        'duration':   _format_duration(info.get('duration')),
        'thumbnail':  info.get('thumbnail'),
        'view_count': info.get('view_count'),
        'upload_date': info.get('upload_date'),

        # All available formats
        'formats': formats,

        # Quick-pick suggestions
        'suggestions': {
            'best_combined': best_combined,
            'best_4k':       best_4k,
            'best_1080p':    best_1080,
            'best_audio':    best_audio,
        },

        # Metadata
        'format_count': len(formats),
        'resolved_at':  int(time.time()),
        'expires_at':   int(time.time()) + Config.CACHE_TTL,
    }

    # --- Store in Caches ---
    with cache_lock:
        l1_cache[cache_key] = result

    if r_client:
        try:
            r_client.setex(cache_key, Config.REDIS_KEY_TTL, json.dumps(result))
        except Exception as e:
            logger.warning(f"Redis write error: {e}")

    logger.info(
        f"Resolved '{result['title']}' - "
        f"{len(formats)} formats found"
    )
    return result

def _format_duration(seconds) -> str | None:
    if not seconds:
        return None
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    if h:
        return f"{h}:{m:02d}:{s:02d}"
    return f"{m}:{s:02d}"

# ============================================================
# ROUTES
# ============================================================
@app.route('/health', methods=['GET'])
def health():
    """Render uses this endpoint to verify the service is alive."""
    return jsonify({
        "status":       "online",
        "latency_ms":   round((time.time() - g.start_time) * 1000, 2),
        "redis":        "connected" if r_client else "disconnected",
        "binaries": {
            tool: ("ok" if path else "missing")
            for tool, path in BINARY_STATUS.items()
        },
        "cache_size":   len(l1_cache),
    })


@app.route('/api/resolve', methods=['GET'])
@limiter.limit("30/minute;200/hour")
def resolve():
    """
    Resolve YouTube URL to direct CDN download URLs.

    Query Parameters:
        url (required): YouTube video URL

    Returns:
        JSON with video metadata and all available format URLs
    """
    url = request.args.get("url", "").strip()

    # --- Validate Input ---
    if not url:
        return jsonify({
            "error": "Missing 'url' parameter",
            "example": "/api/resolve?url=https://www.youtube.com/watch?v=dQw4w9WgXcQ"
        }), 400

    if not is_valid_youtube_url(url):
        return jsonify({
            "error": "Only YouTube URLs are supported",
            "allowed_hosts": list(ALLOWED_HOSTS)
        }), 403

    # --- Resolve ---
    try:
        result = resolve_formats(url)
        return jsonify({"success": True, "data": result})

    except PermissionError as e:
        return jsonify({"error": str(e)}), 403

    except ValueError as e:
        return jsonify({"error": str(e)}), 404

    except RuntimeError as e:
        logger.error(f"Resolution failed: {e}")
        return jsonify({"error": "Failed to resolve video", "detail": str(e)}), 502

    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        return jsonify({"error": "Internal server error"}), 500


@app.route('/api/formats', methods=['GET'])
@limiter.limit("30/minute")
def get_formats_summary():
    """
    Lightweight endpoint - returns only format summary, not all URLs.
    Useful for showing format picker without exposing all CDN URLs upfront.
    """
    url = request.args.get("url", "").strip()

    if not url or not is_valid_youtube_url(url):
        return jsonify({"error": "Invalid or missing YouTube URL"}), 400

    try:
        result = resolve_formats(url)

        # Strip actual CDN URLs from format list (return metadata only)
        formats_summary = [
            {k: v for k, v in fmt.items() if k != 'url'}
            for fmt in result['formats']
        ]

        return jsonify({
            "success": True,
            "data": {
                "title":        result['title'],
                "duration":     result['duration'],
                "thumbnail":    result['thumbnail'],
                "formats":      formats_summary,
                "format_count": result['format_count'],
            }
        })

    except Exception as e:
        logger.error(f"Format summary failed: {e}")
        return jsonify({"error": "Failed to get formats"}), 502


# ============================================================
# ERROR HANDLERS
# ============================================================
@app.errorhandler(429)
def rate_limited(e):
    return jsonify({
        "error": "Rate limit exceeded",
        "message": "Too many requests. Please wait before trying again."
    }), 429

@app.errorhandler(404)
def not_found(e):
    return jsonify({
        "error": "Endpoint not found",
        "available": ["/health", "/api/resolve", "/api/formats"]
    }), 404

@app.errorhandler(500)
def server_error(e):
    logger.error(f"500 error: {e}")
    return jsonify({"error": "Internal server error"}), 500

# ============================================================
# ENTRY POINT
# ============================================================
if __name__ == "__main__":
    print(f"Starting on port {Config.PORT}")
    app.run(
        host="0.0.0.0",
        port=Config.PORT,
        debug=False,
        threaded=True
    )
