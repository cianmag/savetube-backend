import os
import re
import logging
import requests
from flask import Flask, request, jsonify, Response, stream_with_context
from flask_cors import CORS
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from cachetools import TTLCache
import yt_dlp

# ---------------- CONFIG ----------------
video_cache = TTLCache(maxsize=200, ttl=7200)  # 2 hours TTL
PO_TOKEN = os.environ.get("YT_PO_TOKEN")

app = Flask(__name__)
CORS(app)

limiter = Limiter(
    key_func=get_remote_address,
    app=app,
    default_limits=["200 per day", "50 per hour"],
    storage_uri="memory://"
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------- HELPERS ----------------

def is_valid_youtube_url(url):
    pattern = r"(https?://)?(www\.)?(youtube\.com|youtu\.be)/.+"
    return re.match(pattern, url)

def sanitize_filename(name):
    return re.sub(r'[\\/*?:"<>|]', "", name)

def get_ydl_opts(format_query='best'):
    extractor_args = {
        'youtube': {
            'player_client': ['android', 'web']
        }
    }

    # Only add PO_TOKEN if exists
    if PO_TOKEN:
        extractor_args['youtube']['po_token'] = [f'web+{PO_TOKEN}']

    return {
        'quiet': True,
        'no_warnings': True,
        'format': format_query,
        'socket_timeout': 10,
        'retries': 2,
        'extractor_args': extractor_args,
        'http_headers': {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        }
    }

def fetch_video_data(url, format_id):
    with yt_dlp.YoutubeDL(get_ydl_opts(format_id)) as ydl:
        info = ydl.extract_info(url, download=False)

        return {
            "url": info.get("url"),
            "title": sanitize_filename(info.get("title", "video")),
            "ext": info.get("ext", "mp4"),
            "size": info.get("filesize") or info.get("filesize_approx")
        }

# ---------------- ROUTES ----------------

@app.route('/')
def health():
    return "🔥 10/10 Backend Running", 200

@app.route('/api/info')
@limiter.limit("30 per minute")
def info():
    url = request.args.get('url')

    if not url or not is_valid_youtube_url(url):
        return jsonify({'error': 'Valid YouTube URL required'}), 400

    try:
        with yt_dlp.YoutubeDL(get_ydl_opts()) as ydl:
            data = ydl.extract_info(url, download=False)

        formats = [
            {
                "format_id": f.get("format_id"),
                "quality": f.get("format_note", "Standard"),
                "ext": f.get("ext"),
                "height": f.get("height"),
                "size": f.get("filesize") or f.get("filesize_approx")
            }
            for f in data.get("formats", [])
            if f.get("vcodec") != "none" and f.get("acodec") != "none"
        ]

        return jsonify({
            "title": data.get("title"),
            "thumbnail": data.get("thumbnail"),
            "duration": data.get("duration_string"),
            "formats": formats[::-1]
        })

    except Exception as e:
        logger.error(f"INFO ERROR: {e}")
        return jsonify({'error': 'Failed to fetch video info'}), 500


@app.route('/api/proxy', methods=["GET", "HEAD"])
@limiter.limit("15 per minute")
def proxy():
    url = request.args.get("url")
    format_id = request.args.get("format_id", "best")

    if not url or not is_valid_youtube_url(url):
        return jsonify({"error": "Invalid URL"}), 400

    try:
        cache_key = f"{url}_{format_id}"

        if cache_key not in video_cache:
            video_cache[cache_key] = fetch_video_data(url, format_id)

        data = video_cache[cache_key]

        def stream():
            current_url = data['url']

            for attempt in range(2):
                try:
                    with requests.get(current_url, stream=True, timeout=10) as r:
                        r.raise_for_status()
                        for chunk in r.iter_content(chunk_size=262144):
                            if chunk:
                                yield chunk
                    return
                except Exception as e:
                    if attempt == 0:
                        logger.warning("🔄 Refreshing expired URL...")
                        video_cache.pop(cache_key, None)
                        new_data = fetch_video_data(url, format_id)
                        video_cache[cache_key] = new_data
                        current_url = new_data['url']
                    else:
                        raise e

        headers = {
            "Content-Disposition": f'attachment; filename="{data["title"]}.{data["ext"]}"',
            "Content-Type": f"video/{data['ext']}",
            "Connection": "keep-alive"
        }

        if data['size']:
            headers["Content-Length"] = str(data['size'])

        return Response(stream_with_context(stream()), headers=headers)

    except Exception as e:
        logger.error(f"PROXY ERROR: {e}")
        return jsonify({"error": "Streaming failed"}), 500


# ---------------- RUN ----------------
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 10000)))
