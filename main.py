from flask import Flask, request, jsonify, Response
from flask_cors import CORS
import yt_dlp

app = Flask(__name__)
CORS(app)

@app.route('/api/info')
from flask import Response

@app.route('/download')
def download():
    url = request.args.get('url')

    if not url:
        return "No URL provided", 400

    try:
        ydl_opts = {
    'format': 'bestvideo+bestaudio/best',
    'quiet': True,
    'noplaylist': True,
    'cookiefile': 'cookies.txt',
    'extractor_args': {
        'youtube': {
            'player_client': ['android']
        }
    }
}
        }

        def generate():
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=False)
                video_url = info['url']

                import requests
                r = requests.get(video_url, stream=True)

                for chunk in r.iter_content(chunk_size=1024):
                    if chunk:
                        yield chunk

        return Response(generate(), content_type="video/mp4")

    except Exception as e:
        return jsonify({'error': str(e)}), 500
        def get_info():
    url = request.args.get('url')
    if not url:
        return jsonify({'error': 'No URL provided'}), 400
    try:
        ydl_opts = {'quiet': True, 'no_warnings': True}
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            formats = []
            for f in info.get('formats', []):
                if f.get('url'):
                    formats.append({
                        'quality': f.get('format_note', 'unknown'),
                        'format': f.get('ext', 'mp4'),
                        'size': f.get('filesize', 0),
                        'url': f.get('url')
                    })
            return jsonify({
                'title': info.get('title'),
                'thumbnail': info.get('thumbnail'),
                'channel': info.get('uploader'),
                'duration': info.get('duration_string'),
                'formats': formats
            })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    import os
port = int(os.environ.get('PORT', 5000))
app.run(host='0.0.0.0', port=port)
