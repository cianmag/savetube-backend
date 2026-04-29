from flask import Flask, request, jsonify
from flask_cors import CORS
import yt_dlp

app = Flask(__name__)
CORS(app)

@app.route('/api/info')
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
    app.run(host='0.0.0.0', port=5000)
