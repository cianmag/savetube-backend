#!/usr/bin/env bash
set -o errexit

echo "=== Installing Python dependencies ==="
pip install -r requirements.txt
pip install -U yt-dlp

echo "=== Installing FFmpeg ==="
apt-get update -qq && apt-get install -y -qq ffmpeg

echo "=== Verifying installations ==="
echo "yt-dlp: $(yt-dlp --version)"
echo "ffmpeg: $(ffmpeg -version | head -1)"
echo "Python: $(python --version)"

echo "=== Build complete ==="
