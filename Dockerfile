# StreamAdda v2 — Dockerfile
# Free-tier compatible: Render, Koyeb, Hugging Face Spaces

FROM python:3.12-slim

# Install FFmpeg in one layer
RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

RUN useradd -m -u 1000 streamadda
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY --chown=streamadda:streamadda . .

USER streamadda

ENV PORT=8000
ENV FLASK_ENV=production
ENV PYTHONUNBUFFERED=1
# Set this to your Render/Koyeb public URL so the keepalive pings the right address:
# ENV RENDER_EXTERNAL_URL=https://your-app.onrender.com

EXPOSE 8000

# --timeout 0     → never kill long-running SSE or large uploads
# --threads 16    → handle concurrent SSE connections
# --workers 1     → low RAM for free tier
CMD gunicorn app:app \
    --bind "0.0.0.0:${PORT}" \
    --workers 1 \
    --threads 16 \
    --timeout 0 \
    --keep-alive 65 \
    --log-level info \
    --access-logfile - \
    --error-logfile -
