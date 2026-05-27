"""
StreamAdda Clone — Flask Backend v2
Fixes:
  1. Anti-buffering FFmpeg flags (probesize, analyzeduration, fflags nobuffer)
  2. Built-in self-ping keepalive (no Render sleep)
  3. Video file upload from gallery (multipart/form-data → temp file → FFmpeg)
"""

import os
import sys
import uuid
import signal
import logging
import tempfile
import threading
import subprocess
import urllib.request
from datetime import datetime, timezone
from queue import Queue, Empty
from pathlib import Path
from flask import Flask, request, jsonify, render_template, Response, stream_with_context

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ─── App ──────────────────────────────────────────────────────────────────────
app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "streamadda-secret-change-me")
app.config["MAX_CONTENT_LENGTH"] = 4 * 1024 * 1024 * 1024  # 4 GB upload limit

# ─── Upload folder (temp dir on free cloud — wiped on restart) ────────────────
UPLOAD_DIR = Path(tempfile.gettempdir()) / "streamadda_uploads"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

# ─── State ────────────────────────────────────────────────────────────────────
streams: dict[str, dict] = {}
streams_lock = threading.Lock()

RTMP_BASES = {
    "youtube":  "rtmp://a.rtmp.youtube.com/live2/",
    "facebook": "rtmps://live-api-s.facebook.com:443/rtmp/",
    "custom":   "",
}

MAX_LOG_LINES = 300
LOG_QUEUE_MAX = 500
ALLOWED_EXTENSIONS = {".mp4", ".mkv", ".avi", ".mov", ".flv", ".webm", ".ts"}


# ─── Helpers ──────────────────────────────────────────────────────────────────

def find_ffmpeg() -> str:
    env_path = os.environ.get("FFMPEG_PATH")
    if env_path and os.path.isfile(env_path):
        return env_path
    for candidate in ["ffmpeg", "/usr/bin/ffmpeg", "/usr/local/bin/ffmpeg"]:
        try:
            subprocess.run(
                [candidate, "-version"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=True,
            )
            return candidate
        except (FileNotFoundError, subprocess.CalledProcessError):
            continue
    raise RuntimeError("FFmpeg not found. Set FFMPEG_PATH env var or install ffmpeg.")


def build_ffmpeg_command(
    ffmpeg_bin: str,
    video_path: str,          # local file path OR direct URL
    rtmp_url: str,
    video_bitrate: str = "2500k",
    audio_bitrate: str = "128k",
    resolution: str = "1280x720",
    loop: bool = True,
) -> list[str]:
    """
    Anti-buffering FFmpeg command for smooth 24/7 RTMP streaming.
    Key fixes vs v1:
      - fflags +genpts+discardcorrupt  → fixes PTS gaps that cause buffering
      - probesize 10M / analyzeduration 5M → faster start, less pre-buffering
      - -thread_queue_size 512 → prevents input queue starvation
      - -vsync cfr → constant frame rate output (RTMP requirement)
      - bufsize = 2× bitrate (not 1×) → smoother encoder buffer
      - -shortest removed — never truncate on loop
    """
    width, height = resolution.split("x") if "x" in resolution else ("1280", "720")
    vbr_int = int(video_bitrate.replace("k", ""))

    cmd = [ffmpeg_bin]

    # ── Global flags ──────────────────────────────────────────────────────────
    cmd += [
        "-loglevel", "warning",          # suppress verbose info, keep warnings
        "-stats",                         # show frame/fps/bitrate line
    ]

    # ── Input flags (BEFORE -i) ───────────────────────────────────────────────
    # For URL inputs: reconnect on drop
    is_url = video_path.startswith("http://") or video_path.startswith("https://")
    if is_url:
        cmd += [
            "-reconnect",          "1",
            "-reconnect_at_eof",   "1",
            "-reconnect_streamed", "1",
            "-reconnect_delay_max","30",
        ]

    # Reduce probe time → stream starts faster, less initial buffering
    cmd += [
        "-probesize",        "10M",
        "-analyzeduration",  "5000000",   # 5 s max analysis
        "-fflags",           "+genpts+discardcorrupt",  # fix broken timestamps
        "-thread_queue_size","512",        # prevent input starvation
    ]

    if loop:
        cmd += ["-stream_loop", "-1"]

    # Read at native frame rate — CRITICAL for RTMP; without this FFmpeg
    # reads the file as fast as possible and overflows the RTMP buffer
    cmd += ["-re"]

    cmd += ["-i", video_path]

    # ── Video encoding ────────────────────────────────────────────────────────
    cmd += [
        "-vf", (
            f"scale={width}:{height}:"
            f"force_original_aspect_ratio=decrease,"
            f"pad={width}:{height}:(ow-iw)/2:(oh-ih)/2:black,"
            f"setsar=1"                    # fix SAR mismatch that causes decoder hiccups
        ),
        "-c:v",       "libx264",
        "-preset",    "veryfast",          # lowest CPU — essential for free tiers
        "-tune",      "zerolatency",       # minimise encoder latency
        "-profile:v", "high",             # YouTube/FB expect High profile
        "-level:v",   "4.1",
        "-b:v",       video_bitrate,
        "-maxrate",   video_bitrate,
        "-bufsize",   f"{vbr_int * 2}k",  # 2× bitrate → smoother encoder buffer
        "-g",         "60",               # keyframe every 2 s at 30 fps
        "-keyint_min","60",
        "-sc_threshold","0",              # disable scene-cut keyframes (keeps GOP fixed)
        "-vsync",     "cfr",              # constant frame rate → no timestamp jumps
        "-r",         "30",               # enforce 30 fps output
        "-pix_fmt",   "yuv420p",          # compatibility with all decoders
    ]

    # ── Audio encoding ────────────────────────────────────────────────────────
    cmd += [
        "-c:a",  "aac",
        "-b:a",  audio_bitrate,
        "-ar",   "44100",
        "-ac",   "2",
        "-af",   "aresample=async=1:min_hard_comp=0.100000:first_pts=0",  # fix audio drift
    ]

    # ── Output ────────────────────────────────────────────────────────────────
    cmd += [
        "-f",         "flv",
        "-flvflags",  "no_duration_filesize",
        rtmp_url,
    ]

    return cmd


def _read_ffmpeg_stderr(slot_id: str, process: subprocess.Popen):
    """Read FFmpeg stderr in a background thread; push to queue for SSE."""
    with streams_lock:
        slot = streams.get(slot_id)
    if not slot:
        return

    log_queue: Queue = slot["log_queue"]
    log_lines: list  = slot["log_lines"]

    try:
        for raw in iter(process.stderr.readline, b""):
            line = raw.decode("utf-8", errors="replace").rstrip()
            if not line:
                continue
            ts   = datetime.now(timezone.utc).strftime("%H:%M:%S")
            line = f"[{ts}] {line}"
            log_lines.append(line)
            if len(log_lines) > MAX_LOG_LINES:
                log_lines.pop(0)
            log_queue.put(line)
            while log_queue.qsize() > LOG_QUEUE_MAX:
                try:
                    log_queue.get_nowait()
                except Empty:
                    break
    except Exception as exc:
        logger.warning("Log reader error slot=%s: %s", slot_id, exc)
    finally:
        with streams_lock:
            s = streams.get(slot_id)
            if s and s.get("status") == "online":
                s["status"] = "offline"
                s["ended_at"] = datetime.now(timezone.utc).isoformat()
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
        log_queue.put(f"[{ts}] ── FFmpeg process ended ──")
        logger.info("Slot %s ended.", slot_id)


# ─── Self-ping keepalive (prevents Render free tier sleep) ───────────────────

_keepalive_started = False

def _start_keepalive():
    """Ping own /api/health every 4 minutes so Render never sleeps."""
    global _keepalive_started
    if _keepalive_started:
        return
    _keepalive_started = True

    def _ping():
        import time
        time.sleep(30)  # wait for server to fully start
        while True:
            try:
                own_url = os.environ.get("RENDER_EXTERNAL_URL") or \
                          os.environ.get("APP_URL") or \
                          f"http://localhost:{os.environ.get('PORT', 5000)}"
                url = own_url.rstrip("/") + "/api/health"
                urllib.request.urlopen(url, timeout=10)
                logger.info("Keepalive ping OK → %s", url)
            except Exception as e:
                logger.warning("Keepalive ping failed: %s", e)
            time.sleep(240)  # 4 minutes

    t = threading.Thread(target=_ping, daemon=True, name="keepalive")
    t.start()
    logger.info("Self-ping keepalive started.")


# ─── Routes ───────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/streams", methods=["GET"])
def list_streams():
    with streams_lock:
        result = {}
        for sid, slot in streams.items():
            uptime = None
            if slot["status"] == "online" and slot.get("started_at"):
                uptime = int((datetime.now(timezone.utc) - slot["started_at"]).total_seconds())
            result[sid] = {
                "id":            sid,
                "name":          slot["name"],
                "status":        slot["status"],
                "uptime":        uptime,
                "rtmp_url":      slot.get("rtmp_url", ""),
                "resolution":    slot.get("resolution", ""),
                "video_bitrate": slot.get("video_bitrate", ""),
                "audio_bitrate": slot.get("audio_bitrate", ""),
                "loop":          slot.get("loop", True),
                "filename":      slot.get("filename", ""),
                "started_at":    slot["started_at"].isoformat() if slot.get("started_at") else None,
            }
    return jsonify(result)


@app.route("/api/upload", methods=["POST"])
def upload_video():
    """
    Accept a video file upload from the browser gallery.
    Saves to UPLOAD_DIR (tmpfs on free cloud), returns the server-side path.
    """
    if "video" not in request.files:
        return jsonify({"error": "No file field named 'video'"}), 400

    f = request.files["video"]
    if not f.filename:
        return jsonify({"error": "Empty filename"}), 400

    suffix = Path(f.filename).suffix.lower()
    if suffix not in ALLOWED_EXTENSIONS:
        return jsonify({"error": f"File type {suffix} not allowed. Use: {', '.join(ALLOWED_EXTENSIONS)}"}), 400

    # Unique filename to avoid collisions
    uid      = uuid.uuid4().hex[:10]
    safe_ext = suffix if suffix else ".mp4"
    dest     = UPLOAD_DIR / f"{uid}{safe_ext}"

    try:
        f.save(str(dest))
    except Exception as exc:
        return jsonify({"error": f"Save failed: {exc}"}), 500

    size_mb = dest.stat().st_size / (1024 * 1024)
    logger.info("Uploaded: %s (%.1f MB) → %s", f.filename, size_mb, dest)

    return jsonify({
        "path":     str(dest),
        "filename": f.filename,
        "size_mb":  round(size_mb, 1),
    }), 200


@app.route("/api/streams", methods=["POST"])
def start_stream():
    """
    Start an FFmpeg stream.
    Accepts JSON: { video_path, name, platform, stream_key, ... }
    video_path must be a server-side path returned by /api/upload,
    OR a direct HTTP(S) URL.
    """
    data = request.get_json(force=True)

    name         = (data.get("name")          or "Unnamed Stream").strip()
    video_path   = (data.get("video_path")    or "").strip()
    stream_key   = (data.get("stream_key")    or "").strip()
    platform     = (data.get("platform")      or "youtube").lower()
    custom_rtmp  = (data.get("custom_rtmp")   or "").strip()
    video_bitrate= (data.get("video_bitrate") or "2500k").strip()
    audio_bitrate= (data.get("audio_bitrate") or "128k").strip()
    resolution   = (data.get("resolution")    or "1280x720").strip()
    loop         = bool(data.get("loop", True))
    filename     = (data.get("filename")      or Path(video_path).name)

    if not video_path:
        return jsonify({"error": "video_path is required (upload first or provide URL)"}), 400

    # Validate local file exists (if not a URL)
    is_url = video_path.startswith("http://") or video_path.startswith("https://")
    if not is_url and not Path(video_path).is_file():
        return jsonify({"error": f"Video file not found on server: {video_path}"}), 400

    if not stream_key and not custom_rtmp:
        return jsonify({"error": "stream_key or custom_rtmp is required"}), 400

    # Build RTMP URL
    rtmp_url = custom_rtmp if custom_rtmp else (RTMP_BASES.get(platform, RTMP_BASES["youtube"]) + stream_key)

    try:
        ffmpeg_bin = find_ffmpeg()
    except RuntimeError as exc:
        return jsonify({"error": str(exc)}), 500

    cmd = build_ffmpeg_command(
        ffmpeg_bin, video_path, rtmp_url,
        video_bitrate=video_bitrate,
        audio_bitrate=audio_bitrate,
        resolution=resolution,
        loop=loop,
    )

    logger.info("Starting '%s' → %s", name, rtmp_url)
    logger.info("CMD: %s", " ".join(cmd))

    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            stdin=subprocess.DEVNULL,
            preexec_fn=os.setsid if sys.platform != "win32" else None,
        )
    except Exception as exc:
        return jsonify({"error": f"Failed to start FFmpeg: {exc}"}), 500

    slot_id   = str(uuid.uuid4())
    log_queue = Queue(maxsize=LOG_QUEUE_MAX)
    log_lines: list = []

    slot = {
        "id":            slot_id,
        "name":          name,
        "status":        "online",
        "process":       process,
        "started_at":    datetime.now(timezone.utc),
        "ended_at":      None,
        "rtmp_url":      rtmp_url,
        "video_path":    video_path,
        "filename":      filename,
        "video_bitrate": video_bitrate,
        "audio_bitrate": audio_bitrate,
        "resolution":    resolution,
        "loop":          loop,
        "log_queue":     log_queue,
        "log_lines":     log_lines,
    }

    with streams_lock:
        streams[slot_id] = slot

    reader = threading.Thread(
        target=_read_ffmpeg_stderr,
        args=(slot_id, process),
        daemon=True,
        name=f"log-{slot_id[:8]}",
    )
    reader.start()

    return jsonify({
        "id":      slot_id,
        "name":    name,
        "status":  "online",
        "rtmp_url": rtmp_url,
        "message": "Stream started.",
    }), 201


@app.route("/api/streams/<slot_id>/stop", methods=["POST"])
def stop_stream(slot_id: str):
    with streams_lock:
        slot = streams.get(slot_id)
        if not slot:
            return jsonify({"error": "Not found"}), 404
        if slot["status"] != "online":
            return jsonify({"error": "Not running"}), 400

    process: subprocess.Popen = slot["process"]
    try:
        if sys.platform != "win32":
            os.killpg(os.getpgid(process.pid), signal.SIGTERM)
        else:
            process.terminate()
        try:
            process.wait(timeout=8)
        except subprocess.TimeoutExpired:
            if sys.platform != "win32":
                os.killpg(os.getpgid(process.pid), signal.SIGKILL)
            else:
                process.kill()
    except ProcessLookupError:
        pass
    except Exception as exc:
        logger.warning("Stop error slot=%s: %s", slot_id, exc)

    with streams_lock:
        slot["status"]   = "offline"
        slot["ended_at"] = datetime.now(timezone.utc).isoformat()

    return jsonify({"id": slot_id, "status": "offline"})


@app.route("/api/streams/<slot_id>", methods=["DELETE"])
def delete_stream(slot_id: str):
    with streams_lock:
        slot = streams.get(slot_id)
        if not slot:
            return jsonify({"error": "Not found"}), 404

    if slot.get("status") == "online":
        stop_stream(slot_id)

    with streams_lock:
        streams.pop(slot_id, None)

    return jsonify({"deleted": True})


@app.route("/api/streams/<slot_id>/events")
def stream_events(slot_id: str):
    """SSE: real-time FFmpeg log lines."""
    with streams_lock:
        slot = streams.get(slot_id)
        if not slot:
            return Response('data: {"error":"not found"}\n\n',
                            mimetype="text/event-stream", status=404)

    log_queue = slot["log_queue"]

    def generate():
        yield "retry: 3000\n\n"
        with streams_lock:
            history = list(slot.get("log_lines", []))
        for line in history:
            yield f"data: {line}\n\n"
        while True:
            with streams_lock:
                alive = slot_id in streams
            if not alive:
                break
            try:
                line = log_queue.get(timeout=1.0)
                yield f"data: {line}\n\n"
            except Empty:
                yield ": ping\n\n"

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control":    "no-cache",
            "X-Accel-Buffering":"no",
            "Connection":       "keep-alive",
        },
    )


@app.route("/api/health")
def health():
    try:
        ffmpeg_bin = find_ffmpeg()
        ffmpeg_ok  = True
    except RuntimeError:
        ffmpeg_bin = None
        ffmpeg_ok  = False

    with streams_lock:
        active = sum(1 for s in streams.values() if s["status"] == "online")

    return jsonify({
        "status":         "ok",
        "ffmpeg":         ffmpeg_ok,
        "ffmpeg_path":    ffmpeg_bin,
        "active_streams": active,
        "total_slots":    len(streams),
        "timestamp":      datetime.now(timezone.utc).isoformat(),
    })


# ─── Startup ──────────────────────────────────────────────────────────────────
with app.app_context():
    _start_keepalive()

if __name__ == "__main__":
    port  = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_DEBUG", "0") == "1"
    logger.info("StreamAdda v2 on port %d", port)
    app.run(host="0.0.0.0", port=port, debug=debug, threaded=True)
