# Recreating the web_service since execution state was reset

import os
from aiohttp import web
from src.shared import feed_message, feed_message_lock
from threading import Lock


app = web.Application()
corsOrigin = "*"
corsHeaders = "*"

# === Serve GTFS Static zip ===
async def handle_gtfs_zip(request):
    zip_path = "../out/gtfs.zip"
    if not os.path.exists(zip_path):
        return web.Response(status=404, text="GTFS ZIP not found.")
    response = web.FileResponse(zip_path)
    response.headers["Content-Disposition"] = "attachment; filename=gtfs.zip"
    response.headers["Access-Control-Allow-Origin"] = corsOrigin
    return response


# === Serve GTFS Realtime Feed ===
async def handle_gtfs_realtime(request):
    with feed_message_lock:
        binary = feed_message.SerializeToString()
    response = web.Response(body=binary, content_type="application/x-protobuf")
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    response.headers["Access-Control-Allow-Origin"] = corsOrigin
    return response


# === Serve GTFS Version Info ===
async def handle_gtfs_version(request):
    version_file = "../out/feed_info.txt"
    if not os.path.exists(version_file):
        return web.json_response({"error": "version file not found"}, status=404)

    with open(version_file, "r") as f:
        version = f.read().strip()
    response = web.json_response({"version": version})
    response.headers["Cache-Control"] = "no-store"


# === Enable CORS support for browser restrictions ===
async def handle_options(request):
    return web.Response(headers={
        "Access-Control-Allow-Origin": corsOrigin,
        "Access-Control-Allow-Methods": "GET,OPTIONS",
        "Access-Control-Allow-Headers": corsHeaders,
    })



# === Routes ===
app.router.add_get("/gtfs.zip", handle_gtfs_zip)
app.router.add_get("/gtfs-rt.proto", handle_gtfs_realtime)
app.router.add_get("/gtfs-version", handle_gtfs_version)
app.router.add_options("/{tail:.*}", handle_options)

# === Run Server ===
def run_web_service(host="0.0.0.0", port=59966):
    print(f"[web_service] Serving on http://{host}:{port}")
    web.run_app(app, host=host, port=port)
