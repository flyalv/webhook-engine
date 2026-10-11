
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import random

MODE = os.getenv("RECEIVER_MODE", "random")
FAILURE_PERCENT = int(os.getenv("RECEIVER_FAILURE_PERCENT", "25"))
DELAY_SECONDS = float(os.getenv("RECEIVER_DELAY_SECONDS", "15"))

if not 0 <= FAILURE_PERCENT <= 100:
    raise ValueError("RECEIVER_FAILURE_PERCENT must be between 0 and 100")


class WebhookHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path != "/webhook":
            self.send_error(404)
            return

        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)

        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            self.send_error(400, "Invalid JSON")
            return

        print("\n=== Webhook received ===", flush=True)
        print(f"Path: {self.path}", flush=True)
        print(f"X-Event-ID: {self.headers.get('X-Event-ID')}", flush=True)
        print(f"Content-Type: {self.headers.get('Content-Type')}", flush=True)
        print(f"Payload: {json.dumps(payload, ensure_ascii=False)}", flush=True)

        if MODE == "always-fail":
            self.respond_failure()
            return

        if MODE == "always-ok":
            self.respond_success()
            return

        if MODE == "slow":
            import time
            time.sleep(DELAY_SECONDS)
            self.respond_success()
            return

        if MODE == "random":
            if random.random() < FAILURE_PERCENT / 100:
                self.respond_failure()
            else:
                self.respond_success()
            return

        self.send_error(500, "Unknown receiver mode")

    def respond_failure(self):
        print("[RESULT] Simulated HTTP 500", flush=True)
        self.send_response(500)
        self.end_headers()
        self.wfile.write(b"simulated temporary failure")

    def respond_success(self):
        print("[RESULT] HTTP 204", flush=True)
        self.send_response(204)
        self.end_headers()

    def log_message(self, format, *args):
        print(f"[HTTP] {format % args}", flush=True)


server = ThreadingHTTPServer(("0.0.0.0", 8081), WebhookHandler)
print(
    f"Webhook receiver listening on :8081; "
    f"mode={MODE}, failure_percent={FAILURE_PERCENT}",
    flush=True,
)
server.serve_forever()
