"""Stand-in for the Python TinyBERT scoring service.

POST /score {"text": "..."} -> {"score": 0..1, "model_version": "..."}

The real service loads a fine-tuned TinyBERT model. This stub keeps the same
contract and scores with a few word weights, so X-Ray can run end to end.
"""
import json
import os
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

MODEL_VERSION = "stub-tinybert-0.1"
WEIGHTS = {
    "verify": 0.25, "account": 0.15, "suspended": 0.3, "urgent": 0.2, "winner": 0.35,
    "prize": 0.3, "otp": 0.25, "password": 0.3, "click": 0.15, "kyc": 0.3, "refund": 0.2,
}


def score(text: str) -> float:
    words = set(re.findall(r"[a-z]+", text.lower()))
    return min(1.0, sum(w for k, w in WEIGHTS.items() if k in words))


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path != "/score":
            self.send_error(404)
            return
        try:
            length = int(self.headers.get("Content-Length", 0))
            text = json.loads(self.rfile.read(length))["text"]
        except (ValueError, KeyError, TypeError):
            self.send_error(400)
            return
        body = json.dumps({"score": round(score(text), 4), "model_version": MODEL_VERSION}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", int(os.environ.get("PORT", "8000"))), Handler).serve_forever()
