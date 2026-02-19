# core/logging.py
import json, time

class JsonlLogger:
    def __init__(self, path):
        self.path = path

    def event(self, kind: str, payload: dict):
        rec = {"t": time.time(), "kind": kind, **payload}
        with open(self.path, "a") as f:
            f.write(json.dumps(rec) + "\n")
