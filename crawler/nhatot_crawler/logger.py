import datetime
import sys

VN_TZ = datetime.timezone(datetime.timedelta(hours=7))

def _now():
    return datetime.datetime.now(VN_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")

class Logger:
    def __init__(self, out=sys.stdout):
        self.out = out

    def info(self, msg, *args):
        line = f"[{_now()}] {msg.format(*args) if args else msg}"
        self.out.write(line + "\n")
        self.out.flush()

    def error(self, msg, *args):
        line = f"[{_now()}] ERROR: {msg.format(*args) if args else msg}"
        self.out.write(line + "\n")
        self.out.flush()
        
    def warning(self, msg, *args):
        line = f"[{_now()}] WARNING: {msg.format(*args) if args else msg}"
        self.out.write(line + "\n")
        self.out.flush()

log = Logger()