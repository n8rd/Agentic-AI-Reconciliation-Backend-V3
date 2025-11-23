import logging
import re

class RedactingFormatter(logging.Formatter):
    PATTERNS = [
        re.compile(r"sk-[A-Za-z0-9]+"),
        re.compile(r"AIza[0-9A-Za-z_-]+"),
    ]

    def format(self, record):
        msg = super().format(record)
        for p in self.PATTERNS:
            msg = p.sub("[REDACTED]", msg)
        record.msg = msg
        return msg

logger = logging.getLogger("recon")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(RedactingFormatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)
logger.setLevel(logging.INFO)
