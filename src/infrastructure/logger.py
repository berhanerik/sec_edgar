import logging
import sys
from pathlib import Path

def setup_logging(level: str = 'INFO', log_file=None):
    fmt = '%(asctime)s  %(levelname)-8s  %(name)s  %(message)s'
    handlers = [logging.StreamHandler(sys.stdout)]
    if log_file:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file))
    logging.basicConfig(level=getattr(logging, level.upper()),
                        format=fmt, datefmt='%Y-%m-%d %H:%M:%S',
                        handlers=handlers, force=True)
    for noisy in ('google.auth','urllib3','fsspec','googleapiclient'):
        logging.getLogger(noisy).setLevel(logging.WARNING)
