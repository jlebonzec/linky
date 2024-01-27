#!/usr/bin/python3

import time

import db
import monitor
from config import config

DEFAULT_LOOP_DELAY = 60  # seconds

loop_delay = config.get('loop_delay_seconds', DEFAULT_LOOP_DELAY)

# Setup

# Try to connect to db server and create schema if not exists
db.verify_connection(config)

# Main loop
reader = monitor.LinkyReader(config['device'])
while True:
    start_time = time.monotonic()
    reader.process_data_group()
    time.sleep(loop_delay - ((time.monotonic() - start_time) % loop_delay))
