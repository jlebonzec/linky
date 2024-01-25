#!/usr/bin/python3

import linky

# ----------------------------- #
# Setup                         #
# ----------------------------- #

# Trying to connect to db server and creating schema if not exists
linky.test_db_connection(linky.config)


# ----------------------------- #
# Main loop                     #
# ----------------------------- #
while True:
    reader = linky.LinkyReader(linky.config['device'])
    reader.read_data_group()
    # time.sleep(60)  # FIXME: subtract time taken by the loop
