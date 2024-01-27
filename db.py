#!/usr/bin/env python3
import sys
from contextlib import contextmanager

import MySQLdb

from logger import log


def verify_connection(config: dict):
    """
    Verify DB connection. Will create the schema if missing

    :param config: The configuration dictionary
    """
    # testing connection
    required_dbs = ['stream', 'dailies']

    with connect(config) as (db, cr):
        count = cr.execute(
            f"SELECT * "
            f"FROM information_schema.tables "
            f"WHERE table_schema = '{config['database']['name']}' AND table_name IN ({','.join(['%s' for s in required_dbs])})",
            tuple(required_dbs)
        )

    if count != len(required_dbs):
        log.info("Database schema is not there, creating it...")
        try:
            with open('schema.sql', 'r') as f:
                with connect(config) as (db, cr):
                    cr.execute(f.read())
                    db.commit()
        except MySQLdb.OperationalError:
            log.critical('Something went wrong while trying to create database schema:', exc_info=True)
            print('Something went wrong while trying to create database schema. See logs for more info.',
                  file=sys.stderr)
            raise SystemExit(4)
        else:
            log.info("Database schema created successfully")


@contextmanager
def connect(config: dict) -> tuple[MySQLdb.Connection, object]:
    """
    Context manager to connect to the database

    :param config: The configuration dictionary
    """
    try:
        db = MySQLdb.connect(config['database']['server'],
                             config['database']['user'],
                             config['database']['password'],
                             config['database']['name'])
        cr = db.cursor()
    except MySQLdb.Error:
        log.critical('Something went wrong while connecting to database server:', exc_info=True)
        print('Something went wrong while connecting to database server. See logs for more info.', file=sys.stderr)
        raise SystemExit(5)
    try:
        yield db, cr
    finally:
        db.close()
