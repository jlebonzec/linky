#!/usr/bin/python3
from collections import UserDict
from contextlib import contextmanager
from copy import deepcopy

# stdlib
import MySQLdb
import logging
import logging.handlers
import serial
import sys
# 3rd party
import yaml

LOG_PATH = './logs/linky.log'
LOG_MAX_BYTES = 1_000_000
LOG_BACKUP_COUNT = 5

BAUD_RATE = 1200


def init_log_system():
    """
    Initializes logger system
    """
    logger = logging.getLogger('linky')
    logger.setLevel(logging.DEBUG)  # Define minimum severity here
    handler = logging.handlers.RotatingFileHandler(LOG_PATH, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT)
    formatter = logging.Formatter('[%(asctime)s][%(module)s][%(levelname)s] %(message)s', '%Y-%m-%d %H:%M:%S %z')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def load_config():
    """
    Loads config file
    """
    try:
        with open('config.yml', 'r') as f:
            config = yaml.safe_load(f)
    except IOError:
        log.critical('Problem reading config file: config.yml', exc_info=True)
        raise SystemExit(1)
    except Exception:
        log.critical('Something went wrong while opening config file:', exc_info=True)
        raise SystemExit(3)
    else:
        return config


def setup_serial(dev):
    """
    Builds the serial connection object.

    Args:
        dev (str): Linux device of the connector (like "/dev/ttyS0")
    """
    terminal = serial.Serial()
    terminal.port = dev
    terminal.baudrate = BAUD_RATE
    terminal.stopbits = serial.STOPBITS_ONE
    terminal.bytesize = serial.SEVENBITS
    return terminal


def test_db_connection(config):
    """
    Tests DB connection, and also creates the schema if missing

    Args:
        config (dict): The configuration dictionary
    """
    # testing connection
    required_dbs = ['stream', 'dailies']

    with db_manager(config) as (db, cr):
        count = cr.execute(
            f"SELECT * "
            f"FROM information_schema.tables "
            f"WHERE table_schema = '{config['database']['name']}' AND table_name IN ({','.join(required_dbs)})"
        )
        if count != len(required_dbs):
            log.info("Database schema is not there, creating it...")
            try:
                with open('schema.sql', 'r') as f:
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
def db_manager(config):
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


def insert_stream(config, stream):
    """
    Insert a record in the stream table

    Args:
        config (dict): Loaded config from yaml file
        stream (dict): MySQLdb database object
    """
    with db_manager(config) as (db, cr):
        log.debug("Inserting stream record")
        cr.execute(f"INSERT INTO streams ({','.join(stream.keys())}) VALUES ({','.join(stream.values())})")
        db.commit()


@contextmanager
def open_terminal(terminal):
    terminal.open()
    try:
        yield terminal
    except Exception:
        log.critical("Something went wrong while reading from serial:", exc_info=True)
        raise
    finally:
        terminal.close()


class LinkyMetrics(UserDict):

    DEFAULT_DATA = {
        'ADCO': None,  # Adresse du compteur
        'OPTARIF': None,  # Option tarifaire choisie
        'ISOUSC': None,  # Intensité souscrite (A)
        'BASE': None,  # Index option Base (Wh)
        'HCHC': None,  # Index option HC Heures Creuses (Wh)
        'HCHP': None,  # Index option HC Heures Pleines (Wh)
        'EJPHN': None,  # Index option EJP Heures Normales (Wh)
        'EJPHPM': None,  # Index option EJP Heures de Pointe Mobile (Wh)
        'BBRHCJB': None,  # Index option Tempo jours Bleus Heures Creuses (Wh)
        'BBRHPJB': None,  # Index option Tempo jours Bleus Heures Pleines (Wh)
        'BBRHCJW': None,  # Index option Tempo jours Blancs Heures Creuses (Wh)
        'BBRHPJW': None,  # Index option Tempo jours Blancs Heures Pleines (Wh)
        'BBRHCJR': None,  # Index option Tempo jours Rouges Heures Creuses (Wh)
        'BBRHPJR': None,  # Index option Tempo jours Rouges Heures Pleines (Wh)
        'PEJP': None,  # Préavis Début EJP (min)
        'PTEC': None,  # Période Tarifaire en cours
        'DEMAIN': None,  # Couleur du lendemain
        'IINST': None,  # Intensité Instantanée (A)
        'ADPS': None,  # Avertissement de Dépassement De Puissance Souscrite (A)
        'IMAX': None,  # Intensité maximale appelée (A)
        'PAPP': None,  # Puissance apparente (VA)
        'HHPHC': None,  # Horaire Heures Pleines Heures Creuses
        'MOTDETAT': None,  # Mot d'état du compteur
    }

    REQUIRED_DATA = ['ADCO', 'OPTARIF', 'ISOUSC', 'IINST', 'IMAX', 'PAPP', 'MOTDETAT']

    def reset(self):
        self.data = deepcopy(self.DEFAULT_DATA)

    def flush(self):
        self.write_to_db()
        self.reset()

    def write_to_db(self):
        if not all([self.data.get(k) for k in self.REQUIRED_DATA]):
            log.warning("Not enough data to write to db")
            return

        with db_manager(config) as (db, cr):
            log.debug("Inserting stream record")
            cr.execute(f"INSERT INTO streams ({','.join(self.data.keys())}) VALUES ({','.join(self.data.values())})")
            db.commit()


class LinkyReader(object):

    def __init__(self, device='/dev/ttyS0'):
        self.terminal = setup_serial(device)
        self.data = LinkyMetrics()
        self.log = logging.getLogger('linky')

    def get_line(self):
        line = self.terminal.readline().decode('ascii').strip()
        self.log.debug(f"Current line: '{line}'")
        return line

    def parse_line(self, line):
        code, value, _ = line.split(' ', 2)
        self.log.debug(f"Parsed: {code}={value}")
        return code, value

    def get_parsed_line(self):
        return self.parse_line(self.get_line())

    def read_data_group(self):
        with open_terminal(self.terminal):
            code, value = self.get_parsed_line()

            while code.upper() != 'ADCO':  # first word
                code, value = self.get_parsed_line()

            lv = LinkyMetrics({code: value})
            while code.upper() != 'MOTDETAT':  # last word
                code, value = self.get_parsed_line()
                lv[code] = value

            lv.flush()


# Initializing log system
log = init_log_system()

log.debug('Loading config...')
config = load_config()
log.debug(f'Config loaded! Values: {config}')
