from collections import UserDict
from copy import deepcopy

import linky
from config import config
from logger import log
from db import connect


class LinkyMetrics(UserDict):
    """ A dictionary to store and manipulate Linky metrics """
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
        """ Reset the data dict to default values"""
        self.data = deepcopy(self.DEFAULT_DATA)

    def flush(self):
        """ Write to database and reset the data dict """
        self.write_to_db()
        self.reset()

    def write_to_db(self):
        """ Write the data to the database """
        if not all([self.data.get(k) for k in self.REQUIRED_DATA]):
            log.warning("Not enough data to write to db")
            return

        with connect(config) as (db, cr):
            log.debug("Inserting stream record")
            cr.execute(
                f"INSERT INTO stream ({','.join(self.data.keys())}) VALUES ({','.join(['%s' for s in self.data.values()])})",
                tuple(self.data.values())
            )
            db.commit()


class LinkyReader(object):
    """ A class to read data from Linky """

    def __init__(self, device='/dev/ttyS0'):
        self.terminal = linky.setup_serial(device)
        self.data = LinkyMetrics()

    def get_line(self) -> str:
        """ Read a line from the serial connection """
        line = self.terminal.readline().decode('ascii').strip()
        log.debug(f"Current line: '{line}'")
        return line

    def parse_line(self, line: str) -> tuple[str, str]:
        """ Parse a line already read from the serial connection """
        words = line.split()
        if len(words) < 2:
            log.debug(f"Line '{line}' is not valid")
            return "", ""

        code, value = words[:2]
        log.debug(f"Parsed: {code}={value}")
        return code, value

    def get_parsed_line(self) -> tuple[str, str]:
        """ Read a line from the serial connection and parse it """
        return self.parse_line(self.get_line())

    def process_data_group(self):
        """
        Read and process a full data group from the serial connection.

        Will read until the last word (MOTDETAT) is reached
        Once the full group is read, it will flush the data to the database
        """
        with linky.open_terminal(self.terminal):
            code, value = self.get_parsed_line()

            while code.upper() != 'ADCO':  # first word
                code, value = self.get_parsed_line()

            metrics = LinkyMetrics({code: value})
            while code.upper() != 'MOTDETAT':  # last word
                code, value = self.get_parsed_line()
                metrics[code] = value

            metrics.flush()
