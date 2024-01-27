import yaml

from logger import log

CONFIG_FILE = 'config.yml'


def load_config() -> dict:
    """ Load and return the configuration file """
    try:
        with open(CONFIG_FILE, 'r') as f:
            conf = yaml.safe_load(f)
    except IOError:
        log.critical('Problem reading config file: %s', CONFIG_FILE, exc_info=True)
        raise SystemExit(1)
    except Exception:
        log.critical('Something went wrong while opening config file:', exc_info=True)
        raise SystemExit(3)
    else:
        return conf


log.debug('Loading config...')
config = load_config()
log.debug(f'Config loaded! Values: {config}')
