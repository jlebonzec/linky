from contextlib import contextmanager

import serial

from logger import log

BAUD_RATE = 1200


def setup_serial(dev: str = "/dev/ttyS0") -> serial.Serial:
    """
    Build the serial connection object.

    param dev: Linux device of the connector (such as "/dev/ttyS0")
    """
    terminal = serial.Serial()
    terminal.port = dev
    terminal.baudrate = BAUD_RATE
    terminal.stopbits = serial.STOPBITS_ONE
    terminal.bytesize = serial.SEVENBITS
    return terminal


@contextmanager
def open_terminal(terminal: serial.Serial):
    """
    Context manager to use the serial connection

    :param terminal: The serial connection object
    """
    terminal.open()
    try:
        yield terminal
    except Exception:
        log.critical("Something went wrong while reading from serial:", exc_info=True)
        raise
    finally:
        terminal.close()
