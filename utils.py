import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from configparser import ConfigParser

POKEMON_DATA_INGEST = 'pokemon-data-ingest'


def get_config_for_pokeingest():
    return get_config(filename='config.ini')


def get_config(filename):
    parser = ConfigParser()
    config_path = Path(__file__).resolve().parent / filename
    print(f"Looking for config at: {config_path}")
    if not config_path.exists():
        print("Config not found.")
        return {}
    parser.read(config_path)
    config_dict = {s: dict(parser.items(s)) for s in parser.sections()}
    return config_dict


def get_log_level(level):
    if level:
        level = str(level).upper()
    return {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL
    }.get(level, logging.INFO)


def ensure_path_exists(path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)


def init_log(c):
    project_root = Path(__file__).resolve().parent
    log_path = project_root / c['log']['log_file']
    print(f"Resolved log path: {log_path}")

    ensure_path_exists(log_path)

    logger = logging.getLogger(POKEMON_DATA_INGEST)
    logger.setLevel(get_log_level(c['log']['log_level']))

    # Clear existing handlers to avoid duplicates
    logger.handlers.clear()

    # Console handler
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(logging.Formatter("%(asctime)s:%(levelname)s:%(filename)s:%(message)s"))
    logger.addHandler(stream_handler)

    # Rotating file handler
    file_handler = RotatingFileHandler(log_path, maxBytes=10 * 1024 * 1024, backupCount=5)
    file_handler.setFormatter(logging.Formatter("%(asctime)s:%(levelname)s:%(filename)s:%(message)s"))
    logger.addHandler(file_handler)

    print(f"Logs are available at: {log_path}")
    return logger


def load_query(file_name: str) -> str:
    sql_path = Path(__file__).parent / "sql" / file_name
    return sql_path.read_text()

def batch(iterable, n):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]
