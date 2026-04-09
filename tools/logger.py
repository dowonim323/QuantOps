import logging
import os
import sys
from datetime import datetime
from pathlib import Path


def default_log_dir(base_dir: str | os.PathLike[str]) -> str:
    return str(Path(base_dir) / "logs")


def _configure_external_loggers(*, pykis_level: int) -> None:
    pykis_logger = logging.getLogger("pykis")
    pykis_logger.handlers = []
    pykis_logger.propagate = True
    pykis_logger.setLevel(pykis_level)

    logging.getLogger("urllib3").setLevel(logging.ERROR)
    logging.getLogger("requests").setLevel(logging.ERROR)


def setup_logging(
    log_dir: str | os.PathLike[str],
    *,
    pykis_level: int = logging.ERROR,
) -> str:
    """
    Sets up logging to file and console with timestamps.
    """
    log_dir_path = Path(log_dir)
    log_dir_path.mkdir(parents=True, exist_ok=True)

    # Log filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir_path / f"{timestamp}_{os.getpid()}.log"

    # Clear existing handlers to ensure our configuration takes precedence
    root_logger = logging.getLogger()
    if root_logger.handlers:
        root_logger.handlers.clear()

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
        force=True,
    )

    _configure_external_loggers(pykis_level=pykis_level)

    logging.getLogger(__name__).info("Logging initialized. Log file: %s", log_file)
    return str(log_file)


def configure_entrypoint_logging(
    base_dir: str | os.PathLike[str],
    *,
    pykis_level: int = logging.ERROR,
) -> str:
    if logging.getLogger().handlers:
        return ""

    return setup_logging(default_log_dir(base_dir), pykis_level=pykis_level)
