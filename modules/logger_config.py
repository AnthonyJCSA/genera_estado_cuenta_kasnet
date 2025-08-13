import logging
import sys
import traceback
from datetime import datetime
import os

class LoggerConfig:
    @staticmethod
    def setup_logger(nombre_log: str = "logs_ejecucion"):
        """
        Configura el logging global del proyecto.
        Crea un archivo con nombre dinámico: logs_ejecucion_YYYYMMDD.log
        """
        fecha = datetime.now().strftime("%Y%m%d")
        log_dir = "output"
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, f"{nombre_log}_{fecha}.log")

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
            handlers=[
                logging.FileHandler(log_file, mode="a", encoding="utf-8"),
                logging.StreamHandler(sys.stdout)
            ]
        )
        logging.info(f"[logger_config.py] - Logging inicializado en {log_file}")

    @staticmethod
    def log_exception(e: Exception, archivo: str):
        """
        Registra una excepción con traceback detallado.
        """
        tb = "".join(traceback.format_exception(type(e), e, e.__traceback__))
        logging.error(f"[{archivo}] - Excepción capturada:\n{tb}")
