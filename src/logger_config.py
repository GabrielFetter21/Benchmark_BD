import logging
import os

def configurar_logger():
    """Configura o logger principal do sistema."""
    os.makedirs("logs", exist_ok=True)

    logger = logging.getLogger("loja_logger")
    logger.setLevel(logging.DEBUG)

    # Formatter com data e nível
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Arquivo de log
    file_handler = logging.FileHandler("logs/execucao.log", encoding="utf-8")
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)

    # Log no console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)

    # Evita duplicação de handlers
    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger
