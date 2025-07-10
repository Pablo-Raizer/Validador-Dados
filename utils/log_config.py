import logging
import os
import sys
from datetime import datetime
import colorlog

_LOGGER_INITIALIZED = False  # evita múltiplas inicializações

def setup_logging(log_level=logging.INFO):
    """
    Configura sistema de logs estruturados com suporte UTF-8 e evita handlers duplicados
    """
    global _LOGGER_INITIALIZED
    if _LOGGER_INITIALIZED:
        return logging.getLogger(__name__)  # já configurado

    # Forçar UTF-8 no console do Windows
    if sys.platform.startswith("win"):
        import ctypes
        ctypes.windll.kernel32.SetConsoleOutputCP(65001)

    os.makedirs("logs", exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = f"logs/validacao_{timestamp}.log"

    # Formatter para arquivo
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Formatter colorido para console
    console_formatter = colorlog.ColoredFormatter(
        '%(log_color)s%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S',
        log_colors={
            'DEBUG': 'cyan',
            'INFO': 'green',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'red,bg_white',
        }
    )

    # Criar handlers
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setFormatter(file_formatter)
    file_handler.setLevel(log_level)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(log_level)

    # Força flush após cada emit (evita bufferização)
    def emit_and_flush(self, record):
        logging.StreamHandler.emit(self, record)
        self.flush()

    console_handler.emit = emit_and_flush.__get__(console_handler, logging.StreamHandler)

    # Configurar root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Remover todos os handlers antigos
    while root_logger.hasHandlers():
        root_logger.removeHandler(root_logger.handlers[0])

    # Adicionar os dois handlers
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    # Desabilita log duplicado de libs externas
    disable_external_loggers()

    # Log inicial - sem emojis para evitar erro de encoding
    logger = logging.getLogger(__name__)
    logger.info("=" * 60)
    logger.info("SISTEMA DE VALIDACAO DE DADOS - INICIADO")
    logger.info(f"Arquivo de log: {log_filename}")
    logger.info("=" * 60)

    _LOGGER_INITIALIZED = True
    return log_filename


def get_logger(name):
    """
    Retorna logger configurado para um módulo específico
    """
    return logging.getLogger(name)


def disable_external_loggers():
    """
    Desabilita logs verbosos de bibliotecas externas
    """
    for lib in ['urllib3', 'requests', 'google', 'gspread', 'sqlalchemy']:
        logging.getLogger(lib).setLevel(logging.WARNING)
