import importlib
import sys
import logging
import pandas as pd
from utils.log_config import setup_logging, disable_external_loggers

# Configurar logging
setup_logging()
disable_external_loggers()
logger = logging.getLogger(__name__)

def validar_argumentos():
    """Valida argumentos da linha de comando"""
    if len(sys.argv) < 3:
        logger.error("Argumentos insuficientes")
        logger.info("Uso: python main.py <nome_pipeline> <arquivo_de_regras.json>")
        logger.info("Exemplo: python main.py firebird_to_sql regras_clientes.json")
        sys.exit(1)
    
    nome_pipeline = sys.argv[1]
    json_file = sys.argv[2]
    
    logger.info(f"Iniciando validação - Pipeline: {nome_pipeline}, Regras: {json_file}")
    
    return nome_pipeline, json_file

def executar_pipeline(nome_pipeline, json_file):
    """Executa pipeline com validação completa"""
    try:
        logger.info(f"Carregando módulo do pipeline: {nome_pipeline}")
        
        # Importar módulo do pipeline
        modulo = importlib.import_module(f"core.pipelines.{nome_pipeline}")
        
        if not hasattr(modulo, 'executar'):
            logger.error(f"Pipeline '{nome_pipeline}' não possui função 'executar'")
            return False
        
        logger.info(f"Módulo carregado com sucesso: {nome_pipeline}")
        logger.info("=" * 50)
        logger.info("INICIANDO EXECUÇÃO DO PIPELINE")
        logger.info("=" * 50)
        
        resultado = modulo.executar(json_file)
        
        logger.info("=" * 50)
        logger.info("PIPELINE EXECUTADO COM SUCESSO")
        logger.info("=" * 50)
        
        return resultado

    except ImportError as e:
        logger.error(f"Pipeline '{nome_pipeline}' não encontrado: {str(e)}")
        logger.error("Pipelines disponíveis: firebird_to_sql, mysql_to_sql, sql_to_sql")
        return False
    except FileNotFoundError as e:
        logger.error(f"Arquivo de regras '{json_file}' não encontrado: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Falha ao executar o pipeline '{nome_pipeline}': {str(e)}")
        logger.exception("Detalhes do erro:")
        return False

def main():
    """Função principal com tratamento completo de erros"""
    try:
        nome_pipeline, json_file = validar_argumentos()
        sucesso = executar_pipeline(nome_pipeline, json_file)

        if isinstance(sucesso, dict) and isinstance(sucesso.get("resultado"), pd.DataFrame):
            if not sucesso["resultado"].empty:
                logger.info("Validação concluída com sucesso!")
                sys.exit(0)
            else:
                logger.warning("Validação executada, mas nenhum dado retornado.")
                sys.exit(1)
        elif sucesso is True:
            logger.info("Validação concluída com sucesso!")
            sys.exit(0)
        else:
            logger.error("Validação falhou!")
            sys.exit(1)

    except KeyboardInterrupt:
        logger.warning("Execução interrompida pelo usuário")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Erro crítico na execução: {str(e)}")
        logger.exception("Detalhes do erro:")
        sys.exit(1)

if __name__ == "__main__":
    main()
