import pyodbc
import logging
from core.connectors.base_connector import BaseConnector

logger = logging.getLogger(__name__)

class SQLServerConnector(BaseConnector):
    def __init__(self, config: dict):
        self.config = config
        self.database = config.get("database")
        self.conn = None
        self.connection_string = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"Server={config['Server']};"
            f"DATABASE={config['database']};"
            f"UID={config['user']};"
            f"PWD={config['password']};"
            f"Connection Timeout=30;"  # Timeout de conexão
            f"Command Timeout=60;"     # Timeout de comando
        )

    def test_connection(self) -> bool:
        """Testa conexão com o banco SQL Server"""
        try:
            logger.info(f"Testando conexão SQL Server - Server: {self.config['Server']}, Database: {self.database}")
            conn = pyodbc.connect(self.connection_string)
            
            # Teste simples
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            
            if result:
                logger.info("✅ Conexão SQL Server testada com sucesso")
                return True
            else:
                logger.error("❌ Falha no teste de conexão SQL Server - Sem resultado")
                return False
                
        except pyodbc.Error as e:
            logger.error(f"❌ Erro de conexão SQL Server: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"❌ Erro inesperado na conexão SQL Server: {str(e)}")
            return False

    def connect(self):
        """Conecta ao banco com timeout"""
        try:
            if self.conn is None:
                logger.debug(f"Conectando ao SQL Server: {self.config['Server']}:{self.database}")
                self.conn = pyodbc.connect(self.connection_string)
                
                # Configurações adicionais
                self.conn.timeout = 60  # Timeout para comandos
                
            return self.conn
        except pyodbc.Error as e:
            logger.error(f"Erro ao conectar no SQL Server: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Erro inesperado ao conectar no SQL Server: {str(e)}")
            raise

    def fetch_data(self, query: str) -> tuple:
        """Busca dados com tratamento de erros"""
        cursor = None
        
        try:
            logger.debug(f"Executando query SQL Server: {query[:100]}...")
            
            self.connect()
            cursor = self.conn.cursor()
            cursor.execute(query)

            columns = [column[0] for column in cursor.description]
            rows = cursor.fetchall()
            
            # Converter para tuplas
            data = [tuple(row) for row in rows]
            
            logger.info(f"Query SQL Server executada - {len(data)} registros retornados")
            
            return columns, data
            
        except pyodbc.Error as e:
            logger.error(f"Erro SQL Server: {str(e)}")
            logger.error(f"Query: {query}")
            raise
        except Exception as e:
            logger.error(f"Erro inesperado ao executar query SQL Server: {str(e)}")
            raise
        finally:
            if cursor:
                cursor.close()

    def close(self):
        """Fecha conexão"""
        try:
            if self.conn:
                self.conn.close()
                self.conn = None
                logger.debug("Conexão SQL Server fechada")
        except Exception as e:
            logger.error(f"Erro ao fechar conexão SQL Server: {str(e)}")
    
    def __del__(self):
        """Cleanup automático"""
        self.close()