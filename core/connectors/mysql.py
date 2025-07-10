import mysql.connector
from mysql.connector import Error
import logging
from core.connectors.base_connector import BaseConnector

logger = logging.getLogger(__name__)

class MySQLConnector(BaseConnector):
    def __init__(self, config):
        self.config = config
        self.database = config.get("database")
        self.conn = None
        
    def test_connection(self) -> bool:
        """Testa conexão com o banco MySQL"""
        try:
            logger.info(f"Testando conexão MySQL - Host: {self.config['host']}, Database: {self.database}")
            conn = mysql.connector.connect(
                host=self.config['host'],
                database=self.config['database'],
                user=self.config['user'],
                password=self.config['password'],
                connection_timeout=30,  # Timeout de 30 segundos
                autocommit=True
            )
            
            # Teste simples
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            
            if result:
                logger.info("✅ Conexão MySQL testada com sucesso")
                return True
            else:
                logger.error("❌ Falha no teste de conexão MySQL - Sem resultado")
                return False
                
        except Error as e:
            logger.error(f"❌ Erro de conexão MySQL: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"❌ Erro inesperado na conexão MySQL: {str(e)}")
            return False

    def connect(self):
        """Conecta ao banco com timeout"""
        try:
            if self.conn is None:
                logger.debug(f"Conectando ao MySQL: {self.config['host']}:{self.database}")
                self.conn = mysql.connector.connect(
                    host=self.config['host'],
                    database=self.config['database'],
                    user=self.config['user'],
                    password=self.config['password'],
                    connection_timeout=30,  # Timeout de conexão
                    autocommit=True,
                    charset='utf8mb4',
                    use_unicode=True
                )
            return self.conn
        except Error as e:
            logger.error(f"Erro ao conectar no MySQL: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Erro inesperado ao conectar no MySQL: {str(e)}")
            raise

    def fetch_data(self, query: str) -> tuple:
        """Busca dados com tratamento de erros"""
        conn = None
        cursor = None
        
        try:
            logger.debug(f"Executando query MySQL: {query[:100]}...")
            
            conn = self.connect()
            cursor = conn.cursor(dictionary=True)
            cursor.execute(query)
            
            data = cursor.fetchall()
            
            # Extrair colunas do primeiro registro
            columns = list(data[0].keys()) if data else []
            
            logger.info(f"Query MySQL executada - {len(data)} registros retornados")
            
            return columns, data
            
        except Error as e:
            logger.error(f"Erro SQL MySQL: {str(e)}")
            logger.error(f"Query: {query}")
            raise
        except Exception as e:
            logger.error(f"Erro inesperado ao executar query MySQL: {str(e)}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn and conn != self.conn:  # Se não é conexão persistente
                conn.close()
    
    def close(self):
        """Fecha conexão"""
        try:
            if self.conn:
                self.conn.close()
                self.conn = None
                logger.debug("Conexão MySQL fechada")
        except Exception as e:
            logger.error(f"Erro ao fechar conexão MySQL: {str(e)}")
    
    def __del__(self):
        """Cleanup automático"""
        self.close()