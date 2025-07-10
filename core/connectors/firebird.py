import fdb
import logging
from core.connectors.base_connector import BaseConnector

logger = logging.getLogger(__name__)

class FirebirdConnector(BaseConnector):
    def __init__(self, config: dict):
        self.config = config
        self.database = config.get("database")
        self.conn = None
        
    def test_connection(self) -> bool:
        """Testa conexão com o banco Firebird"""
        try:
            logger.info(f"Testando conexão Firebird - Host: {self.config['host']}, Database: {self.database}")
            conn = fdb.connect(
                dsn=f"{self.config['host']}:{self.config['database']}",
                user=self.config['user'],
                password=self.config['password']
            )
            
            # Teste simples
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM RDB$DATABASE")
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            
            if result:
                logger.info("✅ Conexão Firebird testada com sucesso")
                return True
            else:
                logger.error("❌ Falha no teste de conexão Firebird - Sem resultado")
                return False
                
        except fdb.Error as e:
            logger.error(f"❌ Erro de conexão Firebird: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"❌ Erro inesperado na conexão Firebird: {str(e)}")
            return False

    def connect(self):
        """Conecta ao banco Firebird"""
        try:
            if self.conn is None:
                logger.debug(f"Conectando ao Firebird: {self.config['host']}:{self.database}")
                self.conn = fdb.connect(
                    dsn=f"{self.config['host']}:{self.config['database']}",
                    user=self.config['user'],
                    password=self.config['password'],
                    sql_dialect=3
                )
            return self.conn
        except fdb.Error as e:
            logger.error(f"Erro ao conectar no Firebird: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Erro inesperado ao conectar no Firebird: {str(e)}")
            raise

    def fetch_data(self, query: str) -> tuple:
        """Busca dados com tratamento de erros"""
        conn = None
        cursor = None
        
        try:
            logger.debug(f"Executando query Firebird: {query[:100]}...")
            
            conn = self.connect()
            cursor = conn.cursor()
            cursor.execute(query)
            
            columns = [col[0].strip() for col in cursor.description]
            data = cursor.fetchall()
            
            logger.info(f"Query Firebird executada - {len(data)} registros retornados")
            
            return columns, data
            
        except fdb.Error as e:
            logger.error(f"Erro SQL Firebird: {str(e)}")
            logger.error(f"Query: {query}")
            raise
        except Exception as e:
            logger.error(f"Erro inesperado ao executar query Firebird: {str(e)}")
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
                logger.debug("Conexão Firebird fechada")
        except Exception as e:
            logger.error(f"Erro ao fechar conexão Firebird: {str(e)}")
    
    def __del__(self):
        """Cleanup automático"""
        self.close()
