from abc import ABC, abstractmethod
import logging

logger = logging.getLogger(__name__)

class BaseConnector(ABC):
    """Classe base para todos os conectores de banco de dados"""
    
    def __init__(self, config: dict):
        self.config = config
        self.database = config.get("database")
        self.conn = None
    
    @abstractmethod
    def test_connection(self) -> bool:
        """Testa se a conexão está funcionando"""
        pass
    
    @abstractmethod
    def connect(self):
        """Estabelece conexão com o banco"""
        pass
    
    @abstractmethod
    def fetch_data(self, query: str) -> tuple:
        """Executa query e retorna (colunas, dados)"""
        pass
    
    @abstractmethod
    def close(self):
        """Fecha conexão"""
        pass
    
    def validate_config(self) -> bool:
        """Valida configuração básica"""
        required_fields = ['host', 'database', 'user', 'password']
        
        for field in required_fields:
            if field not in self.config or not self.config[field]:
                logger.error(f"Campo obrigatório '{field}' não encontrado na configuração")
                return False
        
        logger.debug("Configuração validada com sucesso")
        return True
    
    def get_info(self) -> str:
        """Retorna informações da conexão"""
        return f"{self.__class__.__name__} - {self.config.get('host', 'N/A')}:{self.database}"