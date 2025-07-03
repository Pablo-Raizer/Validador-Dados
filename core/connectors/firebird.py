import fdb
from core.connectors.base_connector import BaseConnector

class FirebirdConnector(BaseConnector):
    def __init__(self, config: dict):
        self.config = config
        self.database = config.get("database")  # <-- adicionado para nome do banco

    def connect(self):
        return fdb.connect(
            dsn=f"{self.config['host']}:{self.config['database']}",
            user=self.config['user'],
            password=self.config['password']
        )

    def fetch_data(self, query: str) -> tuple:
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [col[0].strip() for col in cursor.description]
        data = cursor.fetchall()
        cursor.close()
        conn.close()
        return columns, data
