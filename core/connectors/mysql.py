import mysql.connector
from core.connectors.base_connector import BaseConnector

class MySQLConnector(BaseConnector):
    def __init__(self, config):
        self.config = config
        self.database = config.get("database")  # <-- necessário para exibir no log

    def connect(self):
        return mysql.connector.connect(
            host=self.config['host'],
            database=self.config['database'],
            user=self.config['user'],
            password=self.config['password']
        )

    def fetch_data(self, query: str) -> list:
        conn = self.connect()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(query)
        data = cursor.fetchall()
        cursor.close()
        conn.close()
        return list(data[0].keys()), data
