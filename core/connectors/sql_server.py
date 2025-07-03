import pyodbc
from core.connectors.base_connector import BaseConnector

class SQLServerConnector(BaseConnector):
    def __init__(self, config: dict):
        self.config = config
        self.database = config.get("database")  # <-- necessário para exibir no log
        self.conn = None
        self.connection_string = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"Server={config['Server']};"
            f"DATABASE={config['database']};"
            f"UID={config['user']};"
            f"PWD={config['password']}"
        )

    def connect(self):
        if self.conn is None:
            self.conn = pyodbc.connect(self.connection_string)

    def fetch_data(self, query: str):
        self.connect()
        cursor = self.conn.cursor()
        cursor.execute(query)

        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()

        return columns, [tuple(row) for row in rows]

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None
