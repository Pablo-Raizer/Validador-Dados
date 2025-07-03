import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import sys

class GoogleSheetsReporter:
    def __init__(self, planilha_id: str):
        escopos = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]

        # Ajusta o caminho do JSON para funcionar no PyInstaller
        if hasattr(sys, '_MEIPASS'):
            base_path = sys._MEIPASS
        else:
            base_path = os.path.abspath(".")

        cred_path = os.path.join(base_path, "config", "google_credentials.json")

        if not os.path.exists(cred_path):
            raise FileNotFoundError(f"Arquivo de credenciais não encontrado em: {cred_path}")

        self.creds = Credentials.from_service_account_file(cred_path, scopes=escopos)
        self.gc = gspread.authorize(self.creds)
        self.planilha_id = planilha_id

    def exportar(self, df: pd.DataFrame, aba: str = "Resultado"):
        planilha = self.gc.open_by_key(self.planilha_id)

        try:
            worksheet = planilha.worksheet(aba)
            # Deleta e recria para limpar dados antigos
            planilha.del_worksheet(worksheet)
            worksheet = planilha.add_worksheet(title=aba, rows=str(len(df) + 10), cols=str(len(df.columns)))
        except gspread.WorksheetNotFound:
            worksheet = planilha.add_worksheet(title=aba, rows=str(len(df) + 10), cols=str(len(df.columns)))

        # Trata dados: datas como string, NaNs como vazio
        df_clean = df.copy()
        for col in df_clean.columns:
            if pd.api.types.is_datetime64_any_dtype(df_clean[col]):
                df_clean[col] = df_clean[col].astype(str)
            else:
                df_clean[col] = df_clean[col].fillna('')

        dados = [df_clean.columns.values.tolist()] + df_clean.values.tolist()

        # Compatível com gspread >=6.0.0 e versões anteriores
        try:
            worksheet.update(range_name=f"A1", values=dados)
        except TypeError:
            worksheet.update(dados, range_name=f"A1")
