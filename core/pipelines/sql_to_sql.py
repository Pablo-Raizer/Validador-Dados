import os
import json
import sys
import io
import re
import pandas as pd
from decimal import Decimal, InvalidOperation

from core.reports.google_sheets import GoogleSheetsReporter
from core.validator import aplicar_regras_central
from core.connectors import SQLServerConnector
from config.credentials import SQL_SERVER, SQL_SERVER_2

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')


def limpar_unicode(texto):
    try:
        return str(texto).encode('utf-8', errors='ignore').decode('utf-8')
    except:
        return ''


def formatar_valor_exportacao(x, campo=None):
    if pd.isna(x) or x in [None, ""]:
        return "null"

    campo = (campo or "").lower()

    if campo == "peso_variavel":
        return "Sim" if str(x).strip().upper() in ["TRUE", "1", "S"] else "Não"
    if campo == "ativo_inativo":
        return "Ativo" if str(x).strip().upper() in ["TRUE", "1", "S"] else "Inativo"

    if isinstance(x, pd.Timestamp):
        return x.strftime('%Y-%m-%d')

    if isinstance(x, (Decimal, float)):
        try:
            dec_val = Decimal(str(x))
            texto = f"{dec_val:.3f}"
            return texto.rstrip('0').rstrip('.') if '.' in texto else texto
        except (InvalidOperation, ValueError):
            pass

    if isinstance(x, float) and x.is_integer():
        return str(int(x))

    if campo == "dfcodigo_barra" and isinstance(x, str) and any(sep in x for sep in [' ', ';']):
        codigos = re.split(r'[;\s]+', x.strip())
        return "; ".join(codigos)

    return limpar_unicode(x)


class SQLToSQLPipeline:
    def __init__(self, json_file, source=None, target=None):
        self.source = source or SQLServerConnector(**SQL_SERVER)
        self.target = target or SQLServerConnector(**SQL_SERVER_2)
        self.planilha_id = "16u8KSsLNidNtlZ4daX2Vs-GALqgM7OvESHRpGB9VUI8"

        base_path = getattr(sys, '_MEIPASS', os.path.abspath("."))
        self.regra_path = os.path.join(base_path, "rules", json_file)

    def carregar_regras(self):
        with open(self.regra_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def converter_para_dataframe(self, dados):
        if isinstance(dados, tuple) and len(dados) == 2:
            colunas, linhas = dados
        else:
            linhas = dados
            colunas = [f"col{i}" for i in range(len(linhas[0]))] if linhas else []
        return pd.DataFrame(linhas, columns=colunas)

    def get_database_names(self):
        origem = self.source.config.get("database", "Origem Desconhecida")
        destino = self.target.config.get("database", "Destino Desconhecida")
        return origem, destino

    def validate(self):
        origem, destino = self.get_database_names()
        print(f"[Banco Origem]: {origem}")
        print(f"[Banco Destino]: {destino}")
        print(">>> Iniciando validacao com pipeline: sql_to_sql")
        print(f">>> Usando regras: {os.path.basename(self.regra_path)}")

        regras = self.carregar_regras()
        df_resultado = aplicar_regras_central(self.source, self.target, regras)

        if "tabelas" in regras:
            primeira = next(iter(regras["tabelas"].values()))
            campos_atuais = primeira.get("campos", {})
        else:
            campos_atuais = regras.get("campos", {})

        try:
            df_resultado["CODIGO_ORD"] = df_resultado["CODIGO"].apply(lambda x: int(str(x).strip()))
        except:
            df_resultado["CODIGO_ORD"] = df_resultado["CODIGO"].astype(str)

        ordem_campos = [campo.replace("DF", "") for campo in campos_atuais.keys()]
        df_resultado["__ordem_campo__"] = df_resultado["CAMPO"].apply(
            lambda x: ordem_campos.index(x) if x in ordem_campos else 999
        )

        df_resultado.sort_values(by=["CODIGO_ORD", "__ordem_campo__"], inplace=True)
        df_resultado.drop(columns=["CODIGO_ORD", "__ordem_campo__"], inplace=True)
        df_resultado.reset_index(drop=True, inplace=True)

        if "tabelas" in regras:
            primeira = next(iter(regras["tabelas"].values()))
            source_query = primeira.get("source_query")
            target_query = primeira.get("target_query")
        else:
            source_query = regras.get("source_query")
            target_query = regras.get("target_query")

        df_origem = self.converter_para_dataframe(self.source.fetch_data(source_query))
        df_destino = self.converter_para_dataframe(self.target.fetch_data(target_query))

        self.salvar_resultados(df_resultado, regras, df_origem, df_destino, campos_atuais)
        return df_resultado

    def salvar_resultados(self, df, regras, df_origem, df_destino, campos_atuais):
        entidade = regras.get("entidade", "Desconhecido").capitalize()
        output_dir = os.path.join("data", "output")
        os.makedirs(output_dir, exist_ok=True)

        output_excel = os.path.join(output_dir, f"validacao_{entidade.lower()}.xlsx")
        output_csv = os.path.join(output_dir, f"validacao_{entidade.lower()}.csv")

        df_export = df.copy()

        for i, row in df_export.iterrows():
            campo = row["CAMPO"]
            for col in ["SQL", "ORIGEM"]:
                val = row[col]
                if row["STATUS"] == "OK":
                    df_export.at[i, col] = formatar_valor_exportacao(val, campo=campo)

        col_status_sql = None
        for nome_dest, props in campos_atuais.items():
            if nome_dest.lower() in ["dfativo", "dfativo_inativo", "ativo", "ativo_inativo"]:
                if nome_dest in df_destino.columns:
                    col_status_sql = nome_dest
                    break

        col_inativ_sql = "DFdata_inativacao" if "DFdata_inativacao" in df_destino.columns else None

        if col_inativ_sql:
            ativos_sql = df_destino[col_inativ_sql].isna().sum()
            inativos_sql = df_destino[col_inativ_sql].notna().sum()
        elif col_status_sql:
            ativos_sql = df_destino[df_destino[col_status_sql].astype(str).str.upper().isin(["TRUE", "1", "S"])].shape[0]
            inativos_sql = df_destino[df_destino[col_status_sql].astype(str).str.upper().isin(["FALSE", "0", "N"])].shape[0]
        else:
            ativos_sql = inativos_sql = 0

        if "ATIVO" in df_origem.columns:
            ativos_ori = df_origem[df_origem["ATIVO"].astype(str).str.upper() == "S"].shape[0]
            inativos_ori = df_origem[df_origem["ATIVO"].astype(str).str.upper() == "N"].shape[0]
        else:
            ativos_ori = inativos_ori = 0

        status_ativos = "OK" if ativos_sql == ativos_ori else "Divergente"
        status_inativos = "OK" if inativos_sql == inativos_ori else "Divergente"

        resumo = pd.DataFrame([
            {"CODIGO": "", "CAMPO": "", "SQL": "", "ORIGEM": "", "STATUS": ""},
            {"CODIGO": "", "CAMPO": f"=== RESUMO {entidade.upper()} ===", "SQL": "", "ORIGEM": "", "STATUS": ""},
            {"CODIGO": "", "CAMPO": f"Total {entidade} no SQL", "SQL": df_destino.shape[0], "ORIGEM": "", "STATUS": ""},
            {"CODIGO": "", "CAMPO": f"Total {entidade} na Origem", "SQL": "", "ORIGEM": df_origem.shape[0], "STATUS": ""},
            {"CODIGO": "", "CAMPO": f"{entidade} Ativos", "SQL": ativos_sql, "ORIGEM": ativos_ori, "STATUS": status_ativos},
            {"CODIGO": "", "CAMPO": f"{entidade} Inativos", "SQL": inativos_sql, "ORIGEM": inativos_ori, "STATUS": status_inativos}
        ])

        df_export = pd.concat([df_export, resumo], ignore_index=True)

        try:
            df_export.to_excel(output_excel, index=False, engine='openpyxl')
            print(f"[OK] Excel salvo em: {os.path.abspath(output_excel)}")
        except Exception as e:
            print(f"[ERRO] Falha ao salvar Excel: {repr(e)}")

        try:
            df_export.to_csv(output_csv, index=False, encoding='utf-8-sig')
            print(f"[OK] CSV salvo em: {os.path.abspath(output_csv)}")
        except Exception as e:
            print(f"[ERRO] Falha ao salvar CSV: {repr(e)}")

        try:
            aba = entidade
            reporter = GoogleSheetsReporter(self.planilha_id)
            reporter.exportar(df_export.astype(str), aba=aba)
            print(f"[OK] Exportado para Google Sheets na aba '{aba}' com sucesso.")
        except Exception as e:
            print(f"[ERRO] Falha ao exportar para Google Sheets: {repr(e)}")


def executar(json_file):
    pipeline = SQLToSQLPipeline(json_file)
    return pipeline.validate()
