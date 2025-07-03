import pandas as pd
import json
import os

CAMINHO_PLANILHA = r"C:\Users\pablo\validacao_dados\data\input\template_regras_clientes.xlsx"
PASTA_SAIDA = r"C:\Users\pablo\validacao_dados\data\output\jsons"

def processar_aba(df):
    colunas_obrigatorias = ["primary_key", "campo_sql", "target_table"]
    for coluna in colunas_obrigatorias:
        if coluna not in df.columns:
            print(f"  ⛔ Aba ignorada: coluna obrigatória '{coluna}' não encontrada.")
            return None

    entidade = df["entidade"].dropna().iloc[0] if "entidade" in df.columns else "Desconhecida"
    tabelas_encontradas = df["target_table"].dropna().unique()

    if len(tabelas_encontradas) == 1:
        print(f"  ➡ Aba '{entidade}' com apenas uma tabela. Mantendo formato simples.")
        return processar_tabela(df, entidade)
    else:
        print(f"  🔁 Aba '{entidade}' com múltiplas tabelas. Usando formato agrupado.")
        return processar_multiplas_tabelas(df, entidade)

def processar_tabela(df, entidade):
    pk_destino = df["primary_key"].dropna().iloc[0]
    linha_chave = df[df["campo_sql"] == pk_destino]
    if linha_chave.empty or pd.isna(linha_chave.iloc[0]["source_column"]):
        print("  ❌ Não foi possível identificar o campo de origem da chave primária.")
        return None
    pk_origem = str(linha_chave.iloc[0]["source_column"]).strip()

    resultado = {
        "entidade": entidade,
        "primary_key": [pk_destino, pk_origem],
        "source_table": df["source_table"].dropna().iloc[0],
        "target_table": df["target_table"].dropna().iloc[0],
        "source_query": df["source_query"].dropna().iloc[0],
        "target_query": df["target_query"].dropna().iloc[0],
        "campos": {}
    }

    for _, row in df.iterrows():
        campo_sql = str(row.get("campo_sql", "")).strip()
        if not campo_sql:
            continue

        campo_dict = montar_dict_campo(row)
        resultado["campos"][campo_sql] = campo_dict

    return resultado

def processar_multiplas_tabelas(df, entidade):
    resultado = {"entidade": entidade, "tabelas": {}}

    for tabela_destino, grupo in df.groupby("target_table"):
        grupo = grupo.reset_index(drop=True)

        if grupo["primary_key"].dropna().empty:
            print(f"  ⚠ Tabela '{tabela_destino}' ignorada (sem primary_key).")
            continue

        pk_destino = grupo["primary_key"].dropna().iloc[0]
        linha_chave = grupo[grupo["campo_sql"] == pk_destino]
        if linha_chave.empty or pd.isna(linha_chave.iloc[0]["source_column"]):
            print(f"  ❌ Tabela '{tabela_destino}' sem source_column válida para a chave primária.")
            continue

        pk_origem = str(linha_chave.iloc[0]["source_column"]).strip()

        tabela_dict = {
            "primary_key": [pk_destino, pk_origem],
            "source_table": grupo["source_table"].dropna().iloc[0],
            "target_table": tabela_destino,
            "source_query": grupo["source_query"].dropna().iloc[0],
            "target_query": grupo["target_query"].dropna().iloc[0],
            "campos": {}
        }

        for _, row in grupo.iterrows():
            campo_sql = str(row.get("campo_sql", "")).strip()
            if not campo_sql:
                continue

            campo_dict = montar_dict_campo(row)
            tabela_dict["campos"][campo_sql] = campo_dict

        resultado["tabelas"][tabela_destino] = tabela_dict

    return resultado

def montar_dict_campo(row):
    campo_dict = {}

    source_column = row.get("source_column", "")
    if pd.notna(source_column) and "," in str(source_column):
        campo_dict["source_column"] = [col.strip() for col in str(source_column).split(",")]
    else:
        campo_dict["source_column"] = str(source_column).strip()

    if pd.notna(row.get("comparador")):
        campo_dict["comparador"] = str(row["comparador"]).strip()

    if pd.notna(row.get("observacao")):
        campo_dict["observacao"] = str(row["observacao"]).strip()

    if pd.notna(row.get("condicao_campo")) and pd.notna(row.get("condicao_valor")):
        condicoes = str(row["condicao_valor"]).split(";")
        cond_dict = {}
        for cond in condicoes:
            if "=" in cond:
                k, v = cond.split("=")
                cond_dict[k.strip()] = v.strip()
        campo_dict["condicao"] = {str(row["condicao_campo"]).strip(): cond_dict}

    if "normalizar" in row and not pd.isna(row["normalizar"]):
        campo_dict["normalizar"] = bool(row["normalizar"])
    else:
        campo_dict["normalizar"] = True

    return campo_dict

def gerar_jsons_de_planilha():
    os.makedirs(PASTA_SAIDA, exist_ok=True)
    abas = pd.read_excel(CAMINHO_PLANILHA, sheet_name=None)

    for nome_aba, df in abas.items():
        print(f"\n🔄 Processando aba: {nome_aba}")
        json_data = processar_aba(df)
        if json_data is None:
            print(f"  ❌ Aba '{nome_aba}' ignorada.")
            continue

        nome_arquivo = os.path.join(PASTA_SAIDA, f"regras_{nome_aba.lower()}.json")
        with open(nome_arquivo, "w", encoding="utf-8") as f:
            json.dump(json_data, f, indent=2, ensure_ascii=False)
        print(f"✅ JSON salvo: {nome_arquivo}")

if __name__ == "__main__":
    gerar_jsons_de_planilha()
