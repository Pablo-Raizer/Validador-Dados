import pandas as pd
import re
import unicodedata
from decimal import Decimal
from datetime import datetime

# --- Normalizadores ---
def normalizar_valor(val):
    """Normaliza texto: tira acento, pontuação e espaços extras. Tudo maiúsculo e limpo para comparação robusta."""
    if pd.isna(val):
        return ''
    if isinstance(val, float) and val.is_integer():
        val = int(val)
    val = str(val).strip()
    val = unicodedata.normalize('NFKD', val)
    val = ''.join(c for c in val if not unicodedata.combining(c))
    val = val.upper()
    val = re.sub(r'[\(\)]', ' ', val)
    val = re.sub(r'[-/]', ' ', val)
    val = re.sub(r'\.', ' ', val)
    val = re.sub(r'[^A-Z0-9\s]', ' ', val)
    val = re.sub(r'\s+', ' ', val)
    return val.strip()

def normalizar_ie(valor):
    """Remove tudo que não é número, mantendo 'ISENTO' literal."""
    valor = str(valor).strip().upper()
    if valor == "ISENTO":
        return "ISENTO"
    return re.sub(r'\D', '', valor)

def normalizar_case_insensitive(val):
    """Converte para minúsculo e tira espaços."""
    if val is None:
        return ''
    return str(val).strip().lower()

def normalizar_numero(val):
    """Extrai apenas números e remove zeros à esquerda."""
    if pd.isna(val):
        return ""
    if isinstance(val, float) and val.is_integer():
        val = int(val)
    return str(int(re.sub(r'\D', '', str(val).strip() or '0')))

def normalizar_data_sem_hora(val):
    """Remove hora e retorna só a data."""
    if isinstance(val, pd.Timestamp) or isinstance(val, datetime):
        return val.date()
    return val

def normalizar_bool_sql_ativo(val):
    """Converte booleanos e strings para 'S' ou 'N' (ativo/inativo)."""
    if isinstance(val, bool):
        return 'S' if val else 'N'
    if str(val).strip().upper() in ['TRUE', '1', 'S']:
        return 'S'
    return 'N'

def normalizar_bool_sql_peso_variavel(val):
    """Converte valores relacionados a peso variável para 'S' ou 'N'."""
    return 'S' if val in [True, '1', 1, 'S', 'TRUE'] else 'N'

def normalizar_numero_null_igual_zero(val):
    """Converte nulos ou strings vazias em Decimal(0.0)."""
    if pd.isna(val) or str(val).strip().lower() in ["", "none", "null"]:
        return Decimal("0.0")
    try:
        return Decimal(str(val))
    except:
        return Decimal("0.0")

def normalizar_codigos_multiplos(val):
    """Divide códigos por espaço/; e ordena para comparar com todos presentes."""
    if not val:
        return ""
    codigos = set(re.split(r'[;\s]+', str(val).strip()))
    codigos = sorted(filter(None, codigos))
    return ";".join(codigos)

# --- Comparadores ---
def comparador_default(val_sql, val_ori, **_):
    return val_sql == val_ori

def comparador_case_insensitive(val_sql, val_ori, **_):
    return val_sql == val_ori

def comparador_ie_equivalente(val_sql, val_ori, **_):
    if val_sql == "ISENTO" and (val_ori is None or val_ori == ""):
        return True
    return val_sql == val_ori

def comparador_data_sem_hora(val_sql, val_ori, **_):
    try:
        data_sql = val_sql.date() if hasattr(val_sql, 'date') else val_sql
        data_ori = val_ori.date() if hasattr(val_ori, 'date') else val_ori
        return data_sql == data_ori
    except:
        return False

def comparador_bool_sql_ativo(val_sql, val_ori, **_):
    return val_sql == val_ori

def comparador_bool_sql_peso_variavel(val_sql, val_ori, **_):
    return val_sql == val_ori

def comparador_numero_string(val_sql, val_ori, **_):
    val_sql_str = normalizar_numero(val_sql)
    val_ori_str = normalizar_numero(val_ori)
    return val_sql_str == val_ori_str

def comparador_numero_null_igual_zero(val_sql, val_ori, **_):
    return val_sql == val_ori

def comparador_multiplos_codigos_todos_presentes(val_sql, val_ori, **_):
    set_sql = set(re.split(r'[;\s]+', str(val_sql).strip())) if val_sql else set()
    set_ori = set(re.split(r'[;\s]+', str(val_ori).strip())) if val_ori else set()
    return set_ori.issubset(set_sql)

def comparador_decimal_tolerancia(val_sql, val_ori, **_):
    try:
        d_sql = Decimal(str(val_sql))
        d_ori = Decimal(str(val_ori))
        return abs(d_sql - d_ori) <= Decimal("0.01")
    except:
        return str(val_sql).strip() == str(val_ori).strip()

def comparador_data_inativacao_logica(val_sql, val_flag_ativo, **_):
    if val_flag_ativo == 'S':
        return pd.isna(val_sql) or val_sql > pd.Timestamp.now()
    if val_flag_ativo == 'N':
        return pd.notna(val_sql)
    return False

# --- Mapeamento comparador -> normalizador padrão ---
normalizador_padrao_por_comparador = {
    "default": "valor",
    "case_insensitive": "case_insensitive",
    "ie_equivalente": "ie",
    "data_sem_hora": "data_sem_hora",
    "bool_sql_ativo": "bool_sql_ativo",
    "bool_sql_peso_variavel": "bool_sql_peso_variavel",
    "data_inativacao_logica": None,
    "numero_string": "numero",
    "numero_null_igual_zero": "numero_null_igual_zero",
    "numero_decimal_aproximado_2": "numero_null_igual_zero",
    "numero_decimal_aproximado_3": "numero_null_igual_zero",
    "decimal_tolerancia": "numero_null_igual_zero",
    "multiplos_codigos_todos_presentes": "multiplos_codigos",
}

# --- Registro global ---
normalizadores = {
    "valor": normalizar_valor,
    "ie": normalizar_ie,
    "case_insensitive": normalizar_case_insensitive,
    "numero": normalizar_numero,
    "data_sem_hora": normalizar_data_sem_hora,
    "bool_sql_ativo": normalizar_bool_sql_ativo,
    "bool_sql_peso_variavel": normalizar_bool_sql_peso_variavel,
    "numero_null_igual_zero": normalizar_numero_null_igual_zero,
    "multiplos_codigos": normalizar_codigos_multiplos,
}

comparadores = {
    "default": comparador_default,
    "case_insensitive": comparador_case_insensitive,
    "ie_equivalente": comparador_ie_equivalente,
    "data_sem_hora": comparador_data_sem_hora,
    "bool_sql_ativo": comparador_bool_sql_ativo,
    "bool_sql_peso_variavel": comparador_bool_sql_peso_variavel,
    "data_inativacao_logica": comparador_data_inativacao_logica,
    "numero_string": comparador_numero_string,
    "numero_null_igual_zero": comparador_numero_null_igual_zero,
    "numero_decimal_aproximado_2": comparador_numero_null_igual_zero,
    "numero_decimal_aproximado_3": comparador_numero_null_igual_zero,
    "decimal_tolerancia": comparador_decimal_tolerancia,
    "multiplos_codigos_todos_presentes": comparador_multiplos_codigos_todos_presentes,
}

# --- Funções auxiliares ---
def aplicar_normalizadores(val, tipo):
    if tipo in normalizadores:
        return normalizadores[tipo](val)
    return val

def obter_valor_origem(linha_origem, regra_campo):
    if "condicao" in regra_campo:
        for cond_coluna, valores in regra_campo["condicao"].items():
            valor_cond = linha_origem.get(cond_coluna)
            if valor_cond in valores:
                col_origem = valores[valor_cond]
                return linha_origem.get(col_origem)
        return None
    else:
        source_col = regra_campo.get("source_column")
        if isinstance(source_col, list):
            for col in source_col:
                val = linha_origem.get(col)
                if val not in [None, "", pd.NA]:
                    return val
            return None
        else:
            return linha_origem.get(source_col)

# --- Lógica principal ---
def aplicar_regras(df_destino: pd.DataFrame, df_origem: pd.DataFrame, regras: dict) -> pd.DataFrame:
    registros = []
    pk_destino, pk_origem = regras["primary_key"]
    campos = regras["campos"]
    ordem_campos = list(campos.keys())

    for linha_dest in df_destino.itertuples(index=False):
        chave = getattr(linha_dest, pk_destino)
        linha_ori_df = df_origem[df_origem[pk_origem] == chave]

        if linha_ori_df.empty:
            for campo_dest in ordem_campos:
                registros.append({
                    "CODIGO": chave,
                    "CAMPO": campo_dest.replace("DF", ""),
                    "SQL": getattr(linha_dest, campo_dest, None),
                    "ORIGEM": "NÃO ENCONTRADO",
                    "STATUS": "Faltando na origem"
                })
            continue

        linha_ori = linha_ori_df.iloc[0]

        for campo_dest in ordem_campos:
            regra_campo = campos[campo_dest]
            val_sql = getattr(linha_dest, campo_dest, None)
            val_ori = obter_valor_origem(linha_ori, regra_campo)
            comparador_valor = regra_campo.get("comparador", "default")
            normalizar_flag = regra_campo.get("normalizar", False)

            comparadores_usados = (
                [c.strip() for c in comparador_valor.split(",")]
                if isinstance(comparador_valor, str)
                else [comparador_valor]
            )

            if normalizar_flag is True:
                comp_primeiro = comparadores_usados[0]
                normalizador_tipo = normalizador_padrao_por_comparador.get(comp_primeiro, None)
                if normalizador_tipo:
                    val_sql = aplicar_normalizadores(val_sql, normalizador_tipo)
                    val_ori = aplicar_normalizadores(val_ori, normalizador_tipo)

            ok = False
            for comp_nome in comparadores_usados:
                comp_func = comparadores.get(comp_nome)
                if comp_func:
                    if comp_nome == "data_inativacao_logica":
                        resultado = comp_func(val_sql, linha_ori.get("ATIVO"))
                    else:
                        resultado = comp_func(val_sql, val_ori)
                    if isinstance(resultado, pd.Series):
                        ok = resultado.all()
                    else:
                        ok = bool(resultado)
                    if ok:
                        break

            registros.append({
                "CODIGO": chave,
                "CAMPO": campo_dest.replace("DF", ""),
                "SQL": val_sql,
                "ORIGEM": val_ori,
                "STATUS": "OK" if ok else "Divergente"
            })

    return pd.DataFrame(registros, columns=["CODIGO", "CAMPO", "SQL", "ORIGEM", "STATUS"])

def aplicar_regras_central(conn_source, conn_target, regras: dict) -> pd.DataFrame:
    if "tabelas" not in regras:
        col_src, dados_src = conn_source.fetch_data(regras["source_query"])
        col_dst, dados_dst = conn_target.fetch_data(regras["target_query"])
        df_src = pd.DataFrame(dados_src, columns=col_src)
        df_dst = pd.DataFrame(dados_dst, columns=col_dst)
        return aplicar_regras(df_dst, df_src, regras)

    resultados = []
    for _, regras_tab in regras["tabelas"].items():
        col_src, dados_src = conn_source.fetch_data(regras_tab["source_query"])
        col_dst, dados_dst = conn_target.fetch_data(regras_tab["target_query"])
        df_src = pd.DataFrame(dados_src, columns=col_src)
        df_dst = pd.DataFrame(dados_dst, columns=col_dst)
        parcial = aplicar_regras(df_dst, df_src, regras_tab)
        resultados.append(parcial)

    return pd.concat(resultados, ignore_index=True)
