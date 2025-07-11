from decimal import Decimal
import pandas as pd
import re
from .normalizadores import normalizar_numero

def comparador_default(val_sql, val_ori, **_):
    return val_sql == val_ori

def comparador_case_insensitive(val_sql, val_ori, **_):
    return str(val_sql).strip().lower() == str(val_ori).strip().lower()

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
    
def comparador_aliquota_decimal_tolerante(val_sql, val_ori, **_):
    try:
        return abs(val_sql - val_ori) <= Decimal("0.01")
    except:
        return False

# Dicionário de comparadores disponíveis
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
    "aliquota_decimal_tolerante": comparador_aliquota_decimal_tolerante,
}

# Mapeamento comparador -> normalizador padrão
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
    "aliquota_decimal_tolerante": "aliquota_percentual",
}
