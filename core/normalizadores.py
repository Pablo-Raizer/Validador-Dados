import re
import unicodedata
import pandas as pd
from decimal import Decimal
from datetime import datetime

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
