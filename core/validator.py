import pandas as pd
from .normalizadores import (
    normalizar_valor,
    normalizar_ie,
    normalizar_case_insensitive,
    normalizar_numero,
    normalizar_data_sem_hora,
    normalizar_bool_sql_ativo,
    normalizar_bool_sql_peso_variavel,
    normalizar_numero_null_igual_zero,
    normalizar_codigos_multiplos,
)
from .comparadores import comparadores, normalizador_padrao_por_comparador

# --- Registro de normalizadores ---
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

# --- Aplicadores ---
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

# --- Regra campo a campo ---
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

# --- Função principal usada nos pipelines ---
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
