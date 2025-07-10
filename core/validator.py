import pandas as pd
import logging
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

# Logger centralizado
logger = logging.getLogger(__name__)

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
    try:
        if tipo in normalizadores:
            return normalizadores[tipo](val)
        return val
    except Exception as e:
        logger.error(f"Erro ao aplicar normalizador '{tipo}' no valor '{val}': {str(e)}")
        return val

def obter_valor_origem(linha_origem, regra_campo):
    try:
        if "condicao" in regra_campo:
            for cond_coluna, valores in regra_campo["condicao"].items():
                valor_cond = linha_origem.get(cond_coluna)
                if valor_cond in valores:
                    col_origem = valores[valor_cond]
                    return linha_origem.get(col_origem)
            return None
        source_col = regra_campo.get("source_column")
        if isinstance(source_col, list):
            for col in source_col:
                val = linha_origem.get(col)
                if val not in [None, "", pd.NA]:
                    return val
            return None
        return linha_origem.get(source_col)
    except Exception as e:
        logger.error(f"Erro ao obter valor da origem: {str(e)}")
        return None

# --- Regra campo a campo ---
def aplicar_regras(df_destino: pd.DataFrame, df_origem: pd.DataFrame, regras: dict) -> pd.DataFrame:
    logger.info(f"Iniciando validação - Registros destino: {len(df_destino)}, origem: {len(df_origem)}")
    
    registros = []
    pk_destino, pk_origem = regras["primary_key"]
    campos = regras["campos"]
    ordem_campos = list(campos.keys())
    
    logger.info(f"Validando {len(ordem_campos)} campos: {ordem_campos}")
    total_registros = len(df_destino)

    for i, linha_dest in enumerate(df_destino.itertuples(index=False), start=1):
        try:
            chave = getattr(linha_dest, pk_destino)
            linha_ori_df = df_origem[df_origem[pk_origem] == chave]

            if linha_ori_df.empty:
                logger.warning(f"Registro {chave} não encontrado na origem")
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
                try:
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
                        try:
                            comp_func = comparadores.get(comp_nome)
                            if comp_func:
                                if comp_nome == "data_inativacao_logica":
                                    resultado = comp_func(val_sql, linha_ori.get("ATIVO"))
                                else:
                                    resultado = comp_func(val_sql, val_ori)
                                ok = resultado.all() if isinstance(resultado, pd.Series) else bool(resultado)
                                if ok:
                                    break
                        except Exception as e:
                            logger.error(f"Erro no comparador '{comp_nome}' para campo '{campo_dest}', registro {chave}: {str(e)}")
                            ok = False

                    status = "OK" if ok else "Divergente"
                    if not ok:
                        logger.debug(f"Divergência - Registro: {chave}, Campo: {campo_dest}, SQL: {val_sql}, Origem: {val_ori}")

                    registros.append({
                        "CODIGO": chave,
                        "CAMPO": campo_dest.replace("DF", ""),
                        "SQL": val_sql,
                        "ORIGEM": val_ori,
                        "STATUS": status
                    })

                except Exception as e:
                    logger.error(f"Erro ao processar campo '{campo_dest}' do registro {chave}: {str(e)}")
                    registros.append({
                        "CODIGO": chave,
                        "CAMPO": campo_dest.replace("DF", ""),
                        "SQL": "ERRO",
                        "ORIGEM": "ERRO",
                        "STATUS": "Erro no processamento"
                    })

            if i % 100 == 0:
                logger.info(f"Progresso: {i}/{total_registros} registros processados")

        except Exception as e:
            logger.error(f"Erro crítico ao processar registro: {str(e)}")
            continue

    logger.info(f"Validação concluída - {len(df_destino)} registros processados")
    
    df_resultado = pd.DataFrame(registros, columns=["CODIGO", "CAMPO", "SQL", "ORIGEM", "STATUS"])
    total_validacoes = len(df_resultado)
    divergencias = len(df_resultado[df_resultado["STATUS"] == "Divergente"])
    erros = len(df_resultado[df_resultado["STATUS"].str.contains("Erro|Faltando", na=False)])

    logger.info(f"RESULTADO - Total: {total_validacoes}, OK: {total_validacoes - divergencias - erros}, Divergências: {divergencias}, Erros: {erros}")
    return df_resultado

# --- Função principal usada nos pipelines ---
def aplicar_regras_central(conn_source, conn_target, regras: dict) -> pd.DataFrame:
    logger.info("Iniciando validação central")
    
    try:
        if "tabelas" not in regras:
            logger.info("Executando validação para uma única tabela")
            col_src, dados_src = conn_source.fetch_data(regras["source_query"])
            col_dst, dados_dst = conn_target.fetch_data(regras["target_query"])
            df_src = pd.DataFrame(dados_src, columns=col_src)
            df_dst = pd.DataFrame(dados_dst, columns=col_dst)
            return aplicar_regras(df_dst, df_src, regras)

        logger.info(f"Executando validação para {len(regras['tabelas'])} tabelas")
        resultados = []

        for nome_tabela, regras_tab in regras["tabelas"].items():
            try:
                logger.info(f"Processando tabela: {nome_tabela}")
                col_src, dados_src = conn_source.fetch_data(regras_tab["source_query"])
                col_dst, dados_dst = conn_target.fetch_data(regras_tab["target_query"])
                df_src = pd.DataFrame(dados_src, columns=col_src)
                df_dst = pd.DataFrame(dados_dst, columns=col_dst)
                parcial = aplicar_regras(df_dst, df_src, regras_tab)
                resultados.append(parcial)
                logger.info(f"Tabela {nome_tabela} processada com sucesso")
            except Exception as e:
                logger.error(f"Erro ao processar tabela {nome_tabela}: {str(e)}")
                continue

        if resultados:
            return pd.concat(resultados, ignore_index=True)
        else:
            logger.error("Nenhuma tabela foi processada com sucesso")
            return pd.DataFrame(columns=["CODIGO", "CAMPO", "SQL", "ORIGEM", "STATUS"])
        
    except Exception as e:
        logger.error(f"Erro crítico na validação central: {str(e)}")
        raise
