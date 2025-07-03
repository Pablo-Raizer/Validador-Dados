def gerar_select_dinamico(base_table: str, campos: dict, pk_origem: str = "CHAVEPRO"):
    selecao = set()
    joins = {}

    base_alias = "p"
    selecao.add(f"{base_alias}.*")

    for campo_dest, regra in campos.items():
        coluna = regra.get("source_column")
        tabela = regra.get("source_table", base_table)
        join_key = regra.get("join_key", pk_origem)

        if not coluna or tabela == base_table:
            continue

        alias = tabela.lower()[0]  # Ex: CODBAR -> c
        selecao.add(f"{alias}.{coluna} AS {campo_dest}")

        if alias not in joins:
            joins[alias] = f"LEFT JOIN {tabela} {alias} ON {alias}.{join_key} = {base_alias}.{pk_origem}"

    sql = f"SELECT {', '.join(sorted(selecao))} FROM {base_table} {base_alias}\n"
    sql += "\n".join(joins.values())
    return sql.strip()
