def build_download_query(from_table, columns_to_select, join_tables,
                         nb_days, date_col, cond='IN', sampling=None):
    from_statement = build_from_statement(from_table, join_tables)
    where_statement = build_where_statement(sampling, date_col, nb_days, cond)
    if columns_to_select:
        cols = ", ".join([c for c in columns_to_select])
        query = "(SELECT {} FROM {} WHERE {})".format(cols, from_statement,
                                                      where_statement)
    else:
        query = "SELECT * FROM {} WHERE {}".format(from_table, where_statement)
    return query


def build_from_statement(from_table, join_tables):
    """from_table: first table
       join_tables: junction tables
    """
    if join_tables:
        tables = [key for key, _ in join_tables.items()]
        keys = [value for _, value in join_tables.items()]
        join_statement = '{0} LEFT JOIN {1} ON {1}.{2} = {0}.{2} '.format(from_table,
                                                                          tables[0],
                                                                          keys[0])
        for i in range(1, len(join_tables)):
            join_statement += 'LEFT JOIN {1} ON {1}.{2} = {0}.{2} '.format(tables[i-1],
                                                                           tables[i],
                                                                           keys[i])

    else:
        join_statement = '{}'.format(from_table)

    return join_statement


def build_where_statement(sampling, date_col, nb_days, cond='IN'):
    """build a statement to set a build_download_query
       Inputs: sampling: a dict like SAMPLING = {
            "DATE_EMON": TODAY,
            'BRC12.ODSQTJDD_CODELIGNE.CODE_LIGNE': ('B7', 'B8', 'W2')}
             date_col: a date column for filtering
             nb_days: numbers of from to consider in the query.
    """
    if sampling is not None:
        return "AND ".join(
            ["{} {} {} ".format(column, cond, value)
             for column, value in sampling.items() if date_col not in column]
            + ["{} >= TO_DATE('{}', 'dd/mm/yy') - {}".format(column,
                                                             value, nb_days)
               for column, value in sampling.items() if date_col in column]
        )
    else:
        return ''
