import pandas as pd
import sqlalchemy as sa
import logging
import csv

from . import db_connection


##############################################################
# READ ON ORACLE
##############################################################
def read_df_from_query(uri_connection, query):
    """ Wrapper around pandas.read_sql

    The query is run against the default database seelcted.

    Parameters
    ----------
    query: str
        SQL query as a string (note: multi-line triple-quoted strings are ok)
    uri_connection: str
        uri for connection

    Returns
    -------
    df: pd.DataFrame
    """
    with db_connection.connect(uri=uri_connection) as connection:
        return pd.read_sql(query, connection)


def create_csv_from_query(filename, uri_connection, query):
    """ Save result of a query as a csv file

    Parameters
    ----------
    filename: str
        path to the csv file that will be created
    uri_connection: str
        uri for connection
    query: str
        SQL query
    """
    # https://stackoverflow.com/questions/2952366/dump-csv-from-sqlalchemy
    with open(filename, 'w') as outfile:
        outcsv = csv.writer(outfile)
        with db_connection.connect(uri=uri_connection) as connection:
            cursor = connection.execute(query)
            # dump column titles (optional)
            outcsv.writerow(cursor.keys())
            # dump rows
            for row in cursor:
                outcsv.writerow(row)
    logging.info('End saving file in : {}'.format(filename))

    return filename


##############################################################
# WRITE ON ORACLE
##############################################################
def df_to_oracle(df, table_name, uri, if_exists='replace'):
    """ Insert a dataframe into oracle

    Parameters
    ----------
    df: pd.Dataframe
        dataframe to be save
    table_name: str
        name of SQL table
    uri: str
        uri for connection into database
    if_exists: str
        how to behave if the table already exists: replace, fails, append
    """
    conn = db_connection.connect(uri)
    df.to_sql(name=table_name, con=conn, if_exists=if_exists,
              index=False, chunksize=10000,
              dtype=make_dtype_dict(df))


def make_dtype_dict(df):
    """ Returns a dictionnary with max length of strings for object type columns.

    Parameters
    ----------
    df: pd.Dataframe
        a specific Dataframe

    Returns
    -------
    dict: dictionnary
        {cols : VARCHART(max_length in the series)} for object type columns in df
    """
    # Create argument dtype of pd.DataFrame.to_sql() function
    # https://pandas.pydata.org/pandas-docs/version/0.22/generated/pandas.DataFrame.to_sql.html
    # This is required because for Oracle database, pandas default the type
    # 'object' to 'clob' which is super heavy. See:
    # https://stackoverflow.com/questions/42727990/speed-up-to-sql-when-writing-pandas-dataframe-to-oracle-database-using-sqlalch
    # https://docs.oracle.com/javadb/10.10.1.2/ref/rrefclob.html
    return {
        col: sa.types.VARCHAR(max_length(df[col]))
        for col, dtype in df.dtypes.astype(str).to_dict().items()
        if dtype == 'object'
    }


def max_length(series):
    """ Returns the length of the longest longest in the series
    Parameters
    ----------
    series: pd.Series
        a specific Series
    Returns
    -------
    maxi: int
        length of the longest element in the series
    """
    maxi = series.str.len().max()
    if maxi and not pd.isnull(maxi):
        return int(maxi)
    return 1
