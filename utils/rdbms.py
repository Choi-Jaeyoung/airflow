import pandas as pd


def get_pandas_from_mysql(mysql, sql, chunksize=10000):

    conn = mysql.get_sqlalchemy_engine().connect().execution_options(
        stream_results=True)
    
    df = pd.DataFrame()
    
    for chunk_dataframe in pd.read_sql(sql, conn, chunksize=chunksize):
        df = pd.concat([df, chunk_dataframe], axis=0, ignore_index=True)
    
    return df


def get_records_from_mysql(mysql, sql):

    conn = mysql.get_sqlalchemy_engine().connect().execution_options(
        stream_results=True)

    results = conn.execute(sql)

    return results.fetchall()  # [(100, 'Joe'), (192, 'Jane')]
