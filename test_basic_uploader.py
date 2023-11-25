import pandas as pd
import dask.dataframe as dd
import pytest
import config
import os

from basic_uploader import BaseEngine, UploaderPD, UploaderDASK


def create_test_csv(csv_file):

    test_case_csv = """PointOfSale; Product; Date; Stock
121017; 17240503103734; 2019-08-17; 2
121017; 17240503103734; 2019-08-18; 2
121017; 17240503103734; 2019-08-19; 2
121017; 17240503103734; 2019-08-20; 2
121017; 17240503103734; 2019-08-21; 2
121017; 17240503103734; 2019-08-22; 2
121017; 17240503103734; 2019-07-27; 1
121017; 17240503103734; 2019-07-28; 1
121017; 17240503103734; 2019-07-29; 1"""

    text_file = open(csv_file, "w")
    text_file.write(test_case_csv)
    text_file.close()


def test_pg_connection():
    pg_config = config.db_conf
    engine = BaseEngine({}, pg_config)
    res_txt = engine.exec_sql('SELECT 1', True)
    assert res_txt[0] == (1,)


def test_create_table():
    pg_config = config.db_conf
    pg_type = {
        "PointOfSale": "CHAR(50)",
        "Product": "CHAR(50)",
        "Date": "DATE",
        "Stock": "INT"
    }
    sql_show_tables = """
    SELECT * FROM pg_catalog.pg_tables 
    where schemaname != 'pgagent' and schemaname != 'pg_catalog' and schemaname != 'information_schema';
    """
    engine = BaseEngine({}, pg_config)
    engine.creat_table('test_case', pg_type)
    res_txt = engine.exec_sql(sql_show_tables, True)
    table_list = [i[1] for i in res_txt]
    assert 'test_case' in table_list


def test_drop_table():
    pg_config = config.db_conf
    sql_show_tables = """
    SELECT * FROM pg_catalog.pg_tables 
    where schemaname != 'pgagent' and schemaname != 'pg_catalog' and schemaname != 'information_schema';
    """
    test_create_table()
    engine = BaseEngine({}, pg_config)
    engine.drop_table('test_case')
    res_txt = engine.exec_sql(sql_show_tables, True)
    table_list = [i[1] for i in res_txt]
    assert 'test_case' not in table_list


def test_read_csv_pandas():
    csv_file = "test_case.CSV"
    create_test_csv(csv_file)
    csv_conf = config.csv_conf.get(os.path.basename(csv_file), None)
    pg_config = config.db_conf

    df1 = pd.read_csv("test_case.CSV", sep='; ', dtype={'PointOfSale': str,
                                                        'Product': str,
                                                        'Date': str,
                                                        'Stock': int})
    engine = UploaderPD(csv_conf=csv_conf, pg_config=pg_config)
    df_iter = engine.load_csv(csv_file, csv_conf, 100)
    df2 = [df_chunk for df_chunk in df_iter][0]

    assert df2.equals(df1)
    os.remove(csv_file)


def test_read_csv_dask():
    csv_file = "test_case.CSV"
    create_test_csv(csv_file)

    csv_conf = config.csv_conf.get(os.path.basename(csv_file), None)
    pg_config = config.db_conf

    df1 = dd.read_csv("test_case.CSV", sep='; ', dtype={'PointOfSale': str,
                                                        'Product': str,
                                                        'Date': str,
                                                        'Stock': int})

    engine = UploaderDASK(csv_conf=csv_conf, pg_config=pg_config)
    df2 = engine.load_csv(csv_file, csv_conf)
    print(df2.eq(df1))
    assert len(df2) == len(df1)
    os.remove(csv_file)


def test_write_csv_pandas():
    csv_file = "test_case.CSV"
    create_test_csv(csv_file)
    csv_conf = config.csv_conf.get(os.path.basename(csv_file), None)
    pg_config = config.db_conf
    table_name = csv_conf.get('table_name')
    table_col = csv_conf.get('pg_type')

    df1 = pd.read_csv("test_case.CSV", sep='; ', dtype={'PointOfSale': str,
                                                        'Product': str,
                                                        'Date': str,
                                                        'Stock': int})

    engine = UploaderPD(csv_conf=csv_conf, pg_config=pg_config)
    df_iter = engine.load_csv(csv_file, csv_conf, 100)
    result = engine.upload(df_iter, table_name, table_col)
    assert result

    select_sql = f"select count(1) from {table_name}"
    res_text = engine.exec_sql(select_sql, True)
    assert res_text[0][0] == len(df1)
    os.remove(csv_file)


def test_write_csv_dask():
    csv_file = "test_case.CSV"
    create_test_csv(csv_file)
    csv_conf = config.csv_conf.get(os.path.basename(csv_file), None)
    pg_config = config.db_conf
    table_name = csv_conf.get('table_name')
    sa_dtype = csv_conf.get('sa_dtype')

    df1 = pd.read_csv("test_case.CSV", sep='; ', dtype={'PointOfSale': str,
                                                        'Product': str,
                                                        'Date': str,
                                                        'Stock': int})
    engine = UploaderDASK(csv_conf=csv_conf, pg_config=pg_config)
    df = engine.load_csv(csv_file, csv_conf)
    result = engine.upload(df, table_name, sa_dtype)
    assert result

    select_sql = f"select count(1) from {table_name}"
    res_text = engine.exec_sql(select_sql, True)
    assert res_text[0][0] == len(df1)
    os.remove(csv_file)


if __name__ == '__main__':
    pass
