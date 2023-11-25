import os.path
from typing import *
import time
import psycopg2 as pg
import pandas as pd
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
import sqlalchemy as sa
from utils import Retry
import logging

logger = logging.getLogger(__name__)


class BaseEngine:
    """
    Base class for all other class
    put all the commen function here
    """

    def __init__(self, csv_conf, pg_config):
        self.csv_file = csv_conf.get('file_path')
        self.csv_conf = csv_conf
        self.batch_size = csv_conf.get('batch', 1e6)
        self.pg_config = pg_config
        self.conn = self.connect_pg()

    def connect_pg(self):
        """
        simple function to connect postgres
        in case I need to reconnect
        :return: psycopg2.connection()
        """
        connection = pg.connect(host=self.pg_config.get('Server'),
                                database=self.pg_config.get('Database'),
                                port=self.pg_config.get('Port'),
                                user=self.pg_config.get('Username'),
                                password=self.pg_config.get('Password'))
        return connection

    @Retry(3)
    def drop_table(self, table_name: str) -> bool:
        """
        Simple function to drop postgres table
        retry three times when fail
        :param table_name: target table name
        :return: true or false
        """
        sql_drop_table = f'''DROP TABLE IF EXISTS {table_name}'''
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql_drop_table)
            self.conn.commit()
            logger.info(f"table {table_name} dropped")
            cursor.close()
            return True
        except Exception as e:
            logger.info(e)
            return False

    def creat_table_demo(self, table_name: str, cols: list):
        sql_creat_table = f"CREATE TABLE {table_name}("
        for key, val in cols.items():
            sql_creat_table += f"{key} {val}"
            sql_creat_table += ", "
        sql_creat_table = sql_creat_table[:-2] + ")"
        return sql_creat_table

    def creat_table(self, table_name: str, cols: list) -> bool:
        """
        create postgres table
        drop table before create it
        :param table_name: target table
        :param cols: col name and type of this table
        :return: true or false
        """

        sql_creat_table = f"CREATE TABLE {table_name}("
        for key, val in cols.items():
            sql_creat_table += f"{key} {val}"
            sql_creat_table += ", "
        sql_creat_table = sql_creat_table[:-2] + ")"

        self.drop_table(table_name)
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql_creat_table)
            self.conn.commit()
            cursor.close()
            return True
        except Exception as e:
            logger.info(e)
            return False

    def exec_sql(self, sql, fetch_res=False):

        try:
            cursor = self.conn.cursor()
            cursor.execute(sql)
            if fetch_res:
                res_txt = cursor.fetchall()
                cursor.close()
                return res_txt
            else:
                cursor.close()
                return True
        except Exception as e:
            logger.info(e)
            return False

    def load_csv(self):
        pass

    def upload(self):
        pass

    def run(self):
        pass


class UploaderPD(BaseEngine):
    """
    Using python as the process lib
    """

    def __init__(self, csv_conf, pg_config):
        super().__init__(csv_conf, pg_config)
        self.csv_sep = self.csv_conf['sep']
        self.csv_dtype = self.csv_conf['dtype']

    def load_csv(self, csv_file: str = None, csv_conf={}) -> Generator:
        """
        load csv to pandas from disk by chunk
        :param csv_file: csv file path
        :param csv_conf: dict contain dtype and seperator of the csv
        :return: return a generator, you can useing for loop to read each chunk
        """
        if csv_conf is None:
            csv_conf = {}
        if not os.path.exists(csv_file):
            logger.error(f"path {csv_file} not exist")
            raise
        read_conf = {}
        if self.csv_conf.get('sep'):
            read_conf['sep'] = self.csv_conf['sep']
        if self.csv_conf.get('dtype'):
            read_conf['dtype'] = self.csv_conf['dtype']
        logger.debug(f"batch_size={self.batch_size}")
        for chunk in pd.read_csv(csv_file, chunksize=self.batch_size, **read_conf):
            yield chunk

    def upload(self, chunk: Generator, table_name: str, table_col: dict) -> bool:
        """
        upload function, using psycopg2 do batch upload
        It's mainly a loop call,for each batch df call the insert_to_pg to upload
        :param chunk: generator, you can use it to get each chunk of the dataframe
        :param table_name: target table name
        :return: boolen, if the process success
        """
        logger.info("start upload by pandas")

        self.creat_table(table_name, table_col)
        total_start_time = time.time()
        idx = 0
        for df in chunk:
            logger.debug(f"col={df.columns}")
            logger.debug(f"dtype={df.columns.to_series().groupby(df.dtypes).groups}")
            logger.debug(f"len={len(df)}")
            idx += 1
            start_time = time.time()
            execute_result = self.insert_to_pg(df, table_name)
            logger.info(f"upload {idx} res {execute_result}, cost {time.time() - start_time}")
        logger.info(f"upload total cost: {time.time() - total_start_time}s")
        return True

    @Retry(3)
    def insert_to_pg(self, df: pd.DataFrame, table_name: str) -> bool:
        """
        insert dataframe to postgres
        seperate it from upload function because I can put retry on each upload batch
        :param df: pandas dataframe
        :param table_name: target table name
        :return: if this insert success
        """
        cursor = self.conn.cursor()
        tuples = [tuple(x) for x in df.to_numpy()]
        # Comma-separated dataframe columns
        cols = ','.join(list(df.columns))
        # SQL quert to execute
        values = [cursor.mogrify("(%s,%s,%s,%s)", tup).decode('utf8') for tup in tuples]
        query = "INSERT INTO %s(%s) VALUES " % (table_name, cols) + ",".join(values)
        try:
            cursor.execute(query, tuples)
            self.conn.commit()
            return True
        except (Exception, pg.DatabaseError) as error:
            logger.error("Error: %s" % error)
            self.conn.rollback()
            cursor.close()
            return False

    def insert_tosql(self, df: pd.DataFrame, table_name: str) -> bool:
        """
        using pandas to_sql function to insert
            It cause more time than the psycopg2 batch insert.
            retry will add dupicate data, so I didn't add retry func
        :param df: pandas dataframe
        :param table_name: target table name
        :return: boolen, if the process success
        """

        # can't apply fast_executemany=True for postgres in sqlalchemy
        conn_sa = sa.create_engine(
            f"postgresql://{self.pg_config.get('Username')}:{self.pg_config.get('Password')}@"
            f"{self.pg_config.get('Server')}:{self.pg_config.get('Port')}/{{self.pg_config.get('Database')}}"
        )
        df.to_sql(con=conn_sa, name=table_name, if_exists='append', index=False)
        return True

    def run(self):
        csv_file = self.csv_conf.get('file_path')
        table_name = self.csv_conf.get('table_name')
        table_col = self.csv_conf.get('pg_type')
        start_time = time.time()
        df_iter = self.load_csv(csv_file, self.csv_conf)
        result = self.upload(df_iter, table_name, table_col)
        finish_time = time.time()
        logger.info(f"all finished. result: {result}\n"
                    f"total time: {finish_time - start_time}")


class UploaderDASK(BaseEngine):
    """
    using dask as the process lib
    """

    def __init__(self, csv_conf, pg_config):
        super().__init__(csv_conf, pg_config)
        # from sqlalchemy import create_engine
        # engine = create_engine('postgresql+psycopg2://user:password@hostname/database_name')
        self.pg_uri = f"postgresql+psycopg2://" \
                      f"{self.pg_config.get('Username')}:{self.pg_config.get('Password')}@" \
                      f"{self.pg_config.get('Server')}:{self.pg_config.get('Port')}/{self.pg_config.get('Database')}"

    def load_csv(self, csv_file, csv_conf):
        """
        load csv to dask from disk
        :param csv_file: csv file path
        :param csv_conf: dict contain dtype and seperator of the csv
        :return: return a generator, you can useing for loop to read each chunk
        """
        if not os.path.exists(self.csv_file):
            logger.error(f"path not exists:: {csv_file}")
            raise

        read_conf = {}
        if self.csv_conf.get('sep'):
            read_conf['sep'] = self.csv_conf['sep']
        if self.csv_conf.get('dtype'):
            read_conf['dtype'] = self.csv_conf['dtype']
        df = dd.read_csv(self.csv_file,
                         blocksize=self.batch_size,
                         **read_conf)
        return df

    def upload(self, df, table_name, sa_dtype):
        logger.debug(df)
        pbar = ProgressBar()
        pbar.register()
        # self.creat_table(table_name)
        self.drop_table(table_name)
        logger.info(f"re_creating db success")
        df.to_sql(table_name, uri=self.pg_uri,
                  if_exists='append',
                  chunksize=1000000,
                  index=False, parallel=True,
                  dtype=sa_dtype)
        logger.info(f"upload finished")
        return True

    def run(self):
        csv_file = self.csv_conf.get('file_path')
        table_name = self.csv_conf.get('table_name')
        sa_dtype = self.csv_conf.get('sa_dtype')

        start_time = time.time()
        df = self.load_csv(csv_file, self.csv_conf)
        logger.info('load finished, now start uploading')
        load_time = time.time()
        result = self.upload(df, table_name, sa_dtype)
        finish_time = time.time()

        logger.info(f"all finished. result: {result}\n"
                    f"load csv time: {load_time - start_time};\n"
                    f"upload time: {finish_time - load_time};\n"
                    f"total time: {finish_time - start_time}")


if __name__ == "__main__":
    pass
