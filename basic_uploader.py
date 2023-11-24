import os.path
from typing import *
import time
import psycopg2 as pg
import pandas as pd
import config
from utils import Retry
import logging

logger = logging.getLogger(__name__)


class BasicEngine:
    def __init__(self):
        self.csv_file = config.input_conf['file_path']
        self.pg_config = config.output_conf
        self.conn = self.connect_pg()

    def connect_pg(self):
        connection = pg.connect(host=self.pg_config.get('Server'),
                                database=self.pg_config.get('Database'),
                                port=self.pg_config.get('Port'),
                                user=self.pg_config.get('Username'),
                                password=self.pg_config.get('Password'))
        return connection

    def creat_table(self, table_name: str) -> bool:
        sql_drop_table = f'''DROP TABLE IF EXISTS {table_name}'''
        sql_creat_table = f'''CREATE TABLE {table_name}(
                        PointOfSale CHAR(50) NOT NULL, 
                        Product CHAR(50) NOT NULL, 
                        Date DATE NOT NULL, 
                        Stock INT NOT NULL
                        )'''
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql_drop_table)
            cursor.execute(sql_creat_table)
            cursor.close()
            return True
        except Exception as e:
            logger.info(e)
            return False


class UploaderPD(BasicEngine):
    def __init__(self):
        super().__init__()

    def load_batch(self, batch_size: int) -> Generator:
        if not os.path.exists(self.csv_file):
            raise
        chunk_num = 0
        for chunk in pd.read_csv(self.csv_file,
                                 sep=';',
                                 chunksize=batch_size,
                                 dtype={'PointOfSale': str,
                                        'Product': str,
                                        'Date': str,
                                        'Stock': int}):
            chunk_num += 1
            yield chunk, chunk_num

    def upload(self, chunk: Generator, table_name: str) -> bool:
        self.creat_table(table_name)
        total_start_time = time.time()
        for df, idx in chunk:
            start_time = time.time()
            execute_result = self.insert_to_pg(df, table_name)
            logger.info(f"{idx} len {len(df)} res {execute_result}, cost {time.time() - start_time}")
        logger.info(f"total cost: {time.time() - total_start_time}s")
        return True

    @Retry(3)
    def insert_to_pg(self, df: pd.DataFrame, table_name: str) -> bool:
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
            print("Error: %s" % error)
            self.conn.rollback()
            cursor.close()
            return False


if __name__ == "__main__":
    a = UploaderPD()
    b = a.load_batch(1e6)
    print(b)
    c = a.upload(b, 'stock')
