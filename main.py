# Main Path
import os.path
import zipfile
from basic_uploader import *
import config
import logging

# set log config
logger = logging.getLogger(__name__)
logging.basicConfig(filename='upload.log',
                    level=logging.DEBUG,
                    format='%(asctime)s %(name)s[line:%(lineno)d] %(levelname)s: %(message)s'
                    )
# show log on screen
console = logging.StreamHandler()
console.setLevel(logging.INFO)
console_format = logging.Formatter('%(asctime)s %(name)s[line:%(lineno)d] %(levelname)s: %(message)s')
console.setFormatter(console_format)
logging.getLogger().addHandler(console)


def unzip_file() -> None:
    zip_path = config.input_conf.get('zip_path', None)
    if not os.path.exists(zip_path):
        # download_file()
        logger.error(f"can't find zip file from given path {zip_path}")
        return
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall('./')


def upload_stock():
    csv_file = config.input_conf.get('file_path')
    if not os.path.exists(csv_file):
        logger.error(f"path not exists:: {csv_file}")
        return
    engine = config.input_conf.get('engine', None)
    csv_conf = config.csv_conf.get(os.path.basename(csv_file), None)
    pg_config = config.db_conf

    if engine == "pandas":
        logger.info(f"start uploading by Pandas")
        upload_engine = UploaderPD(csv_conf=csv_conf, pg_config=pg_config)
        upload_engine.run()
    elif engine == "dask":
        logger.info(f"start uploading by DASK")
        upload_engine = UploaderDASK(csv_conf=csv_conf, pg_config=pg_config)
        upload_engine.run()
    else:
        logger.error("config.input_conf['engine'] setting wrong!")


if __name__ == '__main__':
    unzip_file()
    upload_stock()

