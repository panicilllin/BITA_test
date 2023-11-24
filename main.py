# Main Path
import os.path
import zipfile
from basic_uploader import UploaderPD
import config
import logging

# set log config
logger = logging.getLogger(__name__)
logging.basicConfig(filename='upload.log',
                    level=logging.INFO,
                    format='%(asctime)s %(name)s[line:%(lineno)d] %(levelname)s: %(message)s'
                    )
# show log on screen
console = logging.StreamHandler()
console.setLevel(logging.INFO)
console_format = logging.Formatter('%(asctime)s %(name)s[line:%(lineno)d] %(levelname)s: %(message)s')
console.setFormatter(console_format)
logging.getLogger().addHandler(console)


# def download_file() -> None:
#     import gdown
#     url = 'https://drive.google.com/file/d/1cHPG8mE__kCAIQqYOQ0vaShO09Jvyqp7'
#     output = 'Stock.zip'
#     gdown.download(url, output, quiet=False)


def unzip_file() -> None:
    zip_path = config.input_conf.get('zip_path', None)
    if not os.path.exists(zip_path):
        # download_file()
        logger.error(f"can't find zip file from given path {zip_path}")
        return
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall('./')


if __name__ == '__main__':
    unzip_file()
    logger.info(f"start uploading")
    upload_engine = UploaderPD()
    load_iter = upload_engine.load_batch(config.input_conf['batch'])
    upload_result = upload_engine.upload(load_iter, 'stock')
    logger.info(f"upload result:{upload_result}")

