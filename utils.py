import os
import traceback
import functools
import zipfile
from typing import *
import time
import logging

logger = logging.getLogger(__name__)


def unzip_file(zip_path) -> None:

    if not os.path.exists(zip_path):
        # download_file()
        logger.error(f"can't find zip file from given path {zip_path}")
        return
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall('./')


class Retry:

    def __init__(self, tries: int = -1, delay: int = 0) -> None:
        """
        retry decorator
        usage:
            @Retry()
            def func():...
        @param tries: number of retry
        @param delay: delay between two times
        """
        self.tries = tries
        self.delay = delay

    def __call__(self, func: Callable) -> Callable:
        """
        function be retried
        @param func: decorated func
        @return: retry func
        """

        @functools.wraps(func)
        def inner(*args, **kwargs):
            while self.tries:
                # noinspection PyBroadException
                try:
                    return func(*args, **kwargs)
                except Exception:
                    self.tries -= 1
                    if self.tries == 0:
                        raise
                    logger.info(f'{traceback.format_exc()}, retrying in {self.delay} seconds...')
                    time.sleep(self.delay)

        return inner

