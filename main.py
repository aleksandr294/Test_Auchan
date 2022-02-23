import pickle
from datetime import datetime, timedelta
from multiprocessing import Pool
from urllib.parse import urlparse

import requests
from loguru import logger
from urlextract import URLExtract

status_code_urls_dict = {}
unshorten_urls_dict = {}


def formatter(record) -> str:
    if 'func' in record['extra'] and 'data' in record['extra']:
        return "{time:YYYY-MM-DD HH:mm:ss} | {name} | {level}  | Func={extra[func]} | Data={extra[data]} | {message}\n"
    elif 'func' in record['extra']:
        return "{time:YYYY-MM-DD HH:mm:ss} | {name} | {level} | Func={extra[func]} | {message}\n"
    else:
        return "{time:YYYY-MM-DD HH:mm:ss} | {name} | {level} | {message}\n"


logger.add("logs.log", retention=timedelta(minutes=20), format=formatter, level='DEBUG', backtrace=True,
           diagnose=True, rotation=timedelta(minutes=5))


def deserialization() -> str:
    logger.bind(func='deserialization').debug('Run func')
    with open('messages_to_parse (2).dat', 'rb') as f:
        try:
            data = pickle.load(f)
            data = ''.join(data)
            return data
        except Exception as ex:
            logger.bind(func='deserialization').exception(ex)


def parse(data: str) -> list:
    try:
        logger.bind(func='parse').debug('Run func')
        extr = URLExtract()
        chars = {"’", ",", "(", " "}
        extr.set_stop_chars_right(chars)
        if data is not None:
            urls = extr.find_urls(data)
            return urls
    except Exception as ex:
        logger.bind(func='parse').exception(ex)


def check_url(url: str) -> list:
    try:
        logger.bind(func='check_url').debug('Run func')
        resp = requests.head(url, timeout=2)
        return [url, resp.status_code, resp.headers.get('Location')]
    except requests.ConnectTimeout as ex:
        logger.bind(func='check_url').exception(ex)
        return [url, 522, None]
    except Exception as ex:
        logger.bind(func='check_url').exception(ex)


def add_data_to_dicts(list_values: list) -> None:
    try:
        logger.bind(func='add_data_to_dicts').debug('Run func')
        for item in list_values:
            status_code_urls_dict.update({item[0]: item[1]})
            if item[2] is not None and urlparse(item[0]).hostname != urlparse(item[2]).hostname:
                unshorten_urls_dict.update({item[0]: item[2]})
    except Exception as ex:
        logger.bind(func='add_data_to_dicts').exception(ex)


def write_info_to_file(program_execution_time: float,
                       len_status_code_urls_key: int,
                       len_unshorten_urls_key: int) -> None:
    with open('README.md', mode='w', encoding='utf-8') as file:
        text = f"""
Время выполнения програми: {program_execution_time}\n
Len ключей словаря проверки URLS: {len_status_code_urls_key}\n
Len ключей словаря сокращенных ссылок: {len_unshorten_urls_key}\n
Объяснение почему был использован модуль multiprocessing: Для паралельного выполнения задач, 
чтобы уменьшить время сбора информации по урлам.  
            """
        file.write(text)


if __name__ == '__main__':
    try:
        logger.debug('Run script')
        start_time = datetime.now()
        data = deserialization()
        urls = parse(data)
        if urls is not None:
            with Pool(2) as pool:
                res = pool.map(check_url, urls)
                add_data_to_dicts(res)

        time = datetime.now() - start_time
        write_info_to_file(time, len(status_code_urls_dict.keys()), len(unshorten_urls_dict.keys()))

    except Exception as ex:
        logger.exception(ex)
