import requests, pandas as pd
from time import sleep
from dataManipulations.databaseOperations import *
import json
from collections import namedtuple
from urllib.error import HTTPError

class DataDownloader:
    def __init__(self, config_path):
        downloader_settings = get_config(config_path, 'dataDownloader')[0]
        storage_settings = get_config(config_path, 'storage')[0]
        if downloader_settings is not None:
            self.headers, self.counter_id, fields_dict = downloader_settings.get('headers', None),\
                                                         downloader_settings.get('counter_id', None),\
                                                         downloader_settings.get('fields_dict', None)
            Field = namedtuple('Field', ['name', 'id'])
            self.fields_dict = {req_t: [Field(field.split(':')[-1], field) for field in fields]
                                for req_t, fields in fields_dict.items()}
        else:
            raise ValueError('Отсутствуют конфигурации для загрузки данных.')
        if storage_settings is not None:
            self.storage = Storage(storage_settings.get('host'),
                                   storage_settings.get('port'),
                                   collection_name='RawData')
        else:
            raise ValueError('Отсутствуют конфигурации для хранения данных.')
        if not self._auth_check():
            raise ValueError('Неверный ключ авторизации')

    def _auth_check(self):
        response = requests.get(f'https://api-metrika.yandex.net/management/v1/counter/{self.counter_id}/logrequests',
                                headers=self.headers)
        if response.status_code == 401:
            return False
        elif response.status_code == 200:
            return True
        logging.error(f'Неизвестная ошибка при попытке авторизоваться: {response.status_code}')
        return False

    def _get_requests_list(self):
        response = requests.get(f'https://api-metrika.yandex.net/management/v1/counter/{self.counter_id}/logrequests',
                                headers=self.headers)
        if response.status_code == 200:
            rec_dict = json.loads(response.content)
            requests_list = [req['request_id'] for req in rec_dict['requests']]
            return requests_list
        else:
            logging.error(f'Не удалось получить список запросов: {str(response.status_code)}')
            return None

    def _clean_requests(self, requests_list, n_retries=3):
        for req_id in requests_list:
            url2clean = f'https://api-metrika.yandex.net/management/v1/counter/{self.counter_id}/logrequest/{req_id}/clean'
            requests.post(url2clean, headers=self.headers)

        new_req_list = self._get_requests_list()
        if new_req_list and n_retries:
            logging.warning('Старые запросы очищены не полностью, повторяю попытку через 1 минуту.')
            sleep(60)
            self._clean_requests(new_req_list, n_retries=n_retries-1)

        elif new_req_list:
            logging.warning('Старые запросы очищены не полностью: возможны дальнейшие ошибки')

    def _check_request_type(self, request_type):
        if request_type not in ('visits', 'hits'):
            raise ValueError('Неизвестный тип запроса. Корректные значения: "visits", "hits".')

    def _is_request_possible(self, request_type, s_date, f_date):
        self._check_request_type(request_type)
        fields_str = ','.join([field.id for field in self.fields_dict[request_type]])
        url = f'https://api-metrika.yandex.net/management/v1/counter/{self.counter_id}/logrequests/evaluate?\
        date1={str(s_date)}&date2={str(f_date)}&fields={fields_str}&source={request_type}'
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            req_possibility = json.loads(response.content)['log_request_evaluation']['possible']
            max_possible_period = json.loads(response.content)['log_request_evaluation']['max_possible_day_quantity']
            if (f_date - s_date).days > max_possible_period:
                logging.error('Выбранный период превышает квоту: удостоверьтесь, что старые запросы очищены.')
            return req_possibility
        raise requests.exceptions.HTTPError(response)

    def _request_data(self, request_type, s_date, f_date):
        self._check_request_type(request_type)
        fields_str = ','.join([field.id for field in self.fields_dict[request_type]])
        url = f'https://api-metrika.yandex.net/management/v1/counter/{self.counter_id}/logrequests?\
        date1={str(s_date)}&date2={str(f_date)}&fields={fields_str}&source={request_type}'
        response = requests.post(url, headers=self.headers)
        if response.status_code == 200:
            req_id = json.loads(response.content)['log_request']['request_id']
            return req_id
        raise requests.exceptions.HTTPError(response)

    def _check_request(self, req_id):
        url = f'https://api-metrika.yandex.net/management/v1/counter/{self.counter_id}/logrequest/{req_id}'
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            req_info = json.loads(response.content)['log_request']
            return req_info
        raise requests.exceptions.HTTPError(response)

    def _download_data(self, request_type, req_id, n_parts):
        '''Принимает до json-файла с конфигурациями, ID запроса, объект logging.logging.
        Возвращает словарь с данными по запросу или None'''

        storage_is_empty = self.storage.clear_storage(request_type)
        if storage_is_empty:
            self._check_request_type(request_type)
            fields = [field.name for field in self.fields_dict[request_type]]
            name_pattern = f'{request_type}Part{{}}'
            for part in range(n_parts):
                response = requests.get(f'https://api-metrika.yandex.net/management/v1/counter/{self.counter_id}/logrequest/\
                {req_id}/part/{part}/download', headers=self.headers)
                if response.status_code == '<Response [200]>':
                    data = [x.split('\t') for x in response.content.decode('utf-8').split('\n')[:-1]]
                    df = pd.DataFrame(data[1:], columns=fields)
                    filename = name_pattern.format(part)
                    _ = self.storage.upload_file(df, filename, request_type)
                else:
                    logging.error(f'Не удалось скачать {str(part)} часть данных: {str(response)}. Изменения откатываются.')
                    storage_is_empty = self.storage.clear_storage(request_type)
                    if not storage_is_empty:
                        logging.error('Не удалось скачать откатить изменения, хранилище не очищено.')
                    raise requests.exceptions.HTTPError(response)
        else:
            return False
        return True

    def get_data(self, s_date, f_date, request_type):
        s_date, f_date = s_date.date(), f_date.date()
        logging.info('Сбор старых запросов.')
        req_list = self._get_requests_list()
        if req_list is not None:
            logging.info('Удаление старых запросов.')
            self._clean_requests(req_list)
        logging.info(f'Оценка возможности нового запроса по периоду: {str(s_date)} - {str(f_date)}.')
        req_is_possible = self._is_request_possible(request_type, s_date, f_date)
        if not req_is_possible:
            logging.error('Обработка запроса невозможна. Работа программы остановлена.')
        else:
            logging.info('Запрос данных.')
            req_id = self._request_data(request_type, s_date, f_date)
            logging.info('Ожидание готовности запроса.')
            sleep(60)
            check_request_result = self._check_request(req_id)
            req_status = check_request_result['status']
            n_retries = 72
            while req_status != 'processed' and n_retries > 0:
                sleep(100)
                check_request_result = self._check_request(req_id)
                req_status = check_request_result['status']
                n_retries -= 1
            if req_status != 'processed' and n_retries == 0:
                logging.error('Данные не были подготовлены в течение двух часов. Работа программы остановлена.')
            else:
                n_parts = len(check_request_result['parts'])
                logging.info('Загрузка данных.')
                data_downloaded = self._download_data(request_type, req_id, n_parts)
                if not data_downloaded:
                    logging.error('Не удалось загрузить данные.')
