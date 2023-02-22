import requests, pandas as pd
from time import sleep
from dataManipulations.databaseOperations import *
import json


def check_authorization(config_path):
    headers, counter_id = get_config(config_path, 'headers', 'counterId')
    response = requests.get(f'https://api-metrika.yandex.net/management/v1/counter/{counter_id}/logrequests',
                            headers=headers)

    if response.status_code == 403:
        errors = json.loads(response._content)
        logging.warning(f'Авторизация не пройдена: {str(errors)}.\
        Актуализируйте OAuth-токен перед продолжением работы.')
        return False

    return True

def get_requests_list(config_path):
    '''Принимает путь до json-файла с конфигурациями и объект logging.logging, возвращает список имеющихся запросов или None'''

    headers, counter_id = get_config(config_path, 'headers', 'counterId')

    response = requests.get(f'https://api-metrika.yandex.net/management/v1/counter/{counter_id}/logrequests',
                            headers=headers)

    if response.status_code == 200:
        rec_dict = json.loads(response.content)

        requests_list = [req['request_id'] for req in rec_dict['requests']]
        return requests_list

    else:
        logging.error(f'Не удалось получить список запросов: {str(response)}.\
        Проверьте доступ к интернету и актуальность ключа.')
        return None


def clean_requests(config_path, requests_list, n_retries=5):
    '''Удаляет на стороне метрики запросы и подготовленные данные.
    Принимает путь до json-файла с конфигурациями, список запросов на удаление, объект logging.logging и количество попыток.'''

    headers, counter_id = get_config(config_path, 'headers', 'counterId')

    for req_id in requests_list:
        url2clean = f'https://api-metrika.yandex.net/management/v1/counter/{counter_id}/logrequest/{req_id}/clean'
        requests.post(url2clean, headers=headers)

    new_req_list = get_requests_list(config_path)
    if new_req_list and n_retries:
        logging.warning('Старые запросы очищены не полностью, повторяю попытку через 2 минуты.')
        sleep(60)
        clean_requests(config_path, new_req_list, n_retries=n_retries - 1)

    if new_req_list and not n_retries:
        logging.error('Старые запросы очищены не полностью: возможны дальнейшие ошибки')


def is_request_possible(request_type, config_path, s_date, f_date):
    '''Принимает тип запроса ('visits' или 'hits', путь до json-файла с конфигурациями, дату начала запроса,
    дату окончания запроса (YYYY-MM-DD), объект logging.logging. Возвращает булево значение, означающее возможность запроса
    в указанные даты'''

    headers, counter_id = get_config(config_path, 'headers', 'counterId')

    if request_type not in ['visits', 'hits']:
        logging.error('Неизвестный тип запроса. Корректные значения: "visits", "hits".')
        return None

    elif request_type == 'hits':
        url = f'https://api-metrika.yandex.net/management/v1/counter/{counter_id}/logrequests/evaluate?\
date1={str(s_date)}&date2={str(f_date)}&fields=ym:pv:date,ym:pv:regionCity,ym:pv:URL,ym:pv:watchID,\
ym:pv:isPageView&source=hits'

    elif request_type == 'visits':
        url = f'https://api-metrika.yandex.net/management/v1/counter/{counter_id}/logrequests/evaluate?\
date1={str(s_date)}&date2={str(f_date)}&fields=ym:s:clientID,ym:s:watchIDs&source=visits'

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        req_possibility = json.loads(response.content)['log_request_evaluation']['possible']
        max_possible_period = json.loads(response.content)['log_request_evaluation']['max_possible_day_quantity']
        if (f_date - s_date).days > max_possible_period:
            logging.error('Выбранный период превышает квоту: удостоверьтесь, что старые запросы очищены.')
        return req_possibility

    logging.error(f'Не удалось оценить возможность запроса: {str(response)}.\
    Проверьте доступ к интернету и актуальность ключа.')
    return None


def request_data(request_type, config_path, s_date, f_date):
    '''Принимает тип запроса ('visits' или 'hits', путь до json-файла с конфигурациями, дату начала запроса,
    дату окончания запроса (YYYY-MM-DD), объект logging.logging. Запрашивает данные из метрики по указанным датам.'''

    headers, counter_id = get_config(config_path, 'headers', 'counterId')

    if request_type not in ['visits', 'hits']:
        logging.error('Неизвестный тип запроса. Корректные значения: "visits", "hits".')
        return None

    elif request_type == 'hits':
        url = f'https://api-metrika.yandex.net/management/v1/counter/{counter_id}/logrequests?\
date1={str(s_date)}&date2={str(f_date)}&fields=ym:pv:date,ym:pv:regionCity,ym:pv:URL,ym:pv:watchID,\
ym:pv:isPageView&source=hits'
    elif request_type == 'visits':
        url = f'https://api-metrika.yandex.net/management/v1/counter/{counter_id}/logrequests?\
date1={str(s_date)}&date2={str(f_date)}&fields=ym:s:clientID,ym:s:watchIDs&source=visits'

    response = requests.post(url, headers=headers)

    if response.status_code == 200:
        req_id = json.loads(response.content)['log_request']['request_id']
        return req_id

    logging.error(f'Не удалось запросить данные: {str(response)}. \
    Проверьте доступ к интернету и актуальность ключа.')
    return None


def check_request(config_path, req_id):
    '''Принимает до json-файла с конфигурациями, ID запроса, объект logging.logging.
    Возвращает словарь с данными по запросу или None'''

    headers, counter_id = get_config(config_path, 'headers', 'counterId')

    url = f'https://api-metrika.yandex.net/management/v1/counter/{counter_id}/logrequest/{req_id}'
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        req_info = json.loads(response.content)['log_request']
        return req_info

    logging.error('Не удалось проверить статус запроса: {}. \
    Проверьте доступ к интернету и актуальность ключа.'.format(str(response)))
    return None


def download_data(request_type, config_path, req_id, n_parts):
    '''Принимает до json-файла с конфигурациями, ID запроса, объект logging.logging.
    Возвращает словарь с данными по запросу или None'''

    storage_is_empty = clear_storage(request_type, config_path)

    if storage_is_empty:
        success = True
        headers, counter_id = get_config(config_path, 'headers', 'counterId')

        if request_type not in ['visits', 'hits']:
            logging.error('Неизвестный тип запроса. Корректные значения: "visits", "hits".')
            return False

        elif request_type == 'hits':
            fields = ['timestamp', 'city', 'url', 'watchIDs', 'isView']
            name_pattern = 'hitsPart{}'
        else:
            fields = ['userIDs', 'watchIDs']
            name_pattern = 'visitsPart{}'

        fs = connect_to_gfs(request_type, config_path)

        for part in range(n_parts):
            response = requests.get(f'https://api-metrika.yandex.net/management/v1/counter/{counter_id}/logrequest/\
    {req_id}/part/{part}/download', headers=headers)
            if str(response) == '<Response [200]>':
                data = [x.split('\t') for x in response.content.decode('utf-8').split('\n')[:-1]]
                df = pd.DataFrame(data[1:], columns=fields)
                filename = name_pattern.format(part)
                fs.put(pickle.dumps(df), filename=filename)

            else:
                logging.error(f'Не удалось скачать {str(part)} часть данных: {str(response)}. \
                Проверьте доступ к интернету и актуальность ключа. Изменения откатываются.')
                storage_is_empty = clear_storage(request_type, logging)
                if not storage_is_empty:
                    logging.error('Не удалось скачать откатить изменения, хранилище не очищено.')
                success = False
                break
    else:
        success = False

    return success


def get_data(config_path, s_date, f_date):
    '''Обращается к API Метрики, чтобы обновить данные просмотров в БД.
    Если БД пуста - скачивает данные за последний год, если не пуста - со дня последнего обновления
    до вызова функции не включительно.
    Принимает объект logging.logging, путь до конфигурационного файла, граничные даты периода, возвращает True
    при успешной закачке, False в противном случае.'''

    s_date, f_date = s_date.date(), f_date.date()

    logging.info('Сбор старых запросов.')
    req_list = get_requests_list(config_path)

    if req_list is not None:
        logging.info('Удаление старых запросов.')
        clean_requests(config_path, req_list)
    else:
        logging.warning('Старые запросы отсутствуют или попытка запросить информацию о них провалилась')

    data_downloaded = False
    for request_type in ('visits', 'hits'):
        logging.info(f'Оценка возможности нового запроса: {request_type}')
        req_is_possible = is_request_possible(request_type, config_path, s_date, f_date)

        if not req_is_possible:
            logging.error('Обработка запроса невозможна. Работа программы остановлена.')
            break
        else:
            logging.info('Запрос данных.')
            req_id = request_data(request_type, config_path, s_date, f_date)
            logging.info('Ожидание готовности запроса.')
            if req_id is None:
                logging.error('Не удалось запросить данные. Работа программы остановлена.')
                break
            else:
                check_request_result = check_request(config_path, req_id)
                if check_request_result is None:
                    logging.error('Не удалось получить статус запроса. Работа программы остановлена.')
                    break
                else:
                    req_status = check_request_result['status']
                    n_retries = 72
                    while req_status != 'processed' and n_retries > 0:
                        sleep(100)
                        check_request_result = check_request(config_path, req_id)

                        if check_request_result is None:
                            logging.error('Не удалось получить статус запроса. Работа программы остановлена.')
                            break

                        else:
                            req_status = check_request_result['status']
                            n_parts = len(check_request_result['parts']) if req_status == 'processed' else None
                            n_retries -= 1

                    if req_status != 'processed' and n_retries == 0:
                        logging.error('Данные не были подготовлены в течение двух часов. Работа программы остановлена.')
                        break

                    else:
                        logging.info('Загрузка данных.')
                        data_downloaded = download_data(request_type, config_path, req_id, n_parts)

                        if not data_downloaded:
                            logging.error('Не удалось загрузить данные. Работа программы остановлена.')
                            break
    return data_downloaded
