import copy
import datetime

import pymongo, pickle, logging, json, gridfs, re
from pymongo.errors import ConnectionFailure, OperationFailure
from numpy import array_split, array
import os


class Storage:
    def __init__(self, config_path, collection_name_s=('hits',), db_name='RawData'):
        storage_settings = get_config(config_path, 'storage')[0]
        if storage_settings is not None:
            host, port = storage_settings.get('host'), storage_settings.get('port')
        else:
            raise ValueError('Отсутствуют конфигурации хранилища данных.')
        client = pymongo.MongoClient(host, port)
        self.fs = {collection_name: gridfs.GridFS(client[db_name], collection_name)
                   for collection_name in collection_name_s}

    def __getitem__(self, collection_name):
        return self.fs[collection_name]
    def clear_storage(self, collection_name=None):
        if collection_name is None:
            res = []
            for fs in self.fs.values():
                for i in fs.find({}):
                    fs.delete(i._id)
                res.append(not bool(fs.list()))
            return all(res)
        else:
            for i in self[collection_name].find({}):
                self[collection_name].delete(i._id)
            return not bool(self[collection_name].list())

    def download_file(self, filename, collection_name):
        collection = self[collection_name]
        file = collection.find_one({'filename': filename})
        if file is None:
            logging.error(f'В хранилище {collection_name} отсутствует файл {filename}')
            return None
        file_id = file._id
        with collection.get(file_id) as file_bin:
            res = file_bin.read()
        return pickle.loads(res)

    def delete_file(self, filename, collection_name):
        collection = self[collection_name]
        file = collection.find_one({'filename': filename})
        if file is None:
            logging.error(f'В хранилище {collection_name} отсутствует файл {filename}')
            return None
        file_id = file._id
        collection.delete(file_id)

    def upload_file(self, file, filename, collection_name):
        collection = self[collection_name]
        res_file = collection.find_one({'filename': filename})
        if res_file is not None:
            logging.error(f'В хранилище {collection_name} уже есть файл {filename}')
            return None
        file_id = collection.put(pickle.dumps(file), filename=filename)
        return file_id


class DataBase:
    def __init__(self, config_path, db_name='Records'):
        settings = get_config(config_path, 'storage')[0]
        host, port, self.batch_size, self.doc_batch_size, self.base_collections = settings['host'], settings['port'],\
                                                                                  settings['batch_size'],\
                                                                                  settings['doc_batch_size'],\
                                                                                  settings['collections']
        cities = list(get_config(config_path, 'preparator')[0]['cities'].keys())
        self.collections = [''.join([collection, city]) for collection in self.base_collections for city in cities]
        self.client = pymongo.MongoClient(host, port)
        self.db = self.client[db_name]
        if not self.db.list_collection_names():
            self._initialize_db()
        self.date = datetime.datetime.utcnow()

    def _initialize_db(self):
        for coll in self.collections:
            comp = re.compile('|'.join([collection for collection in self.base_collections]) + '.*')
            coll_type = re.match(comp, coll)
            collection = self.db[f'{coll}']
            res = collection.find_one({})
            if res is not None:
                raise FileExistsError(f'Коллекция {coll} не пуста, удалите данные перед инициализацией.')
            if coll_type.group(0) in ('umap', 'imap'):
                index1 = pymongo.IndexModel([("index", pymongo.ASCENDING)], unique=True)
                index2 = pymongo.IndexModel([("id", pymongo.TEXT)], unique=True)
                _ = collection.create_indexes([index1, index2])
                _ = collection.insert_one({
                    'index': -1,
                    'id': '-1',
                    'latestUpd': None
                })
            elif coll_type.group(0) == 'stream':
                index1 = pymongo.IndexModel([("userID", pymongo.TEXT), ("split", pymongo.TEXT)], unique=False)
                index2 = pymongo.IndexModel([("timestamp", pymongo.ASCENDING)], unique=False)
                #index3 = pymongo.IndexModel([("split", pymongo.TEXT)], name="splitIdx", unique=False)
                _ = collection.create_indexes([index1, index2])
            else:
                _ = collection.create_index([("id", pymongo.TEXT)], unique=True)
                _ = collection.insert_one({
                    'id': '-1',
                    'latestUpd': None
                })

    def __getitem__(self, collection_name):
        return self.db[collection_name]

    def drop_db(self, collections=None):
        if collections is None:
            for coll in self.db.list_collection_names() if collections is None else collections:
                self.db.drop_collection(coll)
                logging.info(f'Коллекция {coll} удалена.')

    def update_mappings(self, city, users, items):
        '''Принимает объект pymongo.MongoClient, название города, список юзеров, список объектов, объект logging.Logger.
        Дополняет мэппинги в БД новыми юзерами и объектами, фиксирует дату обновления.'''
        logging.info('Обновление словарей соответствий.')
        with self.client.start_session() as session:
            try:
                with session.start_transaction():
                    for (coll, obj_set) in ((f'umap{city}', users), (f'imap{city}', items)):
                        collection = self.db[coll]
                        for batch in array_split(array(list(obj_set)), len(obj_set) / self.batch_size + 1):
                            collection.update_many({'id': {'$in': batch.tolist()}},
                                                   {'$set': {'latestUpd': self.date}})
                        updated = [doc['id'] for doc in collection.find({'latestUpd': self.date},
                                                                        {'id': True, '_id': False})]
                        to_insert = tuple(obj_set - set(updated))
                        if to_insert:
                            for batch in array_split(array(list(to_insert)), len(to_insert) / self.batch_size + 1):
                                latest_index = collection.find({},
                                                               {'id': False, '_id': False}).sort('index',
                                                                                                 pymongo.DESCENDING
                                                                                                 ).limit(1)[0]['index']
                                collection.insert_many([{'id': i, 'index': idx, 'latestUpd': self.date}
                                                        for idx, i in enumerate(batch.tolist(), latest_index + 1)])
                        _ = collection.find_one_and_update({'id': -1}, {'$set': {'latestUpd': self.date}})
            except (ConnectionFailure, OperationFailure):
                session.abort_transaction()
                print("Transaction aborted")


    def update_interactions(self, city, inter_dict, collection='views'):
        '''Принимает объект pymongo.MongoClient, название города, словарь взаимодействий, объект logging.Logger.
        Обновляет взаимодействия в БД.'''
        logging.info('Обновление словарей взаимодействий.')
        with self.client.start_session() as session:
            try:
                with session.start_transaction():
                    collection = self.db[''.join([collection, city])]
                    to_update = []
                    for batch in array_split(array(list(inter_dict)), len(inter_dict) / self.batch_size + 1):
                        to_update += [doc['id'] for doc in collection.find({'id': {'$in': batch.tolist()}},
                                                                           {'id': True, '_id': False})]
                    if to_update:
                        for batch in array_split(array(to_update), len(to_update) / self.doc_batch_size + 1):
                            ids_to_del, docs_to_add = [], []
                            for doc in collection.find({'id': {'$in': batch.tolist()}},
                                                       {'id': True, '_id': False, 'interactions': True}):
                                doc = copy.deepcopy(doc)
                                ids_to_del.append(doc['id'])
                                doc['interactions'] = {k: doc['interactions'].get(k, 0) + inter_dict[doc['id']].get(k, 0)
                                                      for k in set(doc['interactions']) | set(inter_dict[doc['id']])}
                                docs_to_add.append(doc)
                            collection.delete_many({'id': {'$in': ids_to_del}})
                            collection.insert_many(docs_to_add)

                    to_insert = tuple(set(inter_dict) - set(to_update))
                    if to_insert:
                        for batch in array_split(array(to_insert), len(to_insert) / self.doc_batch_size + 1):
                            docs = [{'id': i, 'interactions': inter_dict[i]} for i in batch.tolist()]
                            collection.insert_many(docs)

                    _ = collection.find_one_and_update({'id': -1}, {'$set': {'latestUpd': self.date}})
            except (ConnectionFailure, OperationFailure):
                session.abort_transaction()
                print("Transaction aborted")

    def update_stream(self, city, stream_dict):
        with self.client.start_session() as session:
            try:
                with session.start_transaction():
                    collection = self.db[''.join(['stream', city])]
                    stream_data = ({'userID': user,
                                    'pageID': url,
                                    'timestamp': timestamp,
                                    'split': 'train'} for user in stream_dict for url, timestamp in stream_dict[user])
                    batch = []
                    for doc in stream_data:
                        batch.append(doc)
                        if len(batch) == self.doc_batch_size:
                            collection.insert_many(batch)
                            batch = []
                    if batch:
                        collection.insert_many(batch)
            except (ConnectionFailure, OperationFailure):
                session.abort_transaction()
                print("Transaction aborted")

    def split_stream(self, city, user, min_train_seq, n_pos, val_size, test_size):
        min_train_seq, val_size = min_train_seq + n_pos, val_size + n_pos
        stream = self.db[''.join(['stream', city])]
        user_data = [doc for doc in stream.find({'userID': user},
                                                {'userID': True, 'pageID': True,
                                                 'timestamp': True, 'split': True, '_id': True},
                                                sort=[('timestamp', pymongo.ASCENDING)])]
        n_interactions = len(user_data)
        if n_interactions < min_train_seq:
            stream.delete_many({'userID': user})
        elif n_interactions >= min_train_seq + val_size + test_size:
            if test_size > 0:
                test_ids = [doc['_id'] for doc in user_data[-test_size:]]
                stream.update_many({'_id': {'$in': test_ids}}, {'$set': {'split': 'test'}})
            if val_size > 0:
                val_ids = [doc['_id'] for doc in user_data[-(val_size + test_size):-test_size]]
                stream.update_many({'_id': {'$in': val_ids}}, {'$set': {'split': 'val'}})


def get_model(fs, city, filename=None):
    '''Принимает объект gridfs.GridFS и название файла модели, возвращает файл из хранилища.
    Если название не дано, возвращает последнюю загруженную модель или None'''

    if filename is not None:
        model_file = fs.find_one({'filename': filename})
        if model_file is not None:
            model_file_id = model_file._id
            with fs.get(model_file_id) as modelBin:
                res = modelBin.read()
                return pickle.loads(res)

        else:
            logging.error(f'В базе данных отсутствует модель {filename}')
    else:
        models = [x for x in fs.find({}).sort('uploadDate', pymongo.DESCENDING)]
        file_name = 'filename'
        while models and re.search(city, file_name) is None:
            model = models.pop(0)
            file_name = model.filename

        if re.search(city, file_name) is not None:
            return get_model(fs, city, filename=file_name)
        else:
            logging.info(f'Похоже, хранилище моделей пусто.')
            return None


def init_model(config_path):
    logging.info('Инициализирую новую модель.')
    hyperp = get_config(config_path, 'hyperp')[0]
    hyperp = hyperp['est']

    model = None #LightFM(**hyperp)
    return model


def connect_to_gfs(coll, config_path):
    '''Принимает название коллекции, объект logging.Logger, путь до файла с конфигурациями,
    возвращает объект хранилища gridfs.GridFS'''
    host, port = get_config(config_path, 'host', 'port')
    client = pymongo.MongoClient(host, port).RecData
    fs = gridfs.GridFS(client, collection=coll)
    return fs


def load_data(fs, filename):
    '''Принимает объект gridfs.GridFS и название файла, возвращает файл из хранилища'''

    file = fs.find_one({'filename': filename})
    if file is None:
        logging.error(f'В базе данных отсутствует файл {filename}')
        return None

    file_id = file._id
    with fs.get(file_id) as fileBin:
        res = fileBin.read()
    return pickle.loads(res)



def get_files_id(fs):
    '''Принимает объект gridfs.GridFS и объект logging.Logger, возвращает список ID файлов в хранилище'''

    ids = []
    filenames = fs.list()

    for name in filenames:
        ids.append(fs.find_one({'filename': name})._id)

    return ids


def update_config(config_path, update_dict):
    with open(config_path, "r") as config:
        settings = json.load(config)

    for field in update_dict:
        settings[field] = update_dict[field]

    with open(config_path, "w") as config:
        json.dump(settings, config)


def get_config(config_path, *values):
    try:
        with open(config_path, "r") as config:
            settings = json.load(config)
    except FileNotFoundError:
        logging.error(f'Не найден файл конфигураций {config_path}')
        return None
    res = []
    for val in values:
        if val not in settings:
            logging.warning(f'Параметр {val} не найден в файле конфигураций.')
        res.append(settings.get(val))
    return res


def features_is_assigned(config_path):
    fs = connect_to_gfs('featureStorage', config_path)
    file = fs.find_one({'filename': 'itemsFeatures'})
    return True if file is not None else False
