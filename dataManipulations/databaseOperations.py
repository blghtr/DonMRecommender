import pymongo, pickle, logging, json, gridfs, re
from lightfm import LightFM
from numpy import array_split, array


def connect_to_db(config_path):
    '''Принимает объект logging.Logger и путь до файла с конфигурациями, возвращает объект pymongo.MongoClient'''

    host, port = get_config(config_path, 'host', 'port')
    client = pymongo.MongoClient(host, port)
    db = client['RecDB']
    return db


def drop_db(db, *tables_list):
    for coll in tables_list:
        collection = db[f'{coll}']

        _ = collection.delete_many({})
        _ = collection.drop_indexes()

        logging.info(f'Коллекция {coll} удалена.')


def clear_storage(coll, config_path):
    '''Принимает объект logging.logging. Очищает коллекцию просмотров и визитов в БД'''

    logging.info(f'Очистка хранилища {coll}')

    fs = connect_to_gfs(coll, config_path)
    files_id = get_files_id(fs)

    while files_id:
        file_id = files_id.pop()
        fs.delete(file_id)

    return not (bool(get_files_id(fs)))


def initialize_db(db, coll_list):
    for coll in coll_list:
        coll_type = re.match('(umap|imap|interactions|scores|latestUpdate).*', coll)
        if coll_type is None:
            logging.error('Недопустимая коллекция: значения элементов массива colList должны \
            быть подмножеством "(umap|imap|interactions|scores|term).*"')
        else:
            collection = db[f'{coll}']
            res = collection.find_one({})

            if res is not None:
                logging.error(f'Коллекция {coll} не пуста, удалите данные перед инициализацией.')

            if coll_type.group(1) in ('umap', 'imap'):
                index1 = pymongo.IndexModel([("index", pymongo.ASCENDING)], unique=True)
                index2 = pymongo.IndexModel([("id", pymongo.TEXT)], unique=True)
                _ = collection.create_indexes([index1, index2])

                _ = collection.insert_one({
                    'index': -1,
                    'id': 'refP',
                    'latestUpd': None
                })

            else:
                _ = collection.create_index([("id", pymongo.TEXT)], unique=True)
                _ = collection.insert_one({
                    'id': 'refP',
                    'latestUpd': None
                })


def update_mappings(db, city, users, items, f_date, config_path):
    '''Принимает объект pymongo.MongoClient, название города, список юзеров, список объектов, объект logging.Logger.
    Дополняет мэппинги в БД новыми юзерами и объектами, фиксирует дату обновления.'''

    ind_batch_size = get_config(config_path, 'ind_batch_size')[0]
    logging.info('Обновление словарей соответствий.')
    for (coll, obj_set) in ((f'umap{city}', users), (f'imap{city}', items)):
        collection = db[f'{coll}']
        for batch in array_split(array(list(obj_set)), len(obj_set) / ind_batch_size + 1):
            collection.update_many({'id': {'$in': batch.tolist()}},
                                   {'$set': {'latestUpd': f_date}})
        updated = [doc['id'] for doc in collection.find({'latestUpd': f_date},
                                                        {'id': True, '_id': False})]
        to_insert = tuple(obj_set - set(updated))
        if to_insert:
            for batch in array_split(array(list(to_insert)), len(to_insert) / ind_batch_size + 1):
                latest_index = collection.find({},
                                               {'id': False, '_id': False}).sort('index',
                                                                                 pymongo.DESCENDING).limit(1)[0]['index']
                collection.insert_many([{'id': i, 'index': idx, 'latestUpd': f_date}
                                        for idx, i in enumerate(batch.tolist(), latest_index + 1)])

        _ = collection.find_one_and_update({'id': 'refP'}, {'$set': {'latestUpd': f_date}})


def update_interactions(db, city, inter_dict, f_date, config_path, coll='interactions'):
    '''Принимает объект pymongo.MongoClient, название города, словарь взаимодействий, объект logging.Logger.
    Обновляет взаимодействия в БД.'''

    logging.info('Обновление словарей взаимодействий.')
    doc_batch_size, ind_batch_size = get_config(config_path, 'doc_batch_size', 'ind_batch_size')
    collection = db[''.join([coll, city])]
    to_update = []
    for batch in array_split(array(list(inter_dict)), len(inter_dict) / ind_batch_size + 1):
        to_update += [doc['id'] for doc in collection.find({'id': {'$in': batch.tolist()}},
                                                           {'id': True, '_id': False})]
    if to_update:
        for batch in array_split(array(to_update), len(to_update) / doc_batch_size + 1):
            docs = [doc for doc in collection.find({'id': {'$in': batch.tolist()}},
                                                   {'id': True, '_id': False, 'interactions': True})]
            collection.delete_many({'id': {'$in': [doc['id'] for doc in docs]}})
            for doc in docs:
                doc['interactions'] = {k: doc['interactions'].get(k, 0) + inter_dict[doc['id']].get(k, 0)
                                       for k in set(doc['interactions']) | set(inter_dict[doc['id']])}
            collection.insert_many(docs)

    to_insert = tuple(set(inter_dict) - set(to_update))
    if to_insert:
        for batch in array_split(array(to_insert), len(to_insert) / doc_batch_size + 1):
            docs = [{'id': i, 'interactions': inter_dict[i]} for i in batch.tolist()]
            collection.insert_many(docs)

    _ = collection.find_one_and_update({'id': 'refP'}, {'$set': {'latestUpd': f_date}})


def merge_latest_update(db, city, config_path):
    logging.info('Сливаю последнее обновление с основной коллекцией взаимодействий.')
    collection = db[''.join(['latestUpdate', city])]
    up_date = collection.find_one({'id': 'refP'})

    if up_date is None:
        logging.warning('Такого быть не должно, но похоже, коллекция последних обновлений пуста')
    else:
        up_date = up_date['latestUpd']

    ind_batch_size = get_config(config_path, 'ind_batch_size')[0]
    to_merge = [user['id'] for user in
                collection.find({'id': {'$nin': ['refP']}}, {'_id': False, 'interactions': False})]

    u_inter = {}
    for batch in array_split(array(to_merge), len(to_merge) / ind_batch_size + 1):
        u_inter.update({doc['id']: doc['interactions'] for doc in
                        collection.find({'id': {'$in': batch.tolist()}},
                                        {'id': True, 'interactions': True})})
        update_interactions(db, city, u_inter, up_date, config_path)
    db.drop_collection(''.join(['latestUpdate', city]))
    initialize_db(db, [''.join(['latestUpdate', city])])


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

    model = LightFM(**hyperp)
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
    with open(config_path, "r") as config:
        settings = json.load(config)
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
