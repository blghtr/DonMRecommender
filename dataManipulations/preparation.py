from collections import Counter, defaultdict
from numpy import sum, bincount, log, arange, nan, zeros, ones, full, linspace
from numpy.linalg import norm
from scipy.sparse import csr_matrix, diags, hstack
from lightfm.cross_validation import random_train_test_split
from .databaseOperations import *
from joblib import Parallel, delayed
import gc
from copy import deepcopy
from pandas import crosstab, DataFrame, concat, get_dummies, cut, clip


def reverse_client_to_watches(client_to_watches):
    '''Принимает pandas-таблицу с просмотрами юзеров, возвращает словарь просмотры-юзеры из данных этой таблицы'''

    client_to_watches.loc[:, 'watchIDs'] = client_to_watches.watchIDs.apply(lambda x: x.strip('[]').split(','))
    client_to_watches = client_to_watches.explode('watchIDs', True)
    watches_to_client = {key: val for key, val in zip([x.strip() for x in client_to_watches.watchIDs.values.tolist()],
                                                      client_to_watches.userIDs.values.tolist())}
    return watches_to_client


def process_visits_data(config_path):
    '''Принимает объект logging.Logger, возвращает словарь просмотры-юзеры из всех имеющихся данных'''

    fs = connect_to_gfs('visits', config_path)
    filenames = fs.list()
    res_dict = {}

    for filename in filenames:
        client_to_watches = load_data(fs, filename)
        watches_to_client = reverse_client_to_watches(client_to_watches)
        res_dict.update(watches_to_client)

    return res_dict


def process_interactions_data(data, config_path, watches_to_user, city):
    '''Принимает pandas-таблицу с данными просмотров, словарь просмотры-юзеры и название города,
    возвращает pandas-таблицу с обработанными данными'''

    city_operators = get_config(config_path, 'cities')[0]
    data = data.copy()
    data = data[(data.url.str.contains(
        ''.join([f'don-m\.[a-zA-Z]+\/(?!journal){city_operators[city]}', '\/{0,1}(?!0+)[0-9]{2,}']),
        regex=True)) & (data.isView == '1')]
    data.drop(['timestamp', 'city', 'isView'], inplace=True, axis=1)
    data.loc[:, 'watchIDs'] = data.watchIDs.apply(lambda x: watches_to_user.get(str(x), 0))
    data.rename(columns={'watchIDs': 'userIDs'}, inplace=True)
    data.reset_index(inplace=True, drop=True)
    data.loc[:, 'url'] = data.url.apply(lambda x: re.search('\d+', x)[0])
    data.dropna(inplace=True)

    return data


def update_data_dict(data, res_dict, not_found_watches_counter):
    '''Принимает pandas-таблицу с обработанными данными просмотров, накопительный словарь с данными, словарь просмотры-юзеры,
    объект logging.Logger и аккумуляторную переменную с ненайденными просмотрами, обновляет накопительный словарь'''

    hits_dict = crosstab(data.userIDs, data.url).to_dict(orient='index')
    dropped = hits_dict.pop(0, None)
    if dropped is not None:
        not_found_watches_counter[0] += (sum(list(dropped.values())))

    for user in hits_dict:
        res_dict['u2c'][user].update(
            Counter(
                {girl: count for girl, count in hits_dict[user].items() if count > 0}
            )
        )
        res_dict['pages'] = res_dict['pages'].union(set(hits_dict[user]))
        res_dict['users'].add(user)

    del hits_dict
    gc.collect()


def write_latest_upd(db, city, watches_to_user, config_path, f_date, batch_size=5):
    '''Принимает название города, словарь визиты-юзеры и объект logging.Logger, возвращает словарь с данными'''

    n_jobs = get_config(config_path, 'n_jobs')[0]
    fs_raw = connect_to_gfs('hits', config_path)
    filenames = array(fs_raw.list())
    if filenames.size // (n_jobs * batch_size) > 1:
        filenames = array_split(filenames, filenames.size / (n_jobs * batch_size) + 1)
    else:
        filenames = (filenames,)

    logging.info(f'Начинаю обработку {len(filenames)} батчей.')

    res_dict = {
        'u2c': defaultdict(Counter),
        'pages': set(),
        'users': set()
    }

    not_found_watches_counter, parts_counter = 0, 0
    if ''.join(['latestUpdate', city]) not in db.list_collection_names():
        initialize_db(db, [''.join(['latestUpdate', city])])

    for i, batch in enumerate(filenames):
        logging.info(f'Обрабатываю {i} батч..')
        part_res = Parallel(n_jobs=n_jobs, prefer='processes')(delayed(get_part_data_dict)(deepcopy(res_dict), city,
                                                                                           config_path,
                                                                                           watches_to_user,
                                                                                           partFilenames)
                                                               for partFilenames in array_split(batch, n_jobs))
        not_found_watches_counter += sum([i[1][0] for i in part_res])

        for j, part in enumerate(part_res):
            users, items = part[0]['users'], part[0]['pages']
            update_mappings(db, city, users, items, f_date, config_path)
            update_interactions(db, city, part[0]['u2c'], f_date, config_path, coll='latestUpdate')

    logging.info(f'{not_found_watches_counter} записей удалено в процессе обработки посещений по причине: \
номера просмотров не найдены.')


def get_part_data_dict(res_dict, city, config_path, watches_to_user, filenames):
    '''Принимает название города, словарь визиты-юзеры и объект logging.Logger, возвращает словарь с данными'''
    fs = connect_to_gfs('hits', config_path)
    not_found_watches_counter = [0]
    for filename in filenames:
        raw_data = load_data(fs, filename)
        data = process_interactions_data(raw_data, config_path, watches_to_user, city)
        update_data_dict(data, res_dict, not_found_watches_counter)
        del data, raw_data
        gc.collect()

    return res_dict, not_found_watches_counter


def update_db(db, city, config_path, f_date):
    '''Получает название города и объект logging.Logger, возвращает словарь с данными'''

    logging.info('Сопоставление просмотров с идентификаторами юзеров.')
    watches_to_user = process_visits_data(config_path)

    logging.info('Обработка посещений.')
    write_latest_upd(db, city, watches_to_user, config_path, f_date)


def build_csr(db, city, size, data_dict=None):
    '''Принимает объект pymongo.MongoClient, название города, размерность данных (x, x), объект logging.Logger
    и, опционально, словарь данных. Возвращает csr-матрицу взаимодействий из словаря данных или, в случае его отсутствия,
    из всех данных в БД'''

    logging.info('Построение разреженной матрицы взаимодействий.')
    if data_dict is None:
        logging.info('Используются все данные из БД.')
    else:
        logging.info('Используется часть данных с последнего обновления')

    values, row_idxs, col_idxs = list(), list(), list()
    items_mapping = {item['id']: item['index'] for item in db[f'imap{city}'].find({},
                                                                                  {'id': True, 'index': True,
                                                                                   '_id': False})}
    if data_dict is None:
        u_indexes = arange(db[f'umap{city}'].count_documents({'index': {'$gt': -1}}))
        for batch in array_split(u_indexes, len(u_indexes) / 5000 + 1):
            users_mapping = {user['id']: user['index'] for user in
                             db[f'umap{city}'].find({'index': {'$in': batch.tolist()}},
                                                    {'_id': False})}
            try:
                users_inter = {
                    u['id']: u['interactions'] for u in
                    db[f'interactions{city}'].find({'id': {'$in': list(users_mapping)}},
                                                   {'_id': False, 'id': True, 'interactions': True})
                }

            except TypeError as terr:
                logging.error('В базе взаимодействий не найдено индексов из батча: база несогласованна или пуста')
                return None

            for u_id in users_inter:
                u_inter = users_inter[u_id]
                visited_pages = list(u_inter.keys())
                for item_id in visited_pages:
                    values.append(u_inter[item_id])
                    row_idxs.append(users_mapping[u_id])
                    col_idxs.append(items_mapping[item_id])

        not_found_counter = len(u_indexes) - len(set(row_idxs))

    else:
        u_ids = array(list(data_dict))
        for batch in array_split(u_ids, len(u_ids) / 5000 + 1):
            users_mapping = {user['id']: user['index'] for user in
                             db[f'umap{city}'].find({'id': {'$in': batch.tolist()}},
                                                    {'_id': False})}
            users_inter = {u: data_dict.get(u) for u in users_mapping if data_dict.get(u) is not None}
            for u_id in users_inter:
                u_inter = users_inter[u_id]
                visited_pages = list(u_inter.keys())
                for item_id in visited_pages:
                    values.append(u_inter[item_id])
                    row_idxs.append(users_mapping[u_id])
                    col_idxs.append(items_mapping[item_id])

        not_found_counter = len(u_ids) - len(set(row_idxs))

    if not_found_counter:
        logging.warning(f'{not_found_counter} пользователь(я/ей) отсутствует в коллекции взаимодействий')

    city_matrix = csr_matrix((values, (row_idxs, col_idxs)),
                             size)

    return city_matrix


def tf_idf(train, test, full):
    '''Применяет tf-Idf взвешивание к обучающей и тестовой выборкам, возвращает трансформированный вариант.'''

    res = []

    for weights_source in (train, full):
        n_samples, n_features = weights_source.shape
        df = bincount(weights_source.indices, minlength=weights_source.shape[1])
        df += 1
        n_samples += 1
        idf = log(n_samples / df) + 1
        idf_diag = diags(idf, offsets=0,
                         shape=(n_features, n_features),
                         format='csr')

        if weights_source is train:
            weighted = train * idf_diag
            n = norm(weighted, axis=1)
            weighted /= n.reshape((-1, 1))
            res.append(weighted)

        weighted = test * idf_diag
        n = norm(weighted, axis=1)
        weighted /= n.reshape((-1, 1))
        res.append(weighted)

    return res


class FeatureTransformer:

    def __init__(self, config_path):
        settings = get_config(config_path, 'featureTransformer')[0]
        if settings is None:
            msg = 'Отсутствуют конфигурации трансформера.'
            logging.error(msg)
            raise ValueError(msg)

        self.feature_names_seen = None
        self.feature_names_out = None
        self._num_feature_bins = None
        self._nan_replacement_vals = None
        self._seen_cat_vals = None
        self._n_rows = None
        self._n_cuts = settings.get('feature_n_cuts')
        self._weights = settings.get('fe_weights')

    def fit(self, X, items_size):
        """
        Извлекает из данных неоходимые для трансформации значения.

        :param X: pandas.DataFrame -- таблица признаков
        :param items_size: int -- количество объектов, известных модели рекомендаций
        :return: обученный трансформер
        """
        self._n_rows = items_size
        raw_features = X.copy()
        if not isinstance(raw_features, DataFrame):
            msg = 'Неверный тип аргумента Х, допустимо: pd.DataFrame'
            logging.error(msg)
            raise TypeError(msg)

        self.feature_names_seen = list(raw_features.columns)
        self._num_feature_bins, self._nan_replacement_vals, self.feature_names_out = {}, {}, []
        for fe in raw_features.select_dtypes('number'):
            column = raw_features.loc[:, fe]
            fe_n_cuts = self._n_cuts[fe]
            self._nan_replacement_vals[fe] = 0 if fe_n_cuts <= 2 else column.mean()
            self._num_feature_bins[fe] = linspace(column.min() - 0.001, column.max() + 0.001, fe_n_cuts + 1)
            self.feature_names_out.extend(['_'.join([fe, str(i)]) for i in range(fe_n_cuts)])
        self._seen_cat_vals = {}
        for fe in raw_features.select_dtypes('object'):
            vals = raw_features.loc[:, fe].unique().tolist()
            self._nan_replacement_vals[fe] = 'None'
            self.feature_names_out.extend(['_'.join([fe, val]) for val in vals])
            self._seen_cat_vals[fe] = vals

        return self

    def transform(self, X, index_order):
        """
        Трансформирует таблицу признаков в требуемый для обучения модели рекомендаций формат
        :param X: pandas.DataFrame -- таблица признаков
        :param index_order: array-like of ints -- айди объектов в порядке, зафиксированном в базе данных (iMap)
        :return: (scipy.sparse.csr_matrix, scipy.sparse.csr_matrix) - трансформированные признаки для обучения и
        взвешенный вариант для предскзаний (если в конфигурациях указаны веса featureTransformer -> fe_weights.
        В противном случае возвращает трансформированные признаки и None: (scipy.sparse.csr_matrix, scipy.sparse.csr_matrix)
        """
        raw_features = X.copy()
        if not isinstance(raw_features, DataFrame):
            msg = 'Неверный тип аргумента raw_features, допустимо: pd.DataFrame'
            logging.error(msg)
            raise TypeError(msg)

        if raw_features.shape[1] != len(self.feature_names_seen):
            msg = 'Количество признаков во входных данных отличается от распознанного при обучении трансформера'
            logging.error(msg)
            raise ValueError(msg)

        elif set(raw_features.columns) != set(self.feature_names_seen):
            msg = 'Некоторые признаки во входных данных отличаются от использованных при обучении трансформера'
            logging.error(msg)
            raise ValueError(msg)

        raw_features = raw_features.loc[:, self.feature_names_seen]
        raw_features = raw_features.reindex(index_order)

        raw_features = concat([raw_features,
                               DataFrame(full((self._n_rows - raw_features.shape[0], raw_features.shape[1]), nan),
                                         columns=raw_features.columns)], ignore_index=True)

        for fe in raw_features.select_dtypes('number'):
            nan_replacement_val = self._nan_replacement_vals[fe]
            raw_features.loc[:, fe] = raw_features.loc[:, fe].replace(nan, nan_replacement_val)

            feature_bins = self._num_feature_bins[fe]
            raw_features.loc[:, fe] = raw_features.loc[:, fe].clip(feature_bins[0], feature_bins[-1])
            raw_features.loc[:, fe] = cut(raw_features.loc[:, fe], feature_bins,
                                          labels=[str(i) for i in range(feature_bins.shape[0] - 1)])

        for fe in raw_features.select_dtypes('object'):
            nan_replacement_val = self._nan_replacement_vals[fe]
            vals = raw_features.loc[:, fe].unique().tolist()
            seen_vals = self._seen_cat_vals[fe]
            to_replace = [nan]
            to_replace.extend([val for val in vals if val not in seen_vals])
            raw_features.loc[:, fe] = raw_features.loc[:, fe].replace(to_replace, nan_replacement_val)

        dummies = get_dummies(raw_features)
        missing_fe = set(self.feature_names_out) - set(dummies.columns)
        dummies = concat([dummies, DataFrame(zeros((dummies.shape[0], len(missing_fe))),
                                             columns=list(missing_fe))], 1)
        dummies = dummies.loc[:, self.feature_names_out]

        if self._weights is not None:
            weighted_dummies = dummies.copy()
            columns = []
            weights = []
            for fe, w in self._weights.items():
                fe_dummies = [c for c in dummies.columns if re.search(f'{fe}_', c)]
                columns.extend(fe_dummies)
                weights.extend([w] * len(fe_dummies))
            dummies = hstack([dummies, diags(ones(dummies.shape[0]))], format='csr')
            weighted_dummies.loc[:, columns] = weighted_dummies.loc[:, columns] * array(weights)
            weighted_dummies = hstack([weighted_dummies, diags(ones(weighted_dummies.shape[0]))], format='csr')
            return dummies, weighted_dummies

        dummies = hstack([dummies, diags(ones(dummies.shape[0]))], format='csr')
        return dummies, None

    def fit_transform(self, X, items_size, index_order):
        """
        Последовательно применяет методы fit и transform.
        :param X: pandas.DataFrame -- таблица признаков
        :param items_size: int -- количество объектов, известных модели рекомендаций
        :param index_order: array-like of ints -- айди объектов в порядке, зафиксированном в базе данных (iMap)
        :return: (scipy.sparse.csr_matrix, scipy.sparse.csr_matrix) - трансформированные признаки для обучения и
        взвешенный вариант для предскзаний (если в конфигурациях указаны веса featureTransformer -> fe_weights.
        В противном случае возвращает трансформированные признаки и None: (scipy.sparse.csr_matrix, scipy.sparse.csr_matrix)
        """
        return self.fit(X, items_size).transform(X, index_order)


def get_items_fe(db, items_size, city, config_path, transform=True):

    index_order = [i['id'] for i in db[f'imap{city}'].find({'index': {'$gt': -1}}, sort=[('index', pymongo.ASCENDING)])]
    fs = connect_to_gfs('featureStorage', config_path)
    fe_file = fs.find_one({'filename': ''.join(['itemsFeatures', city])})
    if fe_file is None:
        msg = 'Не записаны признаки объектов.'
        logging.error(msg)
        raise FileNotFoundError(msg)
    items_fe = load_data(fs, ''.join(['itemsFeatures', city]))
    transformer_file = fs.find_one({'filename': ''.join(['itemsFeaturesTransformer', city])})

    if transform:
        if transformer_file is None:
            msg = 'Невозможно трансформировать признаки: трансформер не инициализирован.'
            logging.error(msg)
            raise FileNotFoundError(msg)
        else:
            feat_transformer = load_data(fs, ''.join(['itemsFeaturesTransformer', city]))
            items_fe_tr = feat_transformer.transform(items_fe, index_order)
    else:
        if transformer_file is not None:
            logging.info('Перезаписываю старый трансформер.')
            fs.delete(transformer_file._id)

        feat_transformer = FeatureTransformer(config_path)
        items_fe_tr = feat_transformer.fit_transform(items_fe, items_size, index_order)
        fs.put(feat_transformer, filename=''.join(['itemsFeaturesTransformer', city]))

    return items_fe_tr


def prepare_data(db, config_path, city, size, partial):
    source = None
    if partial:
        collection = db[''.join(['latestUpdate', city])]
        to_update = [user['id'] for user in collection.find({'id': {'$nin': ['refP']}},
                                                            {'_id': False, 'interactions': False})]
        source = {}
        ind_batch_size = get_config(config_path, 'ind_batch_size')[0]
        for batch in array_split(array(to_update), len(to_update) / ind_batch_size + 1):
            source |= {doc['id']: doc['interactions'] for doc in
                       collection.find({'id': {'$in': batch.tolist()}},
                                       {'id': True, 'interactions': True})}

    merge_latest_update(db, city, config_path)

    interactions_matrix = build_csr(db, city, size, data_dict=source)

    hyperp = get_config(config_path, 'hyperp')[0]
    test_size, random_state = hyperp['testSize'], hyperp['est']['random_state']

    items_fe, items_fe_weighted = get_items_fe(db, size[1], city, config_path, partial)
    train, test = random_train_test_split(interactions_matrix, test_size, random_state)
    train, test = train.tocsr(), test.tocsr()
    train, test, test_partial_fit = tf_idf(train, test, full=interactions_matrix)
    train, test, test_partial_fit = train.tocoo(), test.tocoo(), test_partial_fit.tocoo()

    return train, test, test_partial_fit, items_fe, items_fe_weighted


def is_model_capacity_enough(city, size, config_path):
    '''Принимает объект обученной модели lightfm.LightFM, извлекает максимальную размерность данных,
    сравнивает с имеющейся размерностью в БД. Возвращает True, если размерность модели как минимум в полтора раза больше,
    в противном случае возвращает False'''
    if size is None:
        return False

    n_model_users, n_model_items = size

    db = connect_to_db(config_path)

    n_db_users, n_db_items = db[f'umap{city}'].count_documents({}), db[f'imap{city}'].count_documents({})
    return n_model_users >= n_db_users and n_model_items >= n_db_items
