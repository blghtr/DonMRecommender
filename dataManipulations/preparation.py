from collections import Counter, defaultdict

import pandas as pd
from numpy import sum, bincount, log, arange, nan, zeros, ones, full, linspace
from numpy.linalg import norm
from scipy.sparse import csr_matrix, diags, hstack
from .databaseOperations import *
from joblib import Parallel, delayed
import gc
from copy import deepcopy
from pandas import crosstab, DataFrame, concat, get_dummies, cut


class DataHandler:
    def __init__(self, config_path):
        self.db = DataBase(config_path)
        self.storage = Storage(config_path)
        settings = get_config(config_path, 'preparator')[0]
        if settings is not None:
            self.cities = settings['cities']
            self.n_jobs = settings['n_jobs']
        else:
            raise ValueError('Не удалось получить конфигурацию предпроцессора по заданному пути')

    def _filter_input(self, city, inter_dict):
        blacklist = self.db.get_blacklist(city)
        valid_input, invalid_input = defaultdict(dict), defaultdict(dict)
        for uID, counter in inter_dict.items():
            for pageID, count in counter.items():
                if pageID not in blacklist:
                    valid_input[uID][pageID] = count
                else:
                    invalid_input[uID][pageID] = count
        return valid_input, invalid_input

    def write_upd(self, city, write_goals=False):
        '''Принимает название города, словарь визиты-юзеры и объект logging.Logger, возвращает словарь с данными'''
        filenames = array(self.storage['hits'].list())
        filenames = array_split(filenames, filenames.size / self.n_jobs + 1) if filenames.size // self.n_jobs > 1 else (filenames,)
        logging.info(f'Начинаю обработку {len(filenames)} батчей(а).')
        res_dict = {
            'u2v': defaultdict(Counter),
            'stream': defaultdict(list),
            'pages': set(),
            'users': set()
        }
        if write_goals:
            res_dict['u2g'] = defaultdict(Counter)
        not_found_watches_counter, parts_counter = 0, 0
        for i, batch in enumerate(filenames):
            logging.info(f'Обрабатываю {i} батч..')
            part_res = Parallel(n_jobs=self.n_jobs,
                                prefer='threads')(delayed(self._get_part_data_dict)(deepcopy(res_dict),
                                                                                    city, filename,
                                                                                    write_goals)
                                                  for filename in batch)
            not_found_watches_counter += sum([i[1][0] for i in part_res])
            for j, part in enumerate(part_res):
                pages_counter = Counter()
                for _, user_counter in part[0]['u2v'].items():
                    pages_counter.update(user_counter)
                self.db.update_filter(city, pages_counter)
                users, items = part[0]['users'], part[0]['pages']
                self.db.update_mappings(city, users, items)
                valid_input, invalid_input = self._filter_input(city, part[0]['u2v'])
                self.db.update_interactions(city, valid_input)
                self.db.update_interactions(city, invalid_input, collection_name='tempViews')
                self.db.update_stream(city, part[0]['stream'])
                if write_goals:
                    valid_input, invalid_input = self._filter_input(city, part[0]['u2g'])
                    self.db.update_interactions(city, valid_input, 'goals')
                    self.db.update_interactions(city, invalid_input, collection_name='tempGoals')
            self.db.merge_temp(city)
        logging.info(f'{not_found_watches_counter} записей удалено в процессе обработки посещений по причине: \
    номера просмотров не найдены.')

    def _get_part_data_dict(self, res_dict, city, filename, write_goals):
        '''Принимает название города, словарь визиты-юзеры и объект logging.Logger, возвращает словарь с данными'''
        not_found_watches_counter = [0]
        raw_data = self.storage.download_file(filename, 'hits')
        data = self._process_data(raw_data, city)
        self._update_data_dict(data, res_dict, not_found_watches_counter, write_goals)
        del data, raw_data
        gc.collect()

        return res_dict, not_found_watches_counter

    def _process_data(self, data, city):
        '''Принимает pandas-таблицу с данными просмотров, словарь просмотры-юзеры и название города,
        возвращает pandas-таблицу с обработанными данными'''
        data = data.sort_values(by=['clientID', 'dateTime'])
        data.loc[:, 'goal'] = 0
        goals = data.URL.str.contains(re.compile('goal.+GIRL'))
        data.loc[goals, 'goal'] = 1
        data.loc[goals, 'isPageView'] = '1'
        data.loc[goals, 'URL'] = data.loc[goals, 'referer']
        data = data.loc[
            (data.URL.str.contains(''.join([f'don-m\.[a-zA-Z]+\/(?!journal){self.cities[city]}',
                                            '\/{0,1}(?!0+)[0-9]{2,}']), regex=True)) & (data.isPageView == '1'),
            ['clientID', 'dateTime', 'URL', 'goal']
        ]
        data.loc[:, 'URL'] = data.URL.apply(lambda x: re.search('\d+', x)[0])
        data.loc[:, 'dateTime'] = pd.to_datetime(data.loc[:, 'dateTime'])
        data.dropna(inplace=True)
        return data

    def _update_data_dict(self, data, res_dict, not_found_watches_counter, write_goals):
        '''Принимает pandas-таблицу с обработанными данными просмотров, накопительный словарь с данными, словарь просмотры-юзеры,
        объект logging.Logger и аккумуляторную переменную с ненайденными просмотрами, обновляет накопительный словарь'''
        views, goals = data[data.goal == 0], data[data.goal == 1]
        views_dict = crosstab(views.clientID, views.URL).to_dict(orient='index')
        goals_dict = crosstab(goals.clientID, goals.URL).to_dict(orient='index') if write_goals else None
        dropped = views_dict.pop(0, None)
        if dropped is not None:
            not_found_watches_counter[0] += (sum(list(dropped.values())))
        for user in views_dict:
            update_counter = Counter(
                {girl: count for girl, count in views_dict[user].items() if count > 0}
            )
            if update_counter:
                res_dict['u2v'][user].update(update_counter)
            if write_goals:
                update_counter = Counter(
                    {girl: count for girl, count in goals_dict.get(user, {'none': 0}).items() if count > 0}
                )
                if update_counter:
                    res_dict['u2g'][user].update(update_counter)
            res_dict['pages'] = res_dict['pages'].union(set(views_dict[user]))
            res_dict['users'].add(user)
            res_dict['stream'][user].extend([
                (data.URL, data.dateTime) for _, data in views.loc[views.clientID == user].iterrows()
            ])
        del views_dict, goals_dict
        gc.collect()

    def split_data(self, city, min_train_seq=5, n_pos=1, val_size=5, test_size=0):
        stream = self.db[''.join(['stream', 'city'])]
        users = stream.find({}, {'_id': False, 'pageID': False, 'timestamp': False, 'split': False}).distinct('userID')
        for user in users:
            self.db.split_stream(city, user, min_train_seq, n_pos, val_size, test_size)

    def map_users(self, users, city, reverse=False):
        umap = self.db[''.join(['umap', city])]
        return self._map(users, umap, reverse)

    def map_items(self, items, city, reverse=False):
        imap = self.db[''.join(['imap', city])]
        return self._map(items, imap, reverse)

    def _map(self, seq, mapping_collection, reverse):
        projection_input = {'_id': False, 'id': True, 'index': True}
        if reverse:
            res = {doc['index']: doc['id'] for doc in mapping_collection.find({'index': {'$in': seq}}, projection_input)}
        else:
            res = {doc['id']: doc['index'] for doc in mapping_collection.find({'id': {'$in': seq}}, projection_input)}
        return [res[obj] for obj in seq]

    def get_negatives(self):
        raise NotImplemented

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
