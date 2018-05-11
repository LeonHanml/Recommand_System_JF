# -*- coding: utf-8 -*-
import os
import lmdb
import time
from ConfigParser import SafeConfigParser
from util.tool import tool
from model import model_cf
from model import model_default
from model import model_embedding
from data import data_pno
from sklearn.neighbors import NearestNeighbors


class RecommendSystem(object):
    def __init__(self):
        config = SafeConfigParser()
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'RecommendSystem.conf')
        config.read(path)
        self.config = config
        self.wksc_db_conn = tool.mysql_db_connect()
        self.ai_db_conn = tool.mysql_ai_db_connect()
        # 协同过滤推荐需要使用的  矩阵数据
        self.lmdb_env_user_behavior = lmdb.open(self.config.get('local_db', 'lmdb_file_user_behavior'), map_size=1e12)

        recommend_system_w2v_bin = self.config.get('file', 'recommend_system_w2v_bin')

        min_return_count = self.config.getint('main', 'min_return_count')
        pno_list_by_default = model_default.recommend_pno_by_default(min_return_count)
        self.item2vec = word2vec.WordVectors.from_binary(recommend_system_w2v_bin, encoding="ISO-8859-1")
        m, d = self.item2vec.vectors.shape
        self.item_dict = {self.item2vec.vocab(i).encode('ISO-8859-1'): i for i in range(m)}
        knn = NearestNeighbors(n_neighbors=config.getint('knn', 'n_neighbors'))
        knn.fit(self.item2vec.vectors)
        self.knn = knn

    def __del__(self):
        self.lmdb_env_user_behavior.close()
        self.ai_db_conn.close()
        self.wksc_db_conn.close()

    @classmethod
    def sample_result(self, pno_list, once_return_count):
        """
        从推荐列表中随机抽取结果
        once_return_count: 一次返回结果数量
        n_first_part :  n_first 要小于  总数的十分之一
        
        :param pno_list: 
        :param once_return_count: 
        :return: 
        """
        pno_list_count = len(pno_list)
        n_first_part = once_return_count / 3
        n_first_part_count = pno_list_count / 10
        while n_first_part > n_first_part_count:
            n_first_part /= 2
        first_part = tool.random_sample(pno_list[:n_first_part_count], n_first_part)
        latter_part = tool.random_sample(pno_list[n_first_part_count: pno_list_count], once_return_count - n_first_part)
        return first_part + latter_part

    @classmethod
    def recommend_pno_for_user(self, user_id, once_return_count):
        """
        为用户推荐产品  
     
        :param user_id: 
        :param once_return_count: 一次推荐返回的推荐商品数量
        :return: 
        """
        self.config = self.config
        self.item2vec = self.item2vec
        self.lmdb_env_user_behavior = self.lmdb_env_user_behavior
        self.knn = self.knn
        self.item_dict = self.item_dict
        self.pno_list_by_default = self.pno_list_by_default

        print 'start recommend for {}'.format(user_id)
        time_0 = time.time()
        min_return_count = self.config.getint('main', 'min_return_count')
        if once_return_count is None:
            once_return_count = self.config.getint('main', 'once_return_count')
        else:
            once_return_count = int(once_return_count)
            once_return_count = 100 if (once_return_count < 1 or once_return_count > 100) else once_return_count

        time_0 = time.time()
        try:
            self.ai_db_conn.ping()
        except:
            self.ai_db_conn = tool.mysql_ai_db_connect()
        try:
            self.wksc_db_conn.ping()
        except:
            self.wksc_db_conn = tool.mysql_db_connect()
        time_01 = time.time()
        print 'spend {}s in trying ping db'.format(time_01 - time_0)

        print 'start querying user behavior'
        # open lmdb cf
        txn = self.lmdb_env_user_behavior.begin()
        product_no_str = txn.get(str(user_id))

        user_behavior_pno_list = product_no_str.split(',') if product_no_str else None

        time_1 = time.time()
        print 'spend {}s in querying user behavior'.format(time_1 - time_01)

        # recommend_pno_by_item_based
        pno_list_by_embedding = model_embedding.recommend_pno_by_item_based(user_behavior_pno_list, self.config,
                                                                            self.item2vec, self.knn, self.item_dict)
        time_2 = time.time()
        print 'spend {}s in calculating embedding result'.format(time_2 - time_1)

        # recommend_pno_by_cf
        pno_list_by_cf = model_cf.recommend_pno_by_cf(user_id, self.config)
        time_3 = time.time()
        print 'spend {}s in collaborative filtering'.format(time_3 - time_2)

        # recommend_pno_by_default

        time_4 = time.time()
        print 'spend {}s in hot items'.format(time_4 - time_3)

        pno_list_by_embedding = list() if not pno_list_by_embedding else pno_list_by_embedding
        pno_list_by_cf = list() if not pno_list_by_cf else pno_list_by_cf
        pno_list_by_default = list() if not self.pno_list_by_default  else self.pno_list_by_default

        # ALL List
        pno_list_all = pno_list_by_embedding + pno_list_by_cf + pno_list_by_default
        pno_list_unique = data_pno.unique_pno_list_by_spu(pno_list_all, self.ai_db_conn, self.wksc_db_conn,
                                                          self.config)
        time_4_5 = time.time()
        print 'spend {}s in deduplicating by spu'.format(time_4_5 - time_4)

        result_pno_list = data_pno.sort_pno_list(pno_list_unique[:min_return_count], self.ai_db_conn,
                                                 self.wksc_db_conn, self.config)
        time_5 = time.time()
        print 'spend {}s in sorting'.format(time_5 - time_4_5)

        return self.sample_result(result_pno_list, once_return_count)


if __name__ == '__main__':
    t1 = time.time()
    for i in range(200):
        res = RecommendSystem.recommend_pno_for_user(str(1000000 + i), 20)
        print res[:5]
    print time.time() - t1

'''
    res = RecommendSystem.recommend_pno_for_user('990322')
    print res[:5]
    res = RecommendSystem.recommend_pno_for_user('1000013')
    print res[:5]
    res = RecommendSystem.recommend_pno_for_user('990322')
    print res[:5]
    '''
