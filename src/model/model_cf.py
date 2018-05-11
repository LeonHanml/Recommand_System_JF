# -*- coding: UTF-8 -*-
from ConfigParser import SafeConfigParser
import os
import lmdb
from collections import Counter
from ..util.tool import tool
from ..data import data_user


def recommend_pno_by_cf(self, user_id, config):
    # Collaborative Filtering CF
    max_similar_user_count = config.getint('cf', 'max_similar_user_count')
    max_recommend_pno_count = config.getint('cf', 'max_recommend_pno_count')
    ai_conn = tool.mysql_ai_db_connect()
    ai_cursor = ai_conn.ai_db_conn.cursor()
    sql_select_features_from_user_feature = \
        "select user_id, birth_year, gender, mobile_areacode, credit_line, usable_line " \
        "from user_feature where user_id = %s" % user_id
    ai_cursor.execute(sql_select_features_from_user_feature)

    feature_row_0 = ai_cursor.fetchone()
    if feature_row_0 is None:
        # print "No user_id in local user feature database"
        return None

    birth_year = feature_row_0[1]
    gender = feature_row_0[2]
    mobile_areacode = feature_row_0[3]
    credit_line = feature_row_0[4]
    usable_line = feature_row_0[5]

    # 获取当前用户id 相同的生日，性别，手机区号  定义相似用户  条件
    sql_format = "select user_id from user_feature where birth_year = '%s' and gender = %s and mobile_areacode = '%s'"
    ai_cursor.execute(sql_format % (birth_year, gender, mobile_areacode))

    recommend_pno_list = list()
    similar_user_count = 0

    user_id_row = ai_cursor.fetchone()  # user_id

    # 通过存储好的 用户行为数据 提取max_count 个商品 进行推荐   如何计算用户行为相似性！！！
    lmdb_env_user_behavior = lmdb.open(config.get('local_db', 'lmdb_file_user_behavior'), map_size=1e12)
    txn = lmdb_env_user_behavior.begin()

    while user_id_row and similar_user_count < max_similar_user_count:
        product_id_str = txn.get(str(user_id_row[0]))
        behavior_pno_list = product_id_str.split(',') if product_id_str else None
        if behavior_pno_list:
            similar_user_count += 1
            # for pno in behavior_pno_list:
            #     recommend_pno_list.append(pno)
            recommend_pno_list += behavior_pno_list

        user_id_row = ai_cursor.fetchone()  # !!!用while 循环fetch 多次查询数据库
    lmdb_env_user_behavior.close()
    recommend_pno_count_list = Counter(recommend_pno_list).most_common(max_recommend_pno_count)
    return map(lambda x: x[0], recommend_pno_count_list)
