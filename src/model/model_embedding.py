# -*- coding: UTF-8 -*-

import os
import numpy as np


# user_behavior_pno_list


# def get_item_list_from_item2vec(item2vec):
#     m, d = item2vec.vectors.shape
#     item_dict = {item2vec.vocab(i).encode('ISO-8859-1'):i  for i in range(m)}
#
#     return item_dict

def recommend_pno_by_item_based(pno_list, config, item2vec, knn, item_dict):
    """
    通过导入w2v bin文件  获取 数据，然后通过近邻knn模型 选出最近的 相似性的商品，进行相似性推荐
    其中商品使用了w2v进行表示，来计算相似性
    :param self: 
    :param pno_list: 
    :param config: 
    :return: 
    """
    # 如果用户之前没有浏览记录，则不运用 knn 推荐
    if pno_list is None:
        return None
    # 模型中的商品dict，获取所有向量化后的 商品字典{商品id：序号}
    item_dict = item_dict

    # m, d = item2vec.vectors.shape
    # for i in range(m):
    #     pno_idx_dict[item2vec.vocab[i].encode('ISO-8859-1')] = i
    #   pno_idx_dict={ 商品id：序号}

    # 获取  需要推荐商品的序号 index list
    idx_list = [item_dict[pno] for pno in pno_list if pno in item_dict]
    if not idx_list:
        return None
    # 获取pno_list 中商品的 vector
    vec_list = [item2vec.vectors[idx] for idx in idx_list]

    # vacab is index   其
    pno_list_vec = item2vec.vectors

    # [[distance] ,[index]]  order by distance
    neighbors = knn.kneighbors(vec_list)

    # k = 30             neighbors[1] indexs neighbors[1][0] index
    if neighbors and len(neighbors[1]) and (len(neighbors[1][0])):
        neighbors_idx_list = np.unique(np.concatenate(neighbors[1], axis=0))
        neighbors_pno_list = [item2vec.vocab[idx].encode('ISO-8859-1') for idx in neighbors_idx_list if
                              idx not in idx_list]
        return neighbors_pno_list
    else:
        return None
