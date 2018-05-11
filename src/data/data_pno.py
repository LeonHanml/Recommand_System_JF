# -*- coding: utf-8 -*-


def unique_pno_list_by_spu(self, pno_list, ai_db_conn, wksc_db_conn,config):
    """
    通过spu获取唯一的产品编号pno 列表
    wksc 万卡商城
    :param self: 
    :param pno_list: 
    :param ai_db_conn: 
    :param wksc_db_conn: 
    :param config: 
    :return: 
     
    product_spu:
    spu :
    qmarket.quality_product:
    """
    ai_cursor = ai_db_conn.cursor()
    wksc_cursor = wksc_db_conn.cursor()

    pno_list_str = '("' + '","'.join(pno_list) + '")'
    # 查询 product_no , spu
    sql_format = "select product_no, spu from product_spu where product_no in %s" % pno_list_str

    ai_cursor.execute(sql_format)
    rows = ai_cursor.fetchall()

    result_list = list(set([row[0] for row in rows if row]))
    spu_set = set([row[1] if row and row[1] and (row[1] != 'NULL') else ('NoSpu' + row[0] if row else None) for row in rows])

    result_list_str = '("' + '","'.join(result_list) + '")'
    filter_sql = "select product_no " \
                 "from qmarket.quality_product " \
                 "where product_no in %s " \
                 "and state=1 " \
                 "and jd_state=1" % result_list_str

    wksc_cursor.execute(filter_sql)
    print 'before filter there are {} items'.format(len(result_list))
    rows = wksc_cursor.fetchall()
    result_list = [row[0] for row in rows if row]
    print 'after filter there are {} items'.format(len(result_list))
    return result_list

@classmethod
def sort_pno_list(self, pno_list, ai_db_conn, wksc_db_conn, config):
    """
    对产品编号进行排序
    score 为 点击 喜爱 销售等加权之和
    排序权重  
    w_clicks = 0.6
    w_sales = 0.2
    w_favorite = 0.1
    w_carts = 0.1
    Table
    product_sort_parameters：
    
    :param pno_list: 
    :return: 
    """
    w_clicks = config.getfloat('sort', 'w_clicks')
    w_sales = config.getfloat('sort', 'w_sales')
    w_favorite = config.getfloat('sort', 'w_favorite')
    w_carts = config.getfloat('sort', 'w_carts')

    cursor = ai_db_conn.cursor()
    sql_select_format = "select product_no , %s * sum_clicks + %s * sales_volume + %s * favorite + %s * carts as score " \
                        "from product_sort_parameters " \
                        "where product_no " \
                        "in %s" % (w_clicks, w_sales, w_favorite, w_carts, '%s')
    # 已经限定了 pno_list  后面还有过滤？ 还是全量计算后在过滤
    pno_list_str = '("' + '","'.join(pno_list) + '")'
    cursor.execute(sql_select_format % pno_list_str)

    rows = cursor.fetchall()
    row_dict = {row[0]: row[1] for row in rows}

    sort_list = [(pno,row_dict[pno]) if pno in row_dict else (pno, 0) for pno in pno_list ]
    return map(lambda x: x[0], sorted(sort_list, key=lambda x: x[1], reverse=True))