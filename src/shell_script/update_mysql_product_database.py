# -*- coding: UTF-8 -*-

import MySQLdb
from ..util.mysql_helper import MysqlConnection, MysqlCursor
from ..util.tool import tool


def update_mysql_product_database(wksc_mysql_conn, ai_mysql_conn):
    if wksc_mysql_conn is None:
        wksc_mysql_conn = tool.mysql_db_connect()
    if ai_mysql_conn is None:
        # FIX sqlite
        # sqlite_conn = sqlite3.connect('/app/luzhicong/recommend_system/data/product.db')
        ai_mysql_conn = tool.mysql_ai_db_connect()
    wksc_mysql_cursor = wksc_mysql_conn.cursor()
    ai_mysql_cursor = ai_mysql_conn.cursor()

    sql_create_product_sort_parameters = "create table if not exists product_sort_parameters" \
                                         "(product_no varchar(255) primary key, " \
                                         "sum_clicks int(11), " \
                                         "sales_volume int(11), " \
                                         "favorite int(11), carts int(11))"

    sql_create_product_spu = "create table if not exists product_spu" \
                             "(product_no varchar(255) primary key, " \
                             "spu varchar(50))"

    ai_mysql_cursor.execute(sql_create_product_sort_parameters)
    ai_mysql_cursor.execute(sql_create_product_spu)
    ai_mysql_cursor.execute("ALTER TABLE product_spu ADD INDEX (spu)")

    # ai_mysql_cursor.execute("create index if not exists spu_idx on product_spu(spu)")
    # 清空
    ai_mysql_cursor.execute("delete from product_sort_parameters")
    ai_mysql_cursor.execute("delete from product_spu")

    wksc_mysql_cursor.execute(
        "select product_no, sales_volume, favorite, carts from qmarket_bigdata.bd_edw_ai_wk_search")
    sql_ai_insert_format = \
        "insert into product_sort_parameters(product_no, sum_clicks, sales_volume, favorite, carts) values('%s', 0, %s, %s, %s)"
    row = wksc_mysql_cursor.fetchone()
    count_i = 0
    # 拷贝数据库表
    while row:
        print 'count row : {}'.format(count_i)
        count_i += 1
        ai_mysql_cursor.execute(sql_ai_insert_format % row)
        row = wksc_mysql_cursor.fetchone()

    sql_quality_product_join_bd_edw_ai_wk_search = '''
		select 
			up_category_type, sum(t2.clicks) as sum_clicks
		from 
			qmarket.quality_product as t1
		join 
			qmarket_bigdata.bd_edw_ai_wk_search as t2
		on 
			t1.product_no = t2.product_no
		group by
			t1.up_category_type
	'''
    wksc_mysql_cursor.execute(sql_quality_product_join_bd_edw_ai_wk_search)
    up_category_clicks_dict = dict()
    # row [up_category_type,sum_clicks]
    for row in wksc_mysql_cursor.fetchall():
        up_category_clicks_dict[row[0]] = row[1]

    wksc_mysql_cursor.execute(
        "select product_no, up_category_type from qmarket.quality_product where jd_state=1 and up_category_type is not NULL and up_category_type <> ''")
    sql_sqlite_update_format = "update product_sort_parameters set sum_clicks = %s where product_no = '%s'"
    sql_ai_insert_format = "insert into product_sort_parameters(product_no, sum_clicks, sales_volume, favorite, carts) values('%s', %s, 0, 0, 0)"
    row = wksc_mysql_cursor.fetchone()  # product_no, up_category_type from qmarket.quality_product
    count_i = 0
    while row:
        print 'count row : {}'.format(count_i)
        count_i += 1
        seq = row[1].split(',')
        for up_category_type in seq:
            if up_category_type in up_category_clicks_dict and up_category_clicks_dict[up_category_type] != 0:
                ai_mysql_cursor.execute("select * from product_sort_parameters where product_no = '%s'" % row[0])
                pno = row[0]
                sum_clicks = up_category_clicks_dict[row[1]]
                if ai_mysql_cursor.fetchone():
                    ai_mysql_cursor.execute(sql_sqlite_update_format % (sum_clicks, pno))
                else:
                    ai_mysql_cursor.execute(sql_ai_insert_format % (pno, sum_clicks))
        row = wksc_mysql_cursor.fetchone()

    wksc_mysql_cursor.execute(
        "select t1.product_no, t2.spu from qmarket.quality_product as t1 join qmarket.quality_product_spu as t2 on t1.sku=t2.sku where t1.jd_state=1")
    sql_ai_insert_format = "replace into product_spu(product_no, spu) values('%s', '%s')"
    row = wksc_mysql_cursor.fetchone()
    count_i = 0
    while row:
        print 'count row : {}'.format(count_i)
        count_i += 1

        ai_mysql_cursor.execute(sql_ai_insert_format % row)
        row = wksc_mysql_cursor.fetchone()

    ai_mysql_conn.commit()
    ai_mysql_conn.close()
    wksc_mysql_conn.close()


if __name__ == '__main__':
    update_mysql_product_database(None, None)
    ai_mysql_conn = tool.mysql_ai_db_connect()
    ai_mysql_cursor = ai_mysql_conn.cursor()
    ai_mysql_cursor.execute("select * from product_sort_parameters")
    f_log = open('../out/log_sqlite_content.txt', 'w')
    for row in ai_mysql_cursor.fetchall():
        print >> f_log, "%s,%s,%s,%s,%s" % row
    ai_mysql_cursor.execute("select count(*) from product_spu")
    print ai_mysql_cursor.fetchone()
    ai_mysql_cursor.execute("select * from product_spu limit 20")
    for row in ai_mysql_cursor.fetchall():
        print row
