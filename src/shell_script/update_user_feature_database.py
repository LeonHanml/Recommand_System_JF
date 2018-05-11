#!/usr/bin/python
# -*- coding: UTF-8 -*-

import json
import os
import re
import MySQLdb
import sqlite3
from ..util import logger
from ConfigParser import SafeConfigParser
from ..util.mysql_helper import MysqlConnection, MysqlCursor
from ..util.tool import tool

def _get_env_dict(conf_file='RecommendSystem.conf'):
    config = SafeConfigParser()
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), conf_file)
    config.read(path)
    #
    env_dict = dict()
    # env_dict['dbhost'] = config.get('database', 'dbhost')
    # env_dict['dbuser'] = config.get('database', 'dbuser')
    # env_dict['dbpasswd'] = config.get('database', 'dbpasswd')
    # env_dict['dbdb'] = config.get('database', 'dbdb')
    # env_dict['sqlite_file_user_feature'] = config.get('local_db', 'sqlite_file_user_feature')

    if not os.path.exists("logs"):
        os.mkdir("logs")
    log_name = os.path.join(os.getcwd(), "logs", config.get("log", "log_name"))
    # In debug mode, we want to keep the stream handler
    logger.set_file_handler(log_name,
                            config.getint("log", "max_size"),
                            config.getint("log", "backup_count"),
                            config.getboolean("log", "debug"))
    logger.set_level(config.get("log", "log_level"))

    return env_dict


def update_user_feature_database(conf_file='RecommendSystem.conf'):
    env_dict = _get_env_dict()
    wksc_conn = tool.mysql_db_connect()

    wksc_mysql_cursor = wksc_conn.cursor()

    # sqlite_conn = sqlite3.connect(env_dict['sqlite_file_user_feature'])
    ai_mysql_conn = tool.mysql_ai_db_connect()
    ai_mysql_conn.autocommit(on=True)
    ai_mysql_cursor = ai_mysql_conn.cursor()

    sql_create_user_feature = 'create table if not exists user_feature ' \
                              '(user_id bigint(11) primary key, ' \
                              'birth_year varchar(4) not null, ' \
                              'gender int not null, ' \
                              'mobile_areacode varchar(20) not null, ' \
                              'credit_line decimal(65,2), ' \
                              'usable_line decimal(65,2))'
    # sqlite_cursor.execute('drop table user_feature')
    ai_mysql_cursor.execute(sql_create_user_feature)
    ai_mysql_cursor.execute('drop table user_feature')
    ai_mysql_cursor.execute(sql_create_user_feature)
    ai_mysql_cursor.execute(
        "ALTER TABLE user_feature ADD INDEX user_feature_index (birth_year, gender, mobile_areacode)")

    # ai_mysql_cursor.execute('create index user_feature_index on user_feature (birth_year, gender, mobile_areacode)')

    sql_select_count = 'select count(*) from qmarket.quality_user'
    wksc_mysql_cursor.execute(sql_select_count)
    row_count = wksc_mysql_cursor.fetchone()[0]
    page_size = 100000
    page_count = (row_count - 1) / page_size + 1

    sql_format_0 = 'select id, id_number, ip_area, mobile_areacode, credit_line, usable_line ' \
                   'from qmarket.quality_user limit %s, %s'
    count_i = 0
    for page_idx in range(page_count):
        sql = sql_format_0 % (page_idx * page_size, page_size)
        print "test 1: %s" % page_idx
        wksc_mysql_cursor.execute(sql)
        print "test 2: %s" % page_idx
        row = wksc_mysql_cursor.fetchone()
        while (row):
            print 'count row : {}'.format(count_i)
            count_i += 1
            # 身份证字段存在和位数判断
            if len(row) == 6 and row[1] is not None and len(row[1]) == 18:
                birth_year = row[1][6:10]
                gender = int(row[1][-2]) % 2

                sql_format = "insert into user_feature (user_id, birth_year, gender, mobile_areacode, credit_line, usable_line) values(%s, '%s', %s, '%s', %s, %s)"
                ai_mysql_cursor.execute(sql_format % (row[0], birth_year, gender, row[3], row[4], row[5]))
            row = wksc_mysql_cursor.fetchone()
    # ai_mysql_conn.commit()


if __name__ == '__main__':
    update_user_feature_database()

