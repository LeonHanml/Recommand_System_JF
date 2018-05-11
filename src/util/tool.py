import MySQLdb
import os
import random
class tool(object):
    from ConfigParser import SafeConfigParser

    config = SafeConfigParser()
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'RecommendSystem.conf')
    config.read(path)
    db_host = config.get('database', 'dbhost')
    db_user = config.get('database', 'dbuser')
    db_passwd = config.get('database', 'dbpasswd')
    db_db = config.get('database', 'dbdb')
    db_db_bigdata = config.get('database', 'dbdb_bigdata')

    ai_host = config.get('ai_db', 'ai_host')
    ai_user = config.get('ai_db', 'ai_user')
    ai_passwd = config.get('ai_db', 'ai_passwd')
    ai_db_user_feature = config.get('ai_db', 'ai_db_user_feature')

    @staticmethod
    def mysql_db_connect(self):
        wksc_conn = MySQLdb.connect(host=self.db_host, user=self.db_user, passwd=self.db_passwd, db=self.db_db, charset='utf8')

        return wksc_conn
    @staticmethod
    def mysql_ai_db_connect(self):
        ai_db_conn = MySQLdb.connect(host=self.ai_host, user=self.ai_user, passwd=self.ai_passwd, db=self.ai_db_user_feature, charset='utf8')
        return ai_db_conn
    def mysql_pool(self):
        from DBUtils.PooledDB import PooledDB
        pool = PooledDB(MySQLdb,5,host=self.db_host, user=self.db_user, passwd=self.db_passwd, db=self.db_db, charset='utf8',port=3306 ) #5为连接池里的最少连接数

        # conn = pool.connection() #以后每次需要数据库连接就是用connection（）函数获取连接就好了
        # cur=conn.cursor()
        # SQL="select * from table1"
        # r=cur.execute(SQL)
        # r=cur.fetchall()
        # cur.close()
        # conn.close()

    def random_sample(self,L_list, n):
        # 随机获取  pno_list 中的 n个样本列表

        idx_list = sorted(random.sample(list(range(len(L_list))), n))
        return [L_list[i] for i in idx_list]