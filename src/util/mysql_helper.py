# -*- coding: utf-8 -*-


import traceback

import MySQLdb
from functools import wraps
from MySQLdb.connections import Connection
from MySQLdb.cursors import Cursor


class MysqlConnection(Connection):
    def __init__(self, **kwargs):
        super(MysqlConnection, self).__init__(**kwargs)
        self.cursorclass = MysqlCursor
        self._info = kwargs

    def cursor(self, cursorclass=None):
        # try:
        #     self.ping()
        # except Exception as e:
        #     print e.message
        #     self.reconnect()

        return super(MysqlConnection, self).cursor(cursorclass)

    def reconnect(self):
        self.__init__(**self._info)
    def commit(self, *args, **kwargs):
        try:
            self.ping()
        except Exception as e:
            print e.message
            print traceback.format_exc()
            self.reconnect()
        return super(MysqlConnection, self).commit(*args, **kwargs)

    @classmethod
    def connect(cls, **kwargs):
        return cls(**kwargs)


class MysqlCursor(Cursor):
    def __init__(self, connection):
        super(MysqlCursor, self).__init__(connection)
        # self.conn = connection

    def check_closed(self):
        # if self.connection is None:
        #     self.__init__(self.conn)
        #     return False
        # else:
        #     return True
        return True

    # def check_connection(self, function):
    #     @wraps(function)
    #     def new_func(self, *args, **kwargs):
    #         try:
    #             self.connection.ping()
    #         except Exception as e:
    #             # TODO : logging
    #             print e.message
    #             print traceback.format_exc()
    #             self.connection.reconnect()
    #         finally:
    #             return function(self, *args, **kwargs)
    #
    #     return new_func
    #

    # @self.check_connection
    def execute(self, query, args=None):
        try:
            self.check_closed()
            self.connection.ping()
        except Exception as e:
            # TODO : logging
            print e.message
            print traceback.format_exc()
            self.connection.reconnect()

        return super(MysqlCursor, self).execute(query, args=args)

    # @check_connection
    def fetchone(self):
        try:
            self.check_closed()
            self.connection.ping()
        except Exception as e:
            # TODO : logging
            print e.message
            print traceback.format_exc()
            self.connection.reconnect()

        return super(MysqlCursor, self).fetchone()

    # @check_connection
    def fetchall(self):
        try:
            self.check_closed()
            self.connection.ping()
        except Exception as e:
            # TODO : logging
            print e.message
            print traceback.format_exc()
            self.connection.reconnect()

        return super(MysqlCursor, self).fetchall()


