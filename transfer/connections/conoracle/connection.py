import cx_Oracle


class ConnOracle(object):

    def __init__(self, server, port, service_name, user, password):
        self.__server = server
        self.__port = port
        self.__service_name = service_name
        self.__user = user
        self.__password = password
        self.__conn = None

        self.open_connection()

    def open_connection(self):
        dsn_tns = cx_Oracle.makedsn(self.__server, self.__port, service_name=self.__service_name)
        self.__conn = cx_Oracle.connect(user=self.__user, password=self.__password, dsn=dsn_tns)

    def execute_query(self, query):
        try:
            cursor = self.__conn.cursor()
            return cursor.execute(query)
        except cx_Oracle.Error as err:
            print(err)

    def __del__(self):
        self.__conn.close()
