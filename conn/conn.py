import psycopg2  # POSTGRE DRIVER
from psycopg2.extras import RealDictCursor
from colorama import Fore

class Conexion:

    host     = "localhost"
    database = "sayhuite"
    user     = "postgres"
    password = "ROOT"
    conn     = ""
    cur      = ""

    #def __init__(self):
        #self.connect()

    def connect(self):
        # Connect to DATABASE
        try:
            self.conn = psycopg2.connect(host = self.host,
                                     database=self.database,
                                     user=self.user,
                                    password=self.password,
                                    port = 5432)

            print("Conectado a " + str(self.database) + "!!" )
        except:
            print('No Se Puede Establecer Conexion con la base de datos')
            #print "No Se Puede Establecer Conexion con la base de datos"
            exit(0)
        # Open a cursor to perform database operations
        self.cur = self.conn.cursor(cursor_factory=RealDictCursor)

    def close(self):
        self.conn.close()
        exit(0)

    def getConn(self):
        return self.conn

    def getCur(self):
        return self.cur
