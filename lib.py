import psycopg2
import mysql.connector
import logging

class Prep:
    def __init__(self, table, logfile,user,password,host,port,database) :
        y=input("Enter the name of datebase:")
       
        if y.upper()=='POSTGRESQL':
            try:
                self.connection = psycopg2.connect(user=user, password=password, host=host, port=port, database=database)
                print("PostgreSQL Database is connected")
            except:
                print("Connecton Failure reconnect")
        elif y=='MYSQL':
            try:
                self.connection=mysql.connect(user=user, password=password, host=host, port=port, database=database)
                print("MYSQL Database is connected")
            except:
                print("Connection Failure")
        else:
            print("Incorrrect Value entered please restart the program")

        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self.table = table
        self.primary_key = ''
        self.logger = logging.getLogger(logfile)
        self.logger.setLevel(logging.INFO)
        self.handler = logging.FileHandler(logfile)
        self.handler.setLevel(logging.DEBUG)
        self.formatter = logging.Formatter('%(asctime)s - %(name)s - %(funcName)s - %(lineno)d - %(levelname)s - %(message)s')
        self.handler.setFormatter(self.formatter)
        self.logger.addHandler(self.handler)
    def create_table(self, values={'name': 'varchar(50)', 'address': 'varchar(50)', 'phone': 'int'}, pk=''):
        try:  
            self.primary_key = pk
            values[self.primary_key] = values[self.primary_key] + ' PRIMARY KEY'
            cursor=self.connection.cursor()
            res = []
            for i, j in values.items():
                res.append(str(i)+' '+str(j))
            query = f"""create table {self.table} ({', '.join(res)})"""
            print(query)
   
            cursor.execute(query)
            self.connection.commit()
            self.logger.info("table created successfully")
        except Exception as e:
            self.logger.error("Error creating table:", e)  
   

    def insert_table(self, query_details):
        try:
            cursor = self.connection.cursor()
            for i in query_details: 
                columns = ', '.join(i.keys())
                query_values = ', '.join([str(f"'{val}'") for val in i.values()])
                query = f"""insert into {self.table}({columns}) values({query_values})"""
                print(query)
                cursor.execute(query)
                self.connection.commit()
                self.logger.info("Table inserted successfully")
        except Exception as e:
            self.logger.info(f"Error while inserting table: {e}")


    def update_table(self, values):
        try:
         
            cursor = self.connection.cursor()
            res = []
            for i, j in values.items():
                res.append(str(i) + " = '" + str(j) + "'")
            set = ", ".join(res)
            query = f"update {self.table} set {set}"
       
            cursor.execute(query)
            self.connection.commit()
            self.logger.info( "Table updated successfully")
        except Exception as e:
            self.logger.info(f"Error while updating table: {e}")
            self.connection.rollback()

    def delete_table(self, values):
        try:

            cursor = self.connection.cursor()
            res= []
            for i, j in values.items():
                res.append(str(i) + " = '" + str(j) + "'")
            where = " and ".join(res)
            query = f"delete from {self.table} where {where}"
            cursor.execute(query)
            self.connection.commit()
            self.logger.info("table deleted successfully")
        except Exception as e:
            self.logger.error(f"Error while deleting table")

Prepobj=Prep ('table name', user='user', password='password', host='host', port='port', database='database')
Prepobj.create_table(pk='phone')
i = [{'name':'name','address':'address','phone': 'phone'},{'name':'name','address':'address','phone':' phone'},]
Prepobj.insert_table(i)
Prep.update_table({})
Prep.delete_table({'address': 'address'})
    