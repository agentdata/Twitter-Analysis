import pyodbc
class SQLConnection:
    
    def __init__(self, _server, _database, _username, _password):
        ## All the information you use to connect to your SQL Server database today
        self.server = _server
        self.database = _database
        self.username = _username
        self.password = _password
        self.connectionString = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + self.server + ';DATABASE=' + self.database
        self.connectionString = self.connectionString + ';UID=' + self.username + ';PWD=' + self.password

    def SQLOpenConnection(self):
        self.connection = pyodbc.connect(self.connectionString)
        self.openConnection = self.connection.cursor()

    def SQLCloseConnection(self):
        self.openConnection.close()
        self.connection.close()

    def SQLInsert(self, command, args = None):
        if(args == None):
            self.openConnection.execute(command)
        else:
            self.openConnection.execute(command, args)
        self.openConnection.commit()