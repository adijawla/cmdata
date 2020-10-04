import pyodbc

class to_db():
    def __init__(self):
        self.server = 'magdb.database.windows.net'
        self.database1 = 'input_dbs_updated'
        self.database2 = 'outputdb_updated'
        self.username = 'letmetry'
        self.password = 'Ins201799'
        self.driver= '{ODBC Driver 17 for SQL Server}'
    def inputstart(self):
        try:
            cnxn = pyodbc.connect('DRIVER='+self.driver+';PORT=1433;SERVER='+self.server+';PORT=1443;DATABASE='+self.database1+';UID='+self.username+';PWD='+ self.password)
            return cnxn
        except:
            raise "not connected"
    def outputstart(self):
        try:
            cnxn = pyodbc.connect('DRIVER='+self.driver+';PORT=1433;SERVER='+self.server+';PORT=1443;DATABASE='+self.database2+';UID='+self.username+';PWD='+ self.password)
            return cnxn
        except:
            raise "not connected"
