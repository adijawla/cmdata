server   = 'magdb.database.windows.net'
database = 'input_db'
username = 'letmetry'
password = 'T@lst0y50'
driver   = '{ODBC Driver 17 for SQL Server}'

self.conn = pyodbc.connect(f'DRIVER={driver};PORT=1433;SERVER={server};PORT=1443;DATABASE={database};UID={username};PWD={password}')
        
##base tables
self.df1 = pd.read_sql_query(f"SELECT * FROM [dbo].[year]", self.conn)
self.df1.drop(['creation_date', 'updation_date'], axis=1,inplace=True
self.df2 = pd.read_sql_query(f"SELECT * FROM [dbo].[country]", self.conn)
self.df2.drop(['creation_date', 'updation_date'], axis=1,inplace=True)

self.df3a = pd.read_sql_query(f"SELECT * FROM [dbo].[currency]", self.conn)
self.df3a.drop(['creation_date', 'updation_date'], axis=1,inplace=True)

        
        ###Refrence tables
self.df3 = pd.read_sql_query(f"SELECT * FROM [dbo].[diesel_price_ex_incl_vat]", self.conn)
self.df3.drop(['creation_date', 'updation_date'], axis=1,inplace=True)

        self.df4 = pd.read_sql_query(f"SELECT * FROM [dbo].[diesel_price_rebate]", self.conn)
        self.df4.drop(['creation_date', 'updation_date'], axis=1,inplace=True)
        
        self.df5 = pd.read_sql_query(f"SELECT * FROM [dbo].[labour_price_rebates]", self.conn)
        self.df5.drop(['creation_date', 'updation_date'], axis=1,inplace=True)
        
        self.df6 = pd.read_sql_query(f"SELECT * FROM [dbo].[quartrly_price_labour]", self.conn)
        #self.df6.drop(['creation_date', 'updation_date'], axis=1,inplace=True)

        self.df7 = pd.read_sql_query(f"SELECT * FROM [dbo].[Water_Price_ex_inc_vat]", self.conn)
        #self.df7.drop(['creation_date', 'updation_date'], axis=1,inplace=True)

        self.df8 = pd.read_sql_query(f"SELECT * FROM [dbo].[Currency_n_FX]", self.conn)
        self.df8.drop(['creation_date', 'updation_date'], axis=1,inplace=True)

        self.df9 = pd.read_sql_query(f"SELECT * FROM [dbo].[Electricity_Explosives_Gasoline_LPG_Water_Rebates]", self.conn)
        self.df9.drop(['creation_date', 'updation_date'], axis=1,inplace=True)
        
        self.df10 = pd.read_sql_query(f"SELECT * FROM [dbo].[main_qrtly_input]", self.conn)
        #self.df10.drop(['creation_date', 'updation_date'], axis=1,inplace=True)
        
        self.df11 = pd.read_sql_query(f"SELECT * FROM [dbo].[prices_including_excluding-vat]", self.conn)
        #self.df11.drop(['creation_date', 'updation_date'], axis=1,inplace=True)
        
        self.df12 = pd.read_sql_query(f"SELECT * FROM [dbo].[VAT_Rates]", self.conn)
        #self.df12.drop(['creation_date', 'updation_date'], axis=1,inplace=True)