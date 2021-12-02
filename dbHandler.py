import mysql.connector
import pandas as pd

class dbHandler:
    def __init__(self):
        #selfs initiate dbs under __init__
        #create statements to be reusable for function
        #under this is for db_datawarehouse
        self.create_sp = 'CREATE TABLE tb_stock_profile (symbol CHAR(10),\
        name VARCHAR(50) NOT NULL,\
        f_exchange CHAR(10),\
        sector CHAR(50),\
        link VARCHAR(100),\
        PRIMARY KEY (symbol))'

        self.create_n = 'CREATE TABLE tb_news (stock_news_id INT AUTO_INCREMENT,\
        symbol CHAR(10) NOT NULL,\
        news_date_published DATE NOT NULL,\
        news_news_title VARCHAR(1000),\
        news_content VARCHAR(100000),\
        news_link VARCHAR(1000),\
        PRIMARY KEY (stock_news_id),\
        FOREIGN KEY (symbol) REFERENCES tb_stock_profile(symbol))'

        self.create_shwh = 'CREATE TABLE tb_stock_history_wh (stockhist_id INT AUTO_INCREMENT,\
        symbol CHAR(10),\
        hist_date DATE NOT NULL,\
        hist_open_price DECIMAL(5,2),\
        hist_high_price DECIMAL(5,2),\
        hist_low_price DECIMAL(5,2),\
        hist_close_price DECIMAL(5,2),\
        hist_volume_avg BIGINT,\
        hist_chg_rate DECIMAL(5,2),\
        PRIMARY KEY (stockhist_id),\
        FOREIGN KEY (symbol) REFERENCES tb_stock_profile(symbol))'

        self.create_scvwh = 'CREATE TABLE tb_stock_compView_wh (compview_id INT AUTO_INCREMENT,\
        symbol CHAR(10),\
        cv_date_recorded DATE NOT NULL,\
        cv_52_week_high DECIMAL(5,2),\
        cv_52_week_low DECIMAL(5,2),\
        cv_market_cap BIGINT,\
        cv_shares_outstanding BIGINT,\
        cv_public_float BIGINT,\
        cv_beta DECIMAL(5,2),\
        cv_rev_per_employee BIGINT,\
        cv_pe_ratio DECIMAL(5,2),\
        cv_eps DECIMAL(5,2),\
        cv_yield DECIMAL(5,2),\
        cv_dividend DECIMAL(5,2),\
        cv_ex_dividend_date DATE,\
        cv_short_interest BIGINT,\
        cv_percent_float_shorted DECIMAL(5,2),\
        cv_average_volume BIGINT,\
        PRIMARY KEY (compview_id),\
        FOREIGN KEY (symbol) REFERENCES tb_stock_profile(symbol))'

        self.dw_list={'tb_stock_profile':self.create_sp,'tb_news':self.create_n,'tb_stock_history_wh':self.create_shwh,'tb_stock_compView_wh':self.create_scvwh}

        #the ones under here are for db_lakes

        self.create_ovl = 'CREATE TABLE tb_overview_l (symbol CHAR(10),\
        overview_date DATE,\
        overview_open VARCHAR(100),\
        overview_day_range VARCHAR(100),\
        overview_52_week_range VARCHAR(100),\
        overview_market_cap VARCHAR(100),\
        overview_shares_outstanding VARCHAR(100),\
        overview_public_float VARCHAR(100),\
        overview_beta VARCHAR(100),\
        overview_rev_per_employee VARCHAR(100),\
        overview_pe_ratio VARCHAR(100),\
        overview_eps VARCHAR(100),\
        overview_yield VARCHAR(100),\
        overview_dividend VARCHAR(100),\
        overview_ex_dividend_date VARCHAR(100),\
        overview_short_interest VARCHAR(100),\
        overview_percent_of_float_shorted VARCHAR(100),\
        overview_average_volume VARCHAR(100),\
        overview_statusNow VARCHAR(100),\
        overview_priceNow VARCHAR(100),\
        overview_volumeNow VARCHAR(100),\
        overview_lastpriceNow VARCHAR(100))'

        self.create_nl = 'CREATE TABLE tb_news_l (comp_name VARCHAR(350),\
        news_date_published DATE,\
        news_title VARCHAR(500),\
        news_link VARCHAR(1000))'

        self.dl_list={'tb_overview_l':self.create_ovl,'tb_news_l':self.create_nl}

        #datamart db

        self.create_sa = 'CREATE TABLE tb_sector_analysis (sa_id INT AUTO_INCREMENT,\
        sector_name CHAR(50) NOT NULL,\
        date_recorded DATE,\
        comp_count INT,\
        average_price decimal(5,2),\
        sma_calc DECIMAL(5,2),\
        lowest_chg DECIMAL(5,2),\
        highest_chg DECIMAL(5,2),\
        average_chg DECIMAL(5,2),\
        public_held DECIMAL(5,2),\
        ave_pe_ratio DECIMAL(5,2),\
        lowest_pe_ratio DECIMAL(5,2),\
        highest_pe_ratio DECIMAL(5,2),\
        ave_eps DECIMAL(5,2),\
        lowest_eps DECIMAL(5,2),\
        highest_eps DECIMAL(5,2),\
        ave_yield DECIMAL(5,2),\
        ave_dividend DECIMAL(5,2),\
        ave_short_interest BIGINT,\
        average65_volume BIGINT,\
        sma10_volume BIGINT,\
        avechg_volume BIGINT,\
        lowest_price DECIMAL(5,2),\
        highest_price DECIMAL(5,2),\
        no_news_per_month INT,\
        positive_news_percent DECIMAL(5,2),\
        negative_news_percent DECIMAL(5,2),\
        PRIMARY KEY (sa_id))'

        self.dm_list={'tb_sector_analysis':self.create_sa}
        #collection for each usage
        
        self.db_checkList = {'db_datalake':self.dl_list,'db_datawarehouse':self.dw_list,'db_datamart':self.dm_list}
        
        self.host = 'localhost'
        self.user='root'
        self.password='sql05CB2021'
        
    def conn_handshake(self):
        db_checkList = self.db_checkList
        dbs_avail = []
        dbs_non_ext = []
        try:
            mydb = mysql.connector.connect(host=self.host,user=self.user,password=self.password)
            myCursor = mydb.cursor()
            myCursor.execute('SHOW DATABASES')
            dbs_avail = [i[0] for i in myCursor]
            for x in list(db_checkList.keys()):
                if x in dbs_avail:
                    myTb = mysql.connector.connect(host=self.host,user=self.user,password=self.password,database=x)
                    myCursorTable = myTb.cursor()
                    myCursorTable.execute('SHOW TABLES')
                    tablesAvail = [i[0] for i in myCursorTable]
                    for i in db_checkList[x].keys():
                        try:
                            if i in tablesAvail:
                                continue
                            else:
                                myCursorTable.execute(db_checkList[x][i])
                        except mysql.connector.Error as er:
                            continue
                    myCursorTable.close()
                else:
                    myDb2 = mysql.connector.connect(host=self.host,user=self.user,password=self.password)
                    myCursorDb = myDb2.cursor()
                    myCursorDb.execute('CREATE DATABASE {db}'.format(db=x))
                    myCursorDb.close()
                    myTb2 = mysql.connector.connect(host=self.host,user=self.user,password=self.password,database=x)
                    myCursorTb = myTb2.cursor()
                    for a in db_checkList[x].keys():
                        print(db_checkList[x][a])
                        myCursorTb.execute(db_checkList[x][a])
                    myCursorTb.close()
        except mysql.connector.Error as err:
            myCursor.close()
            return (False,'error: {er}'.format(er=err))
        myCursor.close()
        return (True,'compiled & created')
    
    def checkFetch_data(self,data_full,db_name,tb_name,id_cols=[]):
        #finds a dataset worth of records
        #returns a dataset if records are found and an empty list if not
        data_avail= []
        for i in data_full:
            if len(id_cols) == 1:
                execute = 'SELECT * FROM {tb_n} WHERE {col_chg} = {val}'.format(tb_n=tb_name,col_chg=id_cols[0],val=i[id_cols[0]])
            elif len(id_cols) == 2:
                execute = 'SELECT * FROM {tb_n} WHERE {col_chg} = {val} AND {col_chg2} ={val2}'.format(tb_n=tb_name,col_chg=id_cols[0],val=i[id_cols[0]],col_chg2=id_cols[1],val2=i[id_cols[1]])
            mydb = mysql.connector.connect(host=self.host,user=self.user,password=self.password,database=db_name)
            myCursor = mydb.cursor()
            myCursor.execute(execute)
            results = myCursor.fetchall()
            for res in results:
                data = {}
                for val,col in zip(res,myCursor.description):
                    data[col[0]] = val
                data_avail.append(data)
            myCursor.close()
        return data_avail
    
    def insert_data(self,data,database_name,table_name):
        #set order of 
        data_labels = list(data[0].keys())
        label_ins = ''
        for x in data_labels:
            label_ins += x+','
        data_full = []
        for i in data:
            data_full.append(tuple(i.values()))
        try:
            mydb = mysql.connector.connect(host=self.host,user=self.user,password=self.password,database=database_name)
            myCursor = mydb.cursor()
            execute = 'INSERT INTO {table_n} ({cols}) VALUES (%s,%s)'.format(table_n=table_name,cols=label_ins[:-1])
            myCursor.executemany(execute,data_full)
            mydb.commit()
            myCursor.close()
        except mysql.connector.Error as err:
            return False,err
        return True

if __name__ == '__main__':
    app = dbHandler()