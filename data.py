from logger import logger
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import pandas as pd
from configs import config

class Data():
    def __init__(self,filename,date,user,aapz_number_type):
        self.filename = filename
        self.date = date
        self.user = user
        # 将收到的参数传入到config类中
        self.configs = config(filename=self.filename,date=self.date,user = self.user)
        self.aapz_number_type = aapz_number_type
        self.engine = None
        # config类初始化后，调用config类中的方法，读取对应的sql链接信息
        self.connect_url = self.configs.sql_engine_connect()
        # config类初始化后，调用config类中的方法，读取发票和银行回单的输入路径
        self.input_path = self.configs.get_file_path()
        # config类初始化后，调用config类中的方法，读取高频词列表
        self.High_frequency_words = self.configs.get_High_frequency_words()

    # 创建sql引擎
    def get_sql_engine(self):
        engine = create_engine(self.connect_url, echo=False)
        logger.info('当前数据库连接url:\n%s' % self.connect_url)
        self.engine = engine

    # 执行sql语句，返回执行结果
    def load_sql(self,sql):
        if self.engine == None:
            self.get_sql_engine()
        try:
            df = pd.read_sql_query(sql,self.engine)
            logger.info('执行sql语句成功:\n'+sql+';')
            logger.info('返回数据条数:' + str(df.shape[0]) + '条;')
            #logger.info(df[0:10])
            return df
        except:
            logger.info('执行sql语句失败:\n'+sql)
            return None

    # 插入数据到指定表格
    def insert_sql(self,df,tablename,dtype=None):
        if self.engine == None:
            self.get_sql_engine()
        try:
            df.to_sql(tablename,con = self.engine,if_exists='append',index=False,dtype=dtype)
            logger.info('执行插入语句成功，插入表格:%s,插入数据%s条',tablename,len(df))
        except:
            logger.info('执行插入语句失败')

    # 执行指定的删除语句
    def delete_sql(self,sql):
        if self.engine == None:
            self.get_sql_engine()
        try:
            self.engine.execute(sql)
            logger.info('执行删除语句成功%s',sql)
        except:
            logger.info('执行删除语句失败%s',sql)


    # 在指定表中，根据指定ID，将指定列修改成指定值
    def modify_sql(self,table,ID,column,value):
        # 对于输入的value值，需要根据数据类型进行处理
        # 若为字符串格式，需要在首尾增加单引号
        if isinstance(value,str):
            value = "'" + value + "'"
        sql = "update {table} set {column} = {value} where ID = {ID}"
        sql = sql.format(table=table, column=column, value=value, ID=ID)
        if self.engine == None:
            self.get_sql_engine()
        try:
            self.engine.execute(sql)
            logger.info('执行修改语句成功%s',sql)
        except:
            logger.info('执行修改语句失败%s',sql)
