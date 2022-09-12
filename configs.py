import configparser
from logger import logger

class config:
   def __init__(self,filename,date,user):
       self.name = filename
       self.date = date
       self.user = user

    # 获取sql连接信息
   def sql_engine_connect(self):
       config = configparser.ConfigParser()
       config.read(self.name,encoding="utf-8")
       try:
           username= config.get("mysql",'username')
           host = config.get("mysql", 'host')
           password = config.get("mysql", 'password')
           port = str(config.get("mysql", 'port'))
           database = config.get("mysql", 'database')
       except:
           logger.info("提取配置文件信息发生错误，请检查配置文件名称和内容")
       connect_url = "mysql+pymysql://%s:%s@%s:%s/%s?charset=utf8mb4" %(username,password,host,port,database)
       return connect_url

   # 获取创建文件夹的配置路径
   def get_file_path(self):
       config = configparser.ConfigParser()
       config.read(self.name,encoding="utf-8")
       result= config.get("input_path",'path')
       return  result

   # 获取高频词信息
   def get_High_frequency_words(self):
       config = configparser.ConfigParser()
       config.read(self.name,encoding="utf-8")
       result= config.get("High_frequency_words",'HF_words')
       return  result
