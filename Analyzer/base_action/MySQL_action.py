from .base import Base_Action
from logger import logger
import pandas as pd
import os


# 获取文件夹下全部文件路径
def getFlist(path):
    read_files_list = []
    for root, dirs, read_files in os.walk(path):
        read_files_list.append(read_files)  # 文件名称，返回list类型
    return read_files_list

# 定义银行回单表操作类
class MySQL_action_bank_receipt_abc(Base_Action):
    def __init__(self,Data):
        super().__init__(Data)

    # 提取该公司该账期下全部的银行回单数据
    def get_All_Data(self):
        logger.info('【提取该公司当前账期下全部的银行回单数据：开始】')
        # 生成sql查询语句
        get_bank_file_sql = "select * from bank_receipt_abc where User_full_name = '{User}' and Time = {date}"
        # 将属性中的当前用户和账期信息传入sql语句中
        get_bank_file_sql = get_bank_file_sql.format(User=self.user, date=self.date)
        # 执行sql语句
        self.bank_file = self.data.load_sql(sql=get_bank_file_sql)
        logger.info('【提取该公司当前账期下全部的银行回单数据：完成】')
        return self.bank_file

    def insert_InputData(self, FolderName_dic):
        logger.info('【读取任务文件夹中的银行回单数据：开始】')
        bank_file_path = getFlist(FolderName_dic['input_path_Bank'])
        bank_file_path = FolderName_dic['input_path_Bank']+'/'+ bank_file_path[0][0]
        logger.info('读取银行回单文件：%s',bank_file_path)
        bank_file = pd.read_excel(io= bank_file_path,skiprows=[0, 1],converters={'收入金额': float, '支出金额': float, '本次余额': float})
        bank_file = bank_file[:-2]
        bank_file.insert(loc=1, column='User_full_name', value=self.user)
        bank_file.insert(loc=2, column='Time', value=self.date)
        bank_file = bank_file.rename(columns={
                                                '交易日期': 'Trade_Time',
                                                '交易时间戳': 'Trade_Time_Tick',
                                                '收入金额': 'Income_Amount',
                                                '支出金额': 'Expend_Amount',
                                                '本次余额': 'Amount',
                                                '手续费总额': 'Commission',
                                                '交易方式': 'Trade_Mode',
                                                '交易行名': 'Counterparty_Bank',
                                                '交易类别': 'Trade_Type',
                                                '对方省市': 'Counterparty_Province',
                                                '对方账号': 'Counterparty_Account',
                                                '对方户名': 'Trade_Account_Name',
                                                '交易说明': 'Trade_Description',
                                                '交易摘要': 'Trade_Summary',
                                                '交易附言': 'Trade_Postscript',
                                                '交易凭证号': 'Trade_proof',
                                                '交易日志号': 'Trade_log',
                                                '账簿号': 'Account_Book_ID',
                                                '账簿名称': 'Account_Book_Name'
                                                })
        bank_file['Income_Amount'] = bank_file['Income_Amount'].astype('float')
        bank_file['Expend_Amount'] = bank_file['Expend_Amount'].astype('float')
        bank_file['Amount'] = bank_file['Amount'].astype('float')
        self.data.insert_sql(df=bank_file, tablename='bank_receipt_abc')
        logger.info('处理农业银行的银行回单完毕，数据已插入表bank_receipt_abc')
        logger.info('【读取任务文件夹中的银行回单数据：完成】')

# 定义交易表操作类
class MySQL_action_execution(Base_Action):
    def __init__(self,Data):
        super().__init__(Data)

    # 该方法用于将农业银行回单数据抽取到execution表中
    # 抽取转换逻辑为
    # 表名：execution
    # ID ：自增
    # User_full_name：透传
    # Time：透传
    # 原始凭证类型：‘1’
    # 原始凭证ID：bank_receipt_abc表ID
    # 回单流水号：bank_receipt_abc表Trade_log，加前缀ABC_ 示例：ABC_356050882
    # 交易对手方：bank_receipt_abc表Trade_Account_Name
    # 交易方向：取值银行回单：收款，付款；根据bank_receipt_abc表Income_Amount和Expend_Amout综合判断
    # 交易金额：根据交易方向，取bank_receipt_abc表Income_Amount或Expend_Amout
    # 交易标的：银行回单-无货物交易；
    # 备注：银行回单取交易说明+交易摘要+交易附言（Trade_Description+Trade_Summary+Trade_Postscript）
    def ETL_bank_receipt_abc_To_execution(self):
        logger.info('【查询当前公司当前账期的农业银行回单数据，抽取到execution表：开始】')
        sql=''' SELECT                
                     User_full_name                
                    ,Time                
                    ,'1' as 原始凭证类型                
                    ,ID as 原始凭证ID                
                    ,concat('ABC_',Trade_log) as 回单流水号                
                    ,Trade_Account_Name as 交易对手方
                    ,Trade_Time as 交易时间               
                    ,IF(Income_Amount <> 0,'收款','付款') as 交易方向                
                    ,IF(Income_Amount <> 0,Income_Amount,Expend_Amount) as 交易金额                
                    ,'无货物交易' as 交易标的                
                    ,concat(ifnull(Trade_Description,''),ifnull(Trade_Summary,''),ifnull(Trade_Postscript,'') ) as 备注                
                FROM auto_account.bank_receipt_abc 
                WHERE User_full_name = '{User}' and Time = {date}'''
        sql = sql.format(User=self.user,date=self.date)
        try:
            df = self.data.load_sql(sql=sql)
            self.data.insert_sql(df=df, tablename='execution')
        except:
            logger.info('从bank_receipt_abc中抽取数据插入到execution表失败，sql语句：%s',sql)
        logger.info('【查询当前公司当前账期的农业银行回单数据，抽取到execution表：结束】')

    def delete_All_Data(self):
        logger.info('【删除该公司当前账期下全部的银行回单交易数据：开始】')
        # 生成sql查询语句
        delete_execution_sql = "DELETE from execution where User_full_name = '{User}' and Time = {date} and 原始凭证类型 = '1'"
        # 将属性中的当前用户和账期信息传入sql语句中
        delete_execution_sql = delete_execution_sql.format(User=self.user, date=self.date)
        # 执行sql语句
        self.data.delete_sql(sql=delete_execution_sql)
        logger.info('【删除该公司当前账期下全部的银行回单交易数据：完成】')