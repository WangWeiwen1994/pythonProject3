from .base import Base_Action
from logger import logger
import pandas as pd
import os
from dbfread import DBF


# 获取文件夹下全部文件路径
def getFlist(path):
    read_files_list = []
    for root, dirs, read_files in os.walk(path):
        read_files_list.append(read_files)  # 文件名称，返回list类型
    return read_files_list

# 读取DBF文件
def read_DBF(filename):
    data = DBF(filename, encoding='GBK')
    df = pd.DataFrame(iter(data))
    return df

# 根据输入的条件，在分析逻辑表中筛选出相应的操作代码
# 已移植到到Match_action_analyze中，待删除
    # 该函数逻辑为，根据每一笔交易中的元素，查找对应的逻辑
    # 示例：
    # 当前交易为银行流水，用户收到一笔来在XXXX公司的款项，随后查找有无逻辑可同时满足上述的用户，原始凭证类型，交易对手方，交易方向。
    # 如能够匹配，则返回逻辑记录的操作ID
    # 如不能匹配，则返回NO_Matched_Action
def action_match(analyze_file,User_full_name,type, opposite,side,symbool):
    result = analyze_file
    result = result.loc[(result['User_full_name'] == User_full_name) & (result['原始凭证类型'] == type) & (result['原始凭证类型'] == type) & (result['交易对手方'] == opposite) & (result['交易方向'] == side) & (result['交易标的'] == symbool)]
    result = result.reset_index(drop=True)
    if len(result['操作']) == 1:
        return result['操作'][0]
    else:
        result = 'NO_Matched_Action_原始凭证类型=%s_交易方向=%s_交易标的=%s' %(type, side, symbool)
        return result

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
        bank_file = self.data.load_sql(sql=get_bank_file_sql)
        logger.info('【提取该公司当前账期下全部的银行回单数据：完成】')
        return bank_file

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

    # 提取该公司该账期下全部的交易表数据
    def get_All_Data(self):
        logger.info('【提取该公司当前账期下全部的交易表数据：开始】')
        # 生成sql查询语句
        get_exectuion_sql = "select * from execution where User_full_name = '{User}' and Time = {date}"
        # 将属性中的当前用户和账期信息传入sql语句中
        get_exectuion_sql = get_exectuion_sql.format(User=self.user, date=self.date)
        # 执行sql语句
        exectuion = self.data.load_sql(sql=get_exectuion_sql)
        logger.info('【提取该公司当前账期下全部的交易表数据：完成】')
        return exectuion

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

    # 遍历每一笔交易,根据逻辑表判断对应的操作,在操作表中打上操作代码
    # 已移植到到Match_action_analyze中，待删除
    def action_match_v0001(self,execution,analyze_file):
        # 遍历每一笔交易,根据逻辑表判断对应的操作
        logger.info('【匹配操作v0001：开始】')
        df = execution
        # 该函数逻辑为，根据每一笔交易中的元素，查找对应的逻辑
        # 示例：
        # 当前交易为银行流水，用户收到一笔来在XXXX公司的款项，随后查找有无逻辑可同时满足上述的用户，原始凭证类型，交易对手方，交易方向。
        # 如能够匹配，则返回逻辑记录的操作ID
        # 如不能匹配，则返回NO_Matched_Action
        df['操作'] = df.apply(lambda x : action_match(analyze_file=analyze_file,User_full_name=x['User_full_name'],type=x['原始凭证类型'],opposite=x['交易对手方'],side=x['交易方向'],symbool=x['交易标的']), axis=1)
        # 删除execution表中同公司同账期全部的银行回单交易数据
        self.delete_All_Data()
        # 将计算结果插入execution表中
        self.data.insert_sql(df=df, tablename='execution')
        logger.info('【匹配操作v0001：结束】')
        return df


    # 定义方法，先删除再插入,以实现更新效果
    def insert_after_delete(self,df):
        self.delete_All_Data()
        self.data.insert_sql(df=df, tablename='execution')

# 定义科目表操作类
class MySQL_action_balance(Base_Action):
    def __init__(self, Data):
        super().__init__(Data)
    # 删除该公司当前账期下全部的科目表数据
    def delete_All_Data(self):
        logger.info('【删除该公司当前账期下全部的科目表数据：开始】')
        # 生成sql查询语句
        sql = "DELETE FROM auto_account.balance WHERE User_full_name = '{User}'"
        # 将属性中的当前用户和账期信息传入sql语句中
        sql = sql.format(User=self.user)
        # 执行sql语句
        self.data.delete_sql(sql=sql)
        logger.info('【删除该公司当前账期下全部的科目表数据：完成】')

    # 该方法专用于读取科目表信息，若从任务文件夹中读取到科目余额表，则覆盖写入balance表中
    def insert_InputData(self,FolderName_dic):
        logger.info('【从任务文件夹读取科目表：开始】')
        try:
            balance_file_path = getFlist(FolderName_dic['input_path_Balance'])
            balance_file_path = FolderName_dic['input_path_Balance']+'/'+ balance_file_path[0][0]
            logger.info('读取科目余额表文件：%s',balance_file_path)
            balance_file = read_DBF(balance_file_path)
            balance_file = balance_file[['LB', 'MXKMS', 'JFKM', 'DFKM', 'JD', 'JZ', 'KMMC']]
            balance_file.insert(loc=0,column='User_full_name',value=self.user)
            # 删除已有的科目余额表
            self.delete_All_Data()
            # 将文件数据插入数据库
            self.data.insert_sql(df=balance_file, tablename='balance')
            logger.info('科目表已根据输入数据更新')
            logger.info('【从任务文件夹读取科目表：完成】')
            return balance_file
        except:
            logger.info('未读取到科目表信息')
            balance_file = pd.DataFrame()
            logger.info('【从任务文件夹读取科目表：完成】')
            return balance_file

    # 提取该公司该账期下全部的科目表数据
    def get_All_Data(self):
        logger.info('【提取该公司当前账期下全部的科目表数据：开始】')
        # 生成sql查询语句
        get_balance_sql = "select * from balance where User_full_name = '{User}'"
        # 将属性中的当前用户和账期信息传入sql语句中
        get_balance_sql = get_balance_sql.format(User=self.user, date=self.date)
        # 执行sql语句
        balance = self.data.load_sql(sql=get_balance_sql)
        logger.info('【提取该公司当前账期下全部的科目表数据：完成】')
        return balance

# 定义名称对照表操作类
class MySQL_action_name_comparative_table(Base_Action):
    def __init__(self, Data):
        super().__init__(Data)

    # 提取该公司全部的公司名称对照表数据
    def get_All_Data(self):
        logger.info('【提取该公司全部的公司名称对照表数据：开始】')
        # 生成sql查询语句
        get_name_comparative_file = "select * from auto_account.name_comparative_table where User_full_name = '{User}'"
        # 将属性中的当前用户和账期信息传入sql语句中
        get_name_comparative_file = get_name_comparative_file.format(User=self.user)
        # 执行sql语句
        name_comparative_file = self.data.load_sql(sql=get_name_comparative_file)
        logger.info('【提取该公司全部的公司名称对照表数据：完成】')
        return name_comparative_file

# 定义名称分析逻辑表操作类
class MySQL_action_analyze(Base_Action):
    def __init__(self, Data):
        super().__init__(Data)

    # 提取该公司全部的分析逻辑表数据
    def get_All_Data(self):
        logger.info('【提取该公司全部的分析逻辑表数据：开始】')
        # 生成sql查询语句
        get_analyze_sql = "select * from auto_account.analyze where User_full_name = '{User}'"
        # 将属性中的当前用户和账期信息传入sql语句中
        get_analyze_sql = get_analyze_sql.format(User=self.user)
        # 执行sql语句
        analyze_file = self.data.load_sql(sql=get_analyze_sql)
        logger.info('【提取该公司全部的分析逻辑表数据：完成】')
        return analyze_file

# 定义名称分析逻辑表操作类
class MySQL_action_action(Base_Action):
    def __init__(self, Data):
        super().__init__(Data)

    # 提取该公司全部的操作表数据
    def get_All_Data(self):
        logger.info('【提取该公司全部的操作表数据：开始】')
        # 生成sql查询语句
        get_action_sql = "select * from auto_account.action"
        # 执行sql语句
        action_file = self.data.load_sql(sql=get_action_sql)
        logger.info('【提取该公司全部的操作表数据：完成】')
        return action_file

# 定义aapz记账凭证表操作类
class MySQL_action_aapz(Base_Action):
    def __init__(self, Data):
        super().__init__(Data)
    # 删除该公司当前账期下全部的科目表数据
    def delete_All_Data(self):
        logger.info('【删除该公司当前账期下全部的记账凭证数据：开始】')
        # 生成sql查询语句
        sql = "DELETE FROM auto_account.aapz where User_full_name = '{User}' and Time = {date}"
        # 将属性中的当前用户和账期信息传入sql语句中
        sql = sql.format(User=self.user, date=self.date)
        # 执行sql语句
        self.data.delete_sql(sql=sql)
        logger.info('【删除该公司当前账期下全部的记账凭证数据：完成】')

    # 定义方法，先删除再插入,以实现更新效果
    def insert_after_delete(self,df):
        self.delete_All_Data()
        self.data.insert_sql(df=df, tablename='aapz')

    # 提取该公司该账期下全部的记账凭证数据
    def get_All_Data(self):
        logger.info('【提取该公司全部的记账凭证数据：开始】')
        # 生成sql查询语句
        sql = "select * from aapz where User_full_name = '{User}' and Time = {date}"
        # 将属性中的当前用户和账期信息传入sql语句中
        sql = sql.format(User=self.user, date=self.date)
        aapz = self.data.load_sql(sql=sql)
        logger.info('【提取该公司全部的记账凭证数据：完成】')
        return aapz


# 定义银行回单表操作类
class MySQL_action_user_information(Base_Action):
    def __init__(self,Data):
        super().__init__(Data)

    # 提取该公司全部的银行回单数据
    def get_All_Data(self):
        logger.info('【提取该公司全部的用户信息：开始】')
        # 生成sql查询语句
        sql = "select * from user_information where User_full_name = '{User}'"
        # 将属性中的当前用户和账期信息传入sql语句中
        sql = sql.format(User=self.user)
        # 执行sql语句
        df = self.data.load_sql(sql=sql)
        logger.info('【提取该公司全部的用户信息：完成】')
        return df

    # 提取该公司工资账户信息
    def get_Salary_Bankaccount(self):
        logger.info('【提取该公司工资发放账户：开始】')
        df = self.get_All_Data()
        result = df['Salary_Bankaccount'][0]
        result = result.split(',')
        logger.info('该公司工资发放账户信息：%s' % result)
        logger.info('【提取该公司工资发放账户：完成】')
        return result