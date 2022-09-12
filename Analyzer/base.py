import pandas as pd
from dbfread import DBF
from logger import logger
import time
import os
import copy
import math
from copy import deepcopy

pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)
pd.set_option('display.max_rows',500)
pd.set_option('display.max_columns',500)
pd.set_option('display.width',1000)


class Analyzer():
    def __init__(self, Data):
        self.data = Data
        self.filename = Data.filename
        self.date = Data.date
        self.user = Data.user
        # 数据库连接引擎
        self.engine = Data.engine
        # 银行回单
        self.bank_file = pd.DataFrame()
        # 科目表
        self.balance_file = pd.DataFrame()
        # 名称对照表
        self.name_comparative_file = pd.DataFrame()
        # 交易信息
        self.execution = pd.DataFrame()
        # 分析逻辑表
        self.analyze = pd.DataFrame()
        # 操作执行表
        self.action = pd.DataFrame()
        # 用户银行账户会计科目
        self.bank_account = None
        # 记账凭证表
        self.aapz = None
        # 每次分析时生成的文件路径
        self.FolderName_dic = None
        # 'default' or 'bank in day'，生成凭证编号时,默认每一笔数据生成一张凭证，可选择将银行回单数据按天汇总
        self.aapz_number_type = Data.aapz_number_type

    # 读取DBF文件
    def read_DBF(self,filename):
        data = DBF(filename, encoding='GBK')
        df = pd.DataFrame(iter(data))
        return df
    # 输入用户校验
    def UserCheck(self):
        logger.info('开始输入用户校验：')
        df = self.data.load_sql("select * from auto_account.user_information where User_full_name = '%s'" %self.user)

        if len(df['User_full_name']) > 1:
            logger.info('该用户名存在多条结果，请检查表auto_account.user_information')
            exit()
        else:
            if df.empty == True:
                logger.info('该用户不存在')
                exit()
            else:
                self.bank_account = df['Bank_account'][0]
                logger.info('用户校验通过')


    # 创建文件夹
    def create_file(self,FolderName):
        # 获取取当前的时间、月份...
        time_1 = time.localtime()
        # 按照strtime的方法来格式化时间
        time_now = time.strftime("%Y-%m-%d-%H_%M_%S", time_1)
        FolderName = FolderName + time_now
        FolderName_dic = {}

        # 字符串拼接，组合成完整的文件路径
        Folder = FolderName + '/银行回单输入'
        if not os.path.exists(Folder):  # 是否存在这个文件夹
            os.makedirs(Folder)  # 如果没有这个文件夹，那就创建一个
        FolderName_dic['input_path_Bank'] = Folder
        Folder = FolderName + '/科目余额表输入'
        if not os.path.exists(Folder):  # 是否存在这个文件夹
            os.makedirs(Folder)  # 如果没有这个文件夹，那就创建一个
        FolderName_dic['input_path_Balance'] = Folder
        Folder = FolderName + '/凭证输出'
        if not os.path.exists(Folder):  # 是否存在这个文件夹
            os.makedirs(Folder)  # 如果没有这个文件夹，那就创建一个
        FolderName_dic['output_path'] = Folder
        self.FolderName_dic = FolderName_dic
        return FolderName_dic

    # 获取文件夹下全部文件路径

    def getFlist(self,path):
        read_files_list = []
        for root, dirs, read_files in os.walk(path):
            read_files_list.append(read_files) # 文件名称，返回list类型
        return read_files_list

    # 该方法专用于处理农业银行的银行回单
    def get_bank_file_ABC(self,FolderName_dic):
        bank_file_path = self.getFlist(FolderName_dic['input_path_Bank'])
        bank_file_path = FolderName_dic['input_path_Bank']+'/'+ bank_file_path[0][0]
        logger.info('读取银行回单文件：%s',bank_file_path)
        bank_file = pd.read_excel(io= bank_file_path,skiprows=[0, 1],converters={'收入金额':float,'支出金额':float,'本次余额':float})
        bank_file = bank_file[:-2]
        #bank_file.insert(loc=0, column='ID', value=None)
        bank_file.insert(loc=1,column='User_full_name',value=self.user)
        bank_file.insert(loc=2,column='Time',value=self.date)
        bank_file = bank_file.rename(columns = {
                                                '交易日期':'Trade_Time',
                                                '交易时间戳':'Trade_Time_Tick',
                                                '收入金额':'Income_Amount',
                                                '支出金额':'Expend_Amount',
                                                '本次余额':'Amount',
                                                '手续费总额':'Commission',
                                                '交易方式':'Trade_Mode',
                                                '交易行名':'Counterparty_Bank',
                                                '交易类别':'Trade_Type',
                                                '对方省市':'Counterparty_Province',
                                                '对方账号':'Counterparty_Account',
                                                '对方户名':'Trade_Account_Name',
                                                '交易说明':'Trade_Description',
                                                '交易摘要':'Trade_Summary',
                                                '交易附言':'Trade_Postscript',
                                                '交易凭证号':'Trade_proof',
                                                '交易日志号':'Trade_log',
                                                '账簿号':'Account_Book_ID',
                                                '账簿名称':'Account_Book_Name'
                                                })
        #bank_file['银行'] = '中国农业银行'
        bank_file['Income_Amount'] = bank_file['Income_Amount'].astype('float')
        bank_file['Expend_Amount'] = bank_file['Expend_Amount'].astype('float')
        bank_file['Amount'] = bank_file['Amount'].astype('float')
        logger.info('处理农业银行的银行回单完毕')
        self.data.insert_sql(df=bank_file, tablename='bank_receipt_abc')

    # 该方法专用于读取科目表信息
    def get_balance_file(self,FolderName_dic):
        try:
            balance_file_path = self.getFlist(FolderName_dic['input_path_Balance'])
            balance_file_path = FolderName_dic['input_path_Balance']+'/'+ balance_file_path[0][0]
            logger.info('读取科目余额表文件：%s',balance_file_path)
            balance_file = self.read_DBF(balance_file_path)
            balance_file = balance_file[['LB', 'MXKMS', 'JFKM', 'DFKM', 'JD', 'JZ', 'KMMC']]
            balance_file.insert(loc=0,column='User_full_name',value=self.user)
            logger.info(balance_file[0:10])
            # 删除已有的科目余额表
            sql = "DELETE FROM auto_account.balance WHERE User_full_name = '{User}'"
            sql = sql.format(User=self.user)
            self.data.delete_sql(sql)
            # 将文件数据插入数据库
            self.data.insert_sql(df=balance_file, tablename='balance')
            logger.info('科目表已根据输入数据更新')

        except:
            logger.info('未读取到科目表信息')





    # 对输入的字符串，每两个字符进行截取
    # 示例
    # name = '上海晨光科力普办公用品有限公司'
    # result = ['上海', '海晨', '晨光', '光科', '科力', '力普', '普办', '办公', '公用', '用品', '品有', '有限', '限公', '公司']
    def slice(self, name):
        a = 0
        b = 2
        l = len(name)
        result = []
        while a < l - 1:
            result.append(name[a:b])
            a = a + 1
            b = b + 1
        return result

    # 计算输入的两个字符串，两个字符串去重后重合元素的数量
    # 示例
    # a = ['上海', '海晨', '晨光', '光科', '科力', '力普', '普办', '办公', '公用', '用品', '品有', '有限', '限公', '公司']
    # b = ['晨光', '光科', '科力', '力普', '普（', '（上', '上海', '海）', '）办', '办公', '公用', '用品', '品有', '有限', '限公', '公司']
    # result = 12

    def inner_count(self,a, b):
        set_c = set(a) & set(b)
        list_c = list(set_c)
        result = len(list_c)
        return result

    # 该方法用于对公司名称进行处理，找到科目余额表中对应的科目
    def find_name(self,name,JFKM):
        Full_Name = copy.copy(name)

        # 读取配置文件中的高频词，对输入的名称进行去除，提高准确率
        HF_words = self.data.High_frequency_words
        HF_words = HF_words.split(',')
        for word in HF_words:
            name = name.replace(word,'')

        # 将去除高频词的名称进行分解
        name = self.slice(name)

        # 与科目余额表中的名称进行比对，找出重合度最高的数据
        balance_file=self.balance_file
        # 对JFKM字段中，取前4位（即借方科目），进行筛选
        balance_file = balance_file[balance_file['JFKM'].map(lambda x:x[0:4])==JFKM]
        balance_file['cal'] = balance_file['KMMC'].apply(lambda x: self.slice(x))
        balance_file['cal_count'] = balance_file['cal'].apply(lambda x: self.inner_count(name, x))
        max_count = balance_file['cal_count'].max()
        result = balance_file.loc[balance_file['cal_count'] == max_count]
        result = result[['User_full_name','JFKM', 'DFKM','KMMC']]
        result['Full_Name'] = Full_Name
        result['一级科目'] = result.apply(lambda x : x['JFKM'][0:4],axis=1)
        result = result[0:5]
        result = result.reset_index(drop=True)
        logger.info('请输入对应的行号，无对应结果请输入N\n查询结果如下:')
        logger.info(result)
        r = input()
        if r == 'N':
            result = pd.DataFrame()
            logger.info(Full_Name+'当前无对应科目')
        else:
            result = result.loc[r:r]
            logger.info(Full_Name + '找到对应关系')
            self.data.insert_sql(df=result, tablename='name_comparative_table')
        return result

    # 该方法用于将银行回单数据抽取到execution表中，仅在银行回单有插入时执行
    # 抽取转换逻辑为
    # 表名：execution
    # ID ：自增
    # User_full_name：透传
    # Time：透传
    # 原始凭证类型：‘1’
    # 原始凭证ID：bank_receipt_abc表ID
    # 银行回单日志号：bank_receipt_abc表Trade_log
    # 交易对手方：bank_receipt_abc表Trade_Account_Name
    # 交易方向：取值银行回单：收款，付款；根据bank_receipt_abc表Income_Amount和Expend_Amout综合判断
    # 交易金额：根据交易方向，取bank_receipt_abc表Income_Amount或Expend_Amout
    # 交易标的：银行回单-无货物交易；
    # 备注：银行回单取交易说明+交易摘要+交易附言（Trade_Description+Trade_Summary+Trade_Postscript）
    def ETL_bank_receipt_abc_To_execution(self):
        sql=''' SELECT                
                     User_full_name                
                    ,Time                
                    ,'1' as 原始凭证类型                
                    ,ID as 原始凭证ID                
                    ,Trade_log as 农行回单日志号                
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



    # 提取数据库中已有的名称对照表信息
    def get_name_comparative_file(self):
        get_name_comparative_file = "select * from auto_account.name_comparative_table where User_full_name = '{User}'"
        get_name_comparative_file = get_name_comparative_file.format(User=self.user)
        logger.info('提取数据库中已有的名称对照表信息')
        self.name_comparative_file = self.data.load_sql(sql = get_name_comparative_file)


    # 提取数据库中的交易信息
    def get_execution(self):
        get_exectuion_sql = "select * from execution where User_full_name = '{User}' and Time = {date}"
        get_exectuion_sql = get_exectuion_sql.format(User=self.user, date=self.date)
        self.execution = self.data.load_sql(sql=get_exectuion_sql)

    # 提取数据库中的分析逻辑表
    def get_analyze(self):
        get_analyze_sql = "select * from auto_account.analyze where User_full_name = '{User}'"
        get_analyze_sql = get_analyze_sql.format(User=self.user)
        logger.info('提取数据库中已有的分析逻辑信息')
        self.analyze = self.data.load_sql(sql = get_analyze_sql)

    # 提取数据库中的操作表
    def get_action(self):
        get_action_sql = "select * from auto_account.action"
        logger.info('提取数据库中已有的操作信息')
        self.action = self.data.load_sql(sql = get_action_sql)

    # 提取数据库中的记账凭证表
    def get_aapz(self):
        get_aapz_sql = "select * from auto_account.aapz where User_full_name = '{User}' and Time = {date}"
        get_aapz_sql = get_aapz_sql.format(User=self.user, date=self.date)
        logger.info('提取数据库中已有的记账凭证表')
        self.aapz = self.data.load_sql(sql = get_aapz_sql)

    # 该函数用于生成凭证数据
    # 首先对交易表和操作表进行左连接
    # 随后对结果进行清洗，主要是计算金额和查找对应二级科目
    # 接着将凭证整理成记账凭证的格式插入到aapz表中
    def aapz_manager(self):
        # 将交易表和操作表进行左连接，连接键为‘操作’
        result = pd.merge(left=self.execution, right=self.action,how='left',left_on='操作',right_on='操作')
        # 根据操作中配置的取值字段，读取对应的每一行取值数据
        result['取值'] = result.apply(lambda x:x[x['取值']],axis=1)
        # 提取生成凭证涉及的字段，去除无关字段
        result = result[['User_full_name','Time','交易对手方','交易时间','交易方向','备注','凭证编号','HS','ZY','JFKM','JFMC','JFJE','DFJE','取值','是否关联二级科目']]
        # 生成凭证中的日期格式 20220630 变为 2022/6/30
        def funshion_01(time):
            year = time[0:4]
            if time[4] == '0':
                month = time[5]
            else:
                month = time[4:6]
            if time[6] == 0:
                day = time[7]
            else:
                day = time[6:8]
            result = str(year)+'/'+str(month)+'/'+str(day)
            return result
        result['RQ'] =result.apply(lambda x : funshion_01(x['Time']), axis=1)
        result['JFJE'] = result.apply(lambda x: x['取值'] * x['JFJE'], axis=1)
        result['DFJE'] = result.apply(lambda x: x['取值'] * x['DFJE'], axis=1)
        result['LSBH'] = result.apply(lambda x: int(x['凭证编号'].replace(' 1-','')), axis=1)

        # 第一次清洗，根据借方科目代码和交易对手方，在比对表中查找对应的二级明细科目和科目名称
        def funshion_02(jfkm,opposite):
            result = self.name_comparative_file
            result = result.loc[result['一级科目'] == jfkm]
            result = result.loc[result['Full_Name'] == opposite]
            result = result.reset_index(drop=True)
            if result.empty:
                result = '未找到对应值'
            else:
                result = result['JFKM'][0]
            return result
        result['二级科目'] = result.apply(lambda x: funshion_02(jfkm=x['JFKM'], opposite=x['交易对手方']), axis=1)
        def funshion_03(jfkm,opposite):
            result = self.name_comparative_file
            result = result.loc[result['一级科目'] == jfkm]
            result = result.loc[result['Full_Name'] == opposite]
            result = result.reset_index(drop=True)
            if result.empty:
                result = '未找到对应值'
            else:
                result = result['KMMC'][0]
            return result
        result['二级科目名称'] = result.apply(lambda x: funshion_03(jfkm=x['JFKM'], opposite=x['交易对手方']), axis=1)

        # 第二次清洗，从用户信息表中获取给定的科目取值，根据给定的科目替换取值
        # 举例 银行科目在用户信息中已给出，将该科目名称写入二级科目中
        result['二级科目'] = result.apply(lambda x: self.bank_account if x['JFKM']=='1002' else x['二级科目'], axis=1)


        # 第三次清洗，需要使用者参与，先人工查找对应关系并补充进名称对应表中，随后再次执行清洗
        # 清洗时会自动跳过交易对手方为空的数据和程序未找到对应操作的数据
        def funshion_05(jfkm,opposite):
            result = self.find_name(name=opposite, JFKM=jfkm)
            if result.empty:
                logger.info(jfkm+opposite+'未找到对应关系')
                result = '未找到对应值'
            else:
                # 更新名称对照表
                self.get_name_comparative_file()
                # 再次执行funshion_02，根据JFKM和对手方查找二级科目
                result = funshion_02(jfkm,opposite)
            return result

        # 生成此时所有未匹配操作对应的摘要列表
        NO_Matched_Action_ZY_list = self.action
        NO_Matched_Action_ZY_list = NO_Matched_Action_ZY_list.loc[NO_Matched_Action_ZY_list['操作'].str.contains('NO_Matched_Action')]
        NO_Matched_Action_ZY_list = NO_Matched_Action_ZY_list['ZY']
        NO_Matched_Action_ZY_list = NO_Matched_Action_ZY_list.tolist()

        # 执行清洗时需要去除交易对手方为空的数据和程序未找到对应操作的数据（根据摘要判断）
        result['二级科目'] = result.apply(lambda x: funshion_05(jfkm=x['JFKM'], opposite=x['交易对手方']) if x['交易对手方'] != None and x['二级科目'] == '未找到对应值' and x['ZY'] not in NO_Matched_Action_ZY_list else x['二级科目'], axis=1)

        # 根据输入的二级科目在科目余额表中查找科目名称并返回
        def funshion_04(EJKM):
            result = self.balance_file
            result = result.loc[result['JFKM'] == EJKM]
            result = result.reset_index(drop=True)
            if result.empty:
                result = '未找到对应值'
            else:
                result = result['KMMC'][0]
            return result
        result['二级科目名称'] = result.apply(lambda x: funshion_04(EJKM=x['二级科目']), axis=1)

        # 如果凭证采用默认方式编号，即一条交易一张凭证，则不用处理'HS'行数数据。
        # 如果凭证采用银行回单按天汇总的编号，则需要处理'HS'行数数据。
        # 处理逻辑为，同一编号内的行数重新编号
        if self.aapz_number_type == 'bank_in_day':
            list_BH = result['凭证编号'].to_list()
            list_HS = [1]
            i = 1
            while i < len(list_BH):
                if list_BH[i] == list_BH[i-1]:
                    HS = list_HS[i-1] + 1
                    list_HS.append(HS)
                else:
                    HS = 1
                    list_HS.append(HS)
                i = i + 1
            result = result.drop(['HS'], axis=1)
            result['HS'] = list_HS

        # 处理摘要数据
        result['ZY'] = result.apply(lambda x: x['ZY'].format(TradeTime=x['交易时间']),axis=1)

        # 数据清洗完毕，进行格式清洗后插入记账凭证表aapz
        # 先删除已有的aapz记账凭证数据，再进行插入
        result = result[['User_full_name','Time', 'RQ', 'HS', '凭证编号', 'LSBH',  'ZY', 'JFKM', 'JFMC', 'JFJE', 'DFJE', '是否关联二级科目', '二级科目', '二级科目名称']]
        result = result.rename(columns={'凭证编号': 'BH'})
        sql = "DELETE FROM auto_account.aapz where User_full_name = '{User}' and Time = {date}"
        sql = sql.format(User=self.user,date=self.date)
        self.data.delete_sql(sql)
        self.data.insert_sql(df=result, tablename='aapz')
        # 提取记账凭证表信息
        self.get_aapz()

    # 该函数用于生成输出的记账凭证数据，返回结果为一个df并输出一个EXCEL
    def aapz_out_put(self,FolderName_dic):
        out_put = self.aapz
        out_put = out_put[['RQ', 'HS', 'BH', 'LSBH', 'FJ',  'ZY', 'JFMXH', 'JFKM', 'JFMC', 'JFJE', 'JFSL', 'DFJE', 'DFSL', '是否关联二级科目', '二级科目', '二级科目名称']]
        # 如果关联二级科目，则JFKM字段取二级科目代码，否则不变
        out_put['JFKM'] = out_put.apply(lambda x: x['二级科目'] if x['是否关联二级科目'] == 'Y' else x['JFKM'], axis=1)
        # 如果关联二级科目，则JFMC字段须处理，否则不变
        # 处理方法为：连接二级科目名称和一级科目名称，中间补空格，且二级科目加空格总长不超过24个字节
        # 示例：
        # 广东金海纳实业有限公司  应付账款
        # 汉字算2个字节，空格算1个字节，‘广东金海纳实业有限公司  ’总长为24个字节
        def funshion_06(KMMC,ejkm):
            ejkm_len = len(ejkm) * 2
            space_len = 24 - ejkm_len
            total_len = space_len + len(ejkm)
            result = ejkm.ljust(total_len, ' ')
            result = result + KMMC
            return result
        out_put['JFMC'] = out_put.apply(lambda x: funshion_06(KMMC=x['JFMC'],ejkm=x['二级科目名称']) if x['是否关联二级科目'] == 'Y' else x['JFMC'], axis=1)
        df_BH_YS_COUNT = out_put.groupby(by = 'BH')
        df_BH_YS_COUNT = df_BH_YS_COUNT['HS'].count()
        out_put['YS'] = out_put.apply(lambda x: math.ceil(int(df_BH_YS_COUNT[x['BH']])/5), axis=1)

        # 如果总页数大于1页，需要处理BH字段
        # 处理结果类似‘ 1-0009( 4/16)’
        # 计算当前页数
        out_put['当前编号'] = out_put.apply(
            lambda x: ' '+ str(math.ceil(int(x['HS']) / 5)) if int(math.ceil(int(x['HS']) / 5))<10 else str(math.ceil(int(x['HS']) / 5)), axis=1)
        out_put['当前编号'] = out_put.apply(lambda x: str(x['BH'])+'('+ x['当前编号'] +'/'+str(x['YS'])+')' if int(x['YS'])>1 else x['BH'],axis=1)
        list_HS_NEW = out_put['当前编号'].to_list()
        out_put = out_put.drop(['BH'], axis=1)
        out_put['BH'] = list_HS_NEW



        # 筛选出输入小精灵需要的字段
        out_put = out_put[['RQ', 'HS', 'BH', 'LSBH', 'FJ', 'ZY', 'JFMXH', 'JFKM', 'JFMC', 'JFJE', 'JFSL', 'DFJE', 'DFSL']]
        # 生成输出路径，输出位置时本次分析创建的文件夹下，凭证输出子文件夹
        out_put_path = FolderName_dic['output_path'] + '/'+str(self.date)+'aapz.xls'
        # 生成输出数据
        out_put.to_excel(out_put_path,index=False)
        logger.info('输出记账凭证文件：%s', out_put_path)
        return out_put

        # 根据匹配的操作，生成相应的记账凭证编号
    def aapz_number(self,df):
        Current_number = input('请输入当前已有凭证的最新编号（生成凭证从下一号开始）：')
        logger.info('当前已有凭证的最新编号（生成凭证从下一号开始）：%s' %Current_number)
        Current_number = int(Current_number) + 1
        if self.aapz_number_type == 'default':
            df['凭证编号'] = df.index.tolist()
            # 记账凭证编号处理成 ‘ 1-XXXX’的格式
            df['凭证编号'] = df.apply(lambda x : ' 1-'+str(int(x['凭证编号']) + Current_number).zfill(4), axis=1)
        if self.aapz_number_type == 'bank_in_day':
            # 获取前一条凭证的类型和交易日期
            list_aapz_type = df['原始凭证类型'].to_list()
            list_aapz_type.insert(0,'0')
            del list_aapz_type[-1]
            df['前一条凭证类型'] = list_aapz_type
            list_aapz_date = df['交易时间'].to_list()
            list_aapz_date.insert(0,'19000101')
            del list_aapz_date[-1]
            df['前一条日期'] = list_aapz_date
            # 定义凭证编号生成方法01
            # 如果该交易原始凭证不是银行回单，编号+1；
            # 如果是银行回单，但前一条凭证不是银行回单，编号+1；
            # 如果是银行回单，前一条凭证是银行回单，当前日期与前一条日期不是同一天，编号+1；
            # 如果是银行回单，前一条凭证是银行回单，当前日期与前一条日期是同一天，编号不变
            def aapz_number_funshion01(type,type_before,date,date_before):
                if type != '1':
                    result = 1
                else:
                    if type_before != '1':
                        result = 1
                    else:
                        if date != date_before:
                            result = 1
                        else:
                            result = 0
                return result
            df['凭证编号增量'] = df.apply(lambda x:aapz_number_funshion01(type=x['原始凭证类型'], type_before=x['前一条凭证类型'], date=x['交易时间'], date_before=x['前一条日期']),axis=1)
            list_aapz_increase = df['凭证编号增量'].to_list()
            list_aapz_number = [0]
            i = 1
            while i < len(list_aapz_increase):
                aapz_number = list_aapz_increase[i] + list_aapz_number[i-1]
                list_aapz_number.append(aapz_number)
                i = i + 1
            df['凭证编号'] = list_aapz_number
            df = df.drop(['凭证编号增量'], axis=1)
            df = df.drop(['前一条日期'], axis=1)
            df = df.drop(['前一条凭证类型'], axis=1)

            # 记账凭证编号处理成 ‘ 1-XXXX’的格式
            df['凭证编号'] = df.apply(lambda x : ' 1-'+str(int(x['凭证编号']) + Current_number).zfill(4), axis=1)




        return df


    def analyzer(self):
        logger.info('当前环境：%s\n当前日期：%s\n当前用户：%s\n' %(self.filename,self.date,self.user))



