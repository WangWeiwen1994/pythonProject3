import pandas as pd

from .base import Analyzer
from logger import logger

# 重要！新增新公司分析类且银行回单来源新增时必看！
# 重要！新增新公司分析类且银行回单来源新增时必看！
# 重要！新增新公司分析类且银行回单来源新增时必看！
# 重要！新增新公司分析类且银行回单来源新增时必看！

# 使用的银行回单为农业银行（ABC）的回单
# 若使用其他银行来源的回单数据，需进行如下修改
# 1、新建一张 bank_receipt_XXX 表，XXX为银行的英文缩写
# 2、修改preparation（）中从数据库提取银行回单的SQL语句get_bank_file_sql，修改其中的FROM 表格
# 3、在base.py中新增用于处理新增银行来源回单的方法get_bank_file_XXX，XXX为银行的英文缩写
# 4、替换preparation（）中，原本的获取原始数据的方法
# 5、新增ETF抽取到execution的方法，命名类似处理银行回单方法ETL_bank_receipt_abc_To_execution()
# 6、替换preparation（）中，原本的ETL的方法

class fu_shi_feng_Analyzer(Analyzer):
    def __init__(self,Data):
        super().__init__(Data)


    def preparation(self):
        # 用户检查
        self.UserCheck()
        FolderName = self.data.input_path + self.user + '_所属账期_' + self.date+'_'

        # 生成用于本次分析的输入和输出文件夹，返回结果为输入文件夹路径的词典
        # 例如：{'input_path_Bank': 'E:\\测试公司_20220731_2022-07-08-20_32_18/银行回单输入', 'output_path': 'E:\\测试公司_20220731_2022-07-08-20_32_18/凭证输出'}
        FolderName_dic = self.create_file(FolderName=FolderName)
        logger.info('已创建任务文件夹，路径为：%s',FolderName)
        x = None
        while x != 'C':
            x = input('导入文件完毕后按C键继续')

        # 提取数据库中已有的农业银行回单信息
        get_bank_file_sql = "select * from bank_receipt_abc where User_full_name = '{User}' and Time = {date}"
        get_bank_file_sql = get_bank_file_sql.format(User=self.user,date=self.date)
        logger.info('提取数据库中已有的银行回单信息')
        self.bank_file = self.data.load_sql(sql = get_bank_file_sql)

        # 如未提取到，则读取输入的信息
        if self.bank_file.empty:
            logger.info('数据库中未查询到银行回单信息，读取输入的文件')
            self.get_bank_file_ABC(FolderName_dic)
            self.bank_file = self.data.load_sql(sql=get_bank_file_sql)
            # 将读取的文件输入信息插入数据库后，将新入库的数据抽取到execution中
            self.ETL_bank_receipt_abc_To_execution()



        # 提取输入的科目表信息
        self.get_balance_file(FolderName_dic)
        # 科目表信息如为空，则读取数据库中的信息
        if self.balance_file.empty:
            logger.info('从数据库中读取科目余额表')
            get_balance_file_sql = "select * from balance where User_full_name = '{User}'"
            get_balance_file_sql = get_balance_file_sql.format(User=self.user, date=self.date)
            logger.info('提取数据库中已有的科目表信息')
            self.balance_file = self.data.load_sql(sql=get_balance_file_sql)

        # 提取数据库中已有的名称对照表信息
        self.get_name_comparative_file()

        # 提取数据库中的交易信息
        self.get_execution()

        # 提取数据库中的分析逻辑表
        self.get_analyze()

        # 提取数据库中的操作信息表
        self.get_action()


        logger.info('数据已加载，准备完毕')
        # 根据输入的原始凭证信息（银行回单，后续还会有发票等）生成交易表
        # 银行回单生成交易信息
        # 该操作由MySQL触发器进行处理，触发条件为银行回单表发生插入后

    # 定义匹配操作的函数
    # 该函数逻辑为，根据每一笔交易中的元素，查找对应的逻辑
    # 示例：
    # 当前交易为银行流水，用户收到一笔来在XXXX公司的款项，随后查找有无逻辑可同时满足上述的用户，原始凭证类型，交易对手方，交易方向。
    # 如能够匹配，则返回逻辑记录的操作ID
    # 如不能匹配，则返回NO_Matched_Action
    def action_match(self,User_full_name,type,opposite,side,symbool):
        result = self.analyze
        result = result.loc[(result['User_full_name'] == User_full_name) & (result['原始凭证类型'] == type) & (result['原始凭证类型'] == type) & (result['交易对手方'] == opposite) & (result['交易方向'] == side) & (result['交易标的'] == symbool)]
        result = result.reset_index(drop=True)
        if len(result['操作']) == 1:
            return result['操作'][0]
        else:
            result = 'NO_Matched_Action_原始凭证类型=%s_交易方向=%s_交易标的=%s' %(type, side, symbool)
            return result





    def analyzer(self):
        logger.info('\ncurrent environment:%s\ncurrent time:%s\ncurrent user:%s\n' %(self.filename,self.date,self.user))
        self.preparation()


        # 遍历每一笔交易,根据逻辑表判断对应的操作
        logger.info('开始匹配操作')
        df = self.execution
        df['操作'] = df.apply(lambda x : self.action_match(User_full_name=x['User_full_name'],type=x['原始凭证类型'],opposite=x['交易对手方'],side=x['交易方向'],symbool=x['交易标的']),axis=1)
        logger.info('操作匹配完毕')

        # 根据匹配的操作，生成相应的记账凭证编号
        #Current_number = input('请输入当前已有凭证的最新编号（生成凭证从下一号开始）：')
        #logger.info('当前已有凭证的最新编号（生成凭证从下一号开始）：%s' %Current_number)
        #Current_number = int(Current_number) + 1
        #df['凭证编号'] = df.index.tolist()
        # 记账凭证编号处理成 ‘ 1-XXXX’的格式
        #df['凭证编号'] = df.apply(lambda x : ' 1-'+str(int(x['凭证编号']) + Current_number).zfill(4), axis=1)
        df = self.aapz_number(df=df)
        self.execution = df

        # 生成记账凭证数据
        self.aapz_manager()

        # 将aapz表中数据转换成可输入小精灵的格式，并放入
        out_put = self.aapz_out_put(FolderName_dic=self.FolderName_dic)








        # 删除已有的交易表
        sql = "DELETE FROM auto_account.execution WHERE User_full_name = '{User}' and Time = {date}"
        sql = sql.format(User=self.user,date=self.date)
        self.data.delete_sql(sql)
        # 插入更新后的数据
        self.data.insert_sql(df,'execution')







        logger.info('分析结束')



