from .base_action import Base_Action_dic
from logger import logger

class FuShifeng_New_analyzer(Base_Action_dic):
    def __init__(self,Data):
        super().__init__(Data)
    def analyzer(self):
        # 输出当前环境配置信息
        logger.info('\ncurrent environment:%s\ncurrent time:%s\ncurrent user:%s\n' %(self.filename,self.date,self.user))

        # 进行用户校验
        user_info = self.Verification.UserCheck()

        # 创建任务文件夹，用户在文件夹中存入待计算数据后，获取对应文件的路径字典
        FolderName_dic = self.Interaction.mission_file_preparation()

        # 尝试提取该公司该账期下全部的银行回单数据
        bank_file = self.MySQL_action_bank_receipt_abc.get_All_Data()

        # 如上一步未提取到银行回单数据，则从任务文件夹中读取银行回单数据
        if bank_file.empty:
            logger.info('数据库中未查询到银行回单信息，读取用户输入的文件')
            self.MySQL_action_bank_receipt_abc.insert_InputData(FolderName_dic)
        else:
            logger.info('数据库中已查询到银行回单信息%s条，直接使用' % (len(bank_file)))

        # 删除execution表中的原有数据(同公司同账期)
        self.MySQL_action_execution.delete_All_Data()

        # 查询银行回单数据，将其抽取转置后插入到execution表中
        self.MySQL_action_execution.ETL_bank_receipt_abc_To_execution()

        # 读取任务文件中的科目余额表信息，若从任务文件夹中读取到科目余额表，则覆盖写入balance表中
        balance_file = self.MySQL_action_balance.insert_InputData(FolderName_dic)

        # 若任务文件夹中未读取到科目余额表信息，则直接调用数据库中的科目表数据
        if balance_file.empty:
            balance_file = self.MySQL_action_balance.get_All_Data()

        # 提取数据库中已有的名称对照表全部数据(同公司)
        self.Produce_aapz_action.name_comparative_file = self.MySQL_action_name_comparative_table.get_All_Data()

        # 提取数据库中的交易表全部数据(同公司)
        execution_file = self.MySQL_action_execution.get_All_Data()

        # 提取数据库中的分析逻辑表全部数据(同公司)
        analyze_file = self.MySQL_action_analyze.get_All_Data()

        # 提取数据库中的操作信息表全部数据
        action_file = self.MySQL_action_action.get_All_Data()



        # 该部分流程须修改。做成一个新的方法类，返回结果插入execution表中

        # 调用匹配逻辑v0001,为execution表中的操作列赋值,返回处理后的execution
        execution_file = self.MySQL_action_execution.action_match_v0001(execution=execution_file,analyze_file=analyze_file)

        # 调用凭证编号处理方法,为execution表中的凭证编号列赋值，插入execution表中，返回处理后的execution
        execution_file = self.MySQL_action_execution.aapz_number(execution_file)

        # 生成记账凭证
        self.Produce_aapz_action.balance_file = balance_file
        self.Produce_aapz_action.execution = execution_file
        self.Produce_aapz_action.analyze = analyze_file
        self.Produce_aapz_action.action = action_file
        self.Produce_aapz_action.execution = execution_file
        self.Produce_aapz_action.bank_account = user_info['Bank_account'][0]
        aapz = self.Produce_aapz_action.aapz_manager()

        #logger.info(aapz)
        #aapz.to_sql('auto_account.aapz', con=self.engine, if_exists='append', index=False, dtype=None)

        # 记账凭证插入aapz表中
        self.MySQL_action_aapz.insert_after_delete(df=aapz)

        # 将aapz表中数据转换成可输入小精灵的格式，生成一个EXCEL
        self.Produce_aapz_action.aapz = self.MySQL_action_aapz.get_All_Data()
        self.Produce_aapz_action.aapz_out_put(FolderName_dic=FolderName_dic)

