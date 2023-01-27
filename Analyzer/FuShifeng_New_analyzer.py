from .base_action import Base_Action_dic
from logger import logger

class FuShifeng_New_analyzer(Base_Action_dic):
    def __init__(self,Data):
        super().__init__(Data)
    def analyzer(self):
        # 输出当前环境配置信息
        logger.info('\ncurrent environment:%s\ncurrent time:%s\ncurrent user:%s\n' %(self.filename,self.date,self.user))

        # 进行用户校验
        self.Verification.UserCheck()

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

        # 删除execution表中的数据(同公司同账期)
        self.MySQL_action_execution.delete_All_Data()

        logger.info('将银行回单数据，抽取转置后插入execution表中')
        self.MySQL_action_execution.ETL_bank_receipt_abc_To_execution()
