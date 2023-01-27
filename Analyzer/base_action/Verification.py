from .base import Base_Action
from logger import logger

class Verification(Base_Action):
    def __init__(self,Data):
        super().__init__(Data)

    # 输入用户校验
    def UserCheck(self):
        logger.info('【用户校验：开始】：')
        df = self.data.load_sql("select * from auto_account.user_information where User_full_name = '%s'" %self.user)

        if len(df['User_full_name']) > 1:
            logger.info('该用户名存在多条结果，请检查表auto_account.user_information')
            exit()
        else:
            if df.empty == True:
                logger.info('【用户校验结果：该用户不存在】')
                exit()
            else:
                self.bank_account = df['Bank_account'][0]
                logger.info('【用户校验结果：校验通过】')