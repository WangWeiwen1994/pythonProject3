from .base import Base_Action
from logger import logger

class Verification(Base_Action):
    def __init__(self,Data):
        super().__init__(Data)

    # 输入用户校验,返回结果为提取到的用户信息
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
        return df

    # 统计交易中，未匹配到操作结果的数量和占比
    def count_NO_Matched_Action(self):
        count_execution = len(self.execution)
        df = self.execution
        no_matched_action = df.loc[df['操作'].str.contains('NO_Matched_Action')]
        count_no_matched_action = len(no_matched_action)
        percent_result = round(count_no_matched_action/count_execution*100,2)
        percent_result = str(percent_result)+'%'
        logger.info('current time:%s\ncurrent user:%s\n' % (self.date, self.user))
        logger.info('本次分析统计到的所有交易笔数:%s' % (count_execution))
        logger.info('本次分析统计到的未匹配交易笔数:%s' % (count_no_matched_action))
        logger.info('本次分析未匹配交易笔数占比:%s' % (percent_result))
