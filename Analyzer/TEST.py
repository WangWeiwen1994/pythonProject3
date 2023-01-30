# 该方法用于开发调试，不做生产使用
# 该方法用于开发调试，不做生产使用
# 该方法用于开发调试，不做生产使用
from .base_action import Base_Action_dic
from logger import logger

class TEST(Base_Action_dic):
    def __init__(self,Data):
        super().__init__(Data)
    def analyzer(self):
        df = self.MySQL_action_execution.get_All_Data()
        self.Match_action_analyze.execution = df
        self.Match_action_analyze.make_aapz_nume_day_action_opposite_tpye()