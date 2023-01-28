from .base import Base_Action
from.Interaction import Interaction
from.Verification import Verification
from.MySQL_action import MySQL_action_bank_receipt_abc
from.MySQL_action import MySQL_action_execution
from.MySQL_action import MySQL_action_balance
from.MySQL_action import MySQL_action_name_comparative_table
from.MySQL_action import MySQL_action_analyze
from.MySQL_action import MySQL_action_action
from.MySQL_action import MySQL_action_aapz
from.Produce_aapz_action import Produce_aapz_action

class Base_Action_dic(Base_Action):
    def __init__(self,Data):
        super().__init__(Data)
        self.Base_Action = Base_Action(Data)
        self.Interaction = Interaction(Data)
        self.Verification = Verification(Data)
        self.MySQL_action_bank_receipt_abc = MySQL_action_bank_receipt_abc(Data)
        self.MySQL_action_execution = MySQL_action_execution(Data)
        self.MySQL_action_balance = MySQL_action_balance(Data)
        self.MySQL_action_name_comparative_table = MySQL_action_name_comparative_table(Data)
        self.MySQL_action_analyze = MySQL_action_analyze(Data)
        self.MySQL_action_action = MySQL_action_action(Data)
        self.MySQL_action_aapz = MySQL_action_aapz(Data)
        self.Produce_aapz_action = Produce_aapz_action(Data)



