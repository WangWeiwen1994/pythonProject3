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


class Base_Action():
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
        # 定义手续费匹配的关键词列表
        self.Service_Charge_keyword_list = ['手续费','短信费','回单费']
        # 定义提现匹配的关键词列表
        self.Withdraw_Charge_keyword_list = ['报销', '提现', '备用金', '取现']
        # 定义结息匹配的关键词列表
        self.Interest_keyword_list = ['结息', '利息']
        # 定义货款匹配的关键词列表
        self.Payment_on_goods_keyword_list = ['货款']