from .base import Base_Action
from logger import logger
import pandas as pd
from dbfread import DBF
import time
import os
import copy
import math


# 创建任务文件夹
def create_file(FolderName):
    # 获取取当前的时间、月份...
    time_1 = time.localtime()
    # 按照strtime的方法来格式化时间
    time_now = time.strftime("%Y-%m-%d-%H_%M_%S", time_1)
    FolderName = FolderName + time_now
    logger.info('已创建任务文件夹，路径为：\n%s', FolderName)
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
    return FolderName_dic


class Interaction(Base_Action):
    def __init__(self,Data):
        super().__init__(Data)

    # 生成用于本次分析的输入和输出文件夹，返回结果为输入文件夹路径的词典
    # 例如：{'input_path_Bank': 'E:\\测试公司_20220731_2022-07-08-20_32_18/银行回单输入', 'output_path': 'E:\\测试公司_20220731_2022-07-08-20_32_18/凭证输出'}
    def mission_file_preparation(self):
        logger.info('【开始创建任务文件夹】：')
        FolderName = self.data.input_path + self.user + '_所属账期_' + self.date + '_'
        FolderName_dic = create_file(FolderName=FolderName)
        self.FolderName_dic = FolderName_dic
        logger.info('【任务文件夹创建完毕】')
        x = None
        while x != 'C':
            x = input('导入文件完毕后按C键继续\n')
        return FolderName_dic

