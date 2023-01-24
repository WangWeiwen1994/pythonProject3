# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
from logger import logger
import argparse
from configs import config
import pandas as pd
from data import Data
from Analyzer import Analyzers




def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.




# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    logger.info("--------------------------------------------------------------------------------------------------")
    logger.info("Start print log")
    parser = argparse.ArgumentParser()

    # 配置传入的参数
    # -a 选择要调用的分析类
    parser.add_argument("-a", "--Analyzer", help="choose the order", required=True)
    # -e 选择要调用的配置文件
    parser.add_argument("-e", "--Environment", help="choose the Environment", required=True)
    # -t 选择要计算的记账日期
    parser.add_argument("-t", "--Time", help="choose the Time", required=True)
    # -u 选择要计算的用户
    parser.add_argument("-u", "--User", help="choose the Time", required=True)
    # -at 选择要计算的记账模式
    parser.add_argument("-at", "--aapz_number_type", help="choose the type 'default' or 'bank_in_day'", required=True, default='default')
    args = vars(parser.parse_args())

    # 处理传入的分析类参数
    args['Analyzer'] = args['Analyzer'].split(',')
    # 获取当前可供执行的分析类,检查用户传入的分析类是否有效
    Analyzers_list = []
    for analyzer_execution in Analyzers:
        Analyzers_list.append(analyzer_execution.__name__)
    for analyzer in args['Analyzer']:
        if analyzer not in Analyzers_list:
            # 如为无效的分析类名称，则删除
            logger.info('undefined_Analyzer:%s' % analyzer)
            args['Analyzer'].remove(analyzer)

    # 向data类中传入除analyzer外的全部配置参数
    data = Data(filename=args['Environment'], date=args['Time'], user=args['User'], aapz_number_type=args['aapz_number_type'] )

    # 逐个执行传入的analyer分析类
    for analyzer in args['Analyzer']:
        for analyzer_execution in Analyzers:
            if analyzer_execution.__name__ == analyzer:
                logger.info('----------------------------------Start to do %s------------------------------' % analyzer)
                analyzer_execution(data).analyzer()
                logger.info('---------------------------------%s Finished----------------------------------' % analyzer)


