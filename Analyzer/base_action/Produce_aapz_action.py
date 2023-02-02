# 该操作类用于生成记账凭证

from .base import Base_Action
from logger import logger
import pandas as pd
import math
import copy


# 对输入的字符串，每两个字符进行截取
# 示例
# name = '上海晨光科力普办公用品有限公司'
# result = ['上海', '海晨', '晨光', '光科', '科力', '力普', '普办', '办公', '公用', '用品', '品有', '有限', '限公', '公司']
def slice(name):
    a = 0
    b = 2
    l = len(name)
    result = []
    while a < l - 1:
        result.append(name[a:b])
        a = a + 1
        b = b + 1
    return result


# 计算输入的两个字符串，两个字符串去重后重合元素的数量
# 示例
# a = ['上海', '海晨', '晨光', '光科', '科力', '力普', '普办', '办公', '公用', '用品', '品有', '有限', '限公', '公司']
# b = ['晨光', '光科', '科力', '力普', '普（', '（上', '上海', '海）', '）办', '办公', '公用', '用品', '品有', '有限', '限公', '公司']
# result = 12

def inner_count(a, b):
    set_c = set(a) & set(b)
    list_c = list(set_c)
    result = len(list_c)
    return result

class Produce_aapz_action(Base_Action):
    def __init__(self,Data):
        super().__init__(Data)


    # 提取数据库中已有的名称对照表信息
    def get_name_comparative_file(self):
        get_name_comparative_file = "select * from auto_account.name_comparative_table where User_full_name = '{User}'"
        get_name_comparative_file = get_name_comparative_file.format(User=self.user)
        logger.info('提取数据库中已有的名称对照表信息')
        self.name_comparative_file = self.data.load_sql(sql = get_name_comparative_file)

    # 该方法用于对公司名称进行处理，找到科目余额表中对应的科目
    def find_name(self, name, JFKM):
        Full_Name = copy.copy(name)

        # 读取配置文件中的高频词，对输入的名称进行去除，提高准确率
        HF_words = self.data.High_frequency_words
        HF_words = HF_words.split(',')
        for word in HF_words:
            name = name.replace(word, '')

        # 将去除高频词的名称进行分解
        name = slice(name)

        # 与科目余额表中的名称进行比对，找出重合度最高的数据
        balance_file = self.balance_file
        # 对JFKM字段中，取前4位（即借方科目），进行筛选
        balance_file = balance_file[balance_file['JFKM'].map(lambda x: x[0:4]) == JFKM]
        balance_file['cal'] = balance_file['KMMC'].apply(lambda x: slice(x))
        balance_file['cal_count'] = balance_file['cal'].apply(lambda x: inner_count(name, x))
        max_count = balance_file['cal_count'].max()
        result = balance_file.loc[balance_file['cal_count'] == max_count]
        result = result[['User_full_name', 'JFKM', 'DFKM', 'KMMC']]
        result['Full_Name'] = Full_Name
        result['一级科目'] = result.apply(lambda x: x['JFKM'][0:4], axis=1)
        result = result[0:5]
        result = result.reset_index(drop=True)
        logger.info('请输入对应的行号，无对应结果请输入N\n查询结果如下:')
        logger.info(result)
        r = input()
        if r == 'N':
            result = pd.DataFrame()
            logger.info(Full_Name + '当前无对应科目')
        else:
            result = result.loc[r:r]
            logger.info(Full_Name + '找到对应关系')
            self.data.insert_sql(df=result, tablename='name_comparative_table')
        return result


    # 该函数用于生成凭证数据
    # 首先对交易表和操作表进行左连接
    # 随后对结果进行清洗，主要是计算金额和查找对应二级科目
    # 接着将凭证整理成记账凭证的格式插入到aapz表中
    def aapz_manager(self):
        logger.info('【生成记账凭证：开始】')
        # 将交易表和操作表进行左连接，连接键为‘操作’
        result = pd.merge(left=self.execution, right=self.action,how='left',left_on='操作',right_on='操作')
        # 根据操作中配置的取值字段，读取对应的每一行取值数据
        result['取值'] = result.apply(lambda x:x[x['取值']],axis=1)
        # 提取生成凭证涉及的字段，去除无关字段
        result = result[['User_full_name','Time','交易对手方','交易时间','交易方向','备注','凭证编号','HS','ZY','JFKM','JFMC','JFJE','DFJE','取值','是否关联二级科目','二级科目','二级科目名称']]
        # 生成凭证中的日期格式 20220630 变为 2022/6/30
        def funshion_01(time):
            year = time[0:4]
            if time[4] == '0':
                month = time[5]
            else:
                month = time[4:6]
            if time[6] == 0:
                day = time[7]
            else:
                day = time[6:8]
            result = str(year)+'/'+str(month)+'/'+str(day)
            return result
        result['RQ'] =result.apply(lambda x : funshion_01(x['Time']), axis=1)
        result['JFJE'] = result.apply(lambda x: x['取值'] * x['JFJE'], axis=1)
        result['DFJE'] = result.apply(lambda x: x['取值'] * x['DFJE'], axis=1)
        result['LSBH'] = result.apply(lambda x: int(x['凭证编号'].replace(' 1-','')), axis=1)

        # 第一次清洗，根据借方科目代码和交易对手方，在比对表中查找对应的二级明细科目和科目名称
        def funshion_02(jfkm,opposite,ejkm):
            if ejkm == '未找到对应值':
                result = self.name_comparative_file
                result = result.loc[result['一级科目'] == jfkm]
                result = result.loc[result['Full_Name'] == opposite]
                result = result.reset_index(drop=True)
                if result.empty:
                    result = '未找到对应值'
                else:
                    result = result['JFKM'][0]
                return result
            else:
                result = ejkm
                return result
        result['二级科目'] = result.apply(lambda x: funshion_02(jfkm=x['JFKM'], opposite=x['交易对手方'],ejkm=x['二级科目']), axis=1)
        def funshion_03(jfkm,opposite,kmmc):
            if kmmc == '未找到对应值':
                result = self.name_comparative_file
                result = result.loc[result['一级科目'] == jfkm]
                result = result.loc[result['Full_Name'] == opposite]
                result = result.reset_index(drop=True)
                if result.empty:
                    result = '未找到对应值'
                else:
                    result = result['KMMC'][0]
                return result
            else:
                return kmmc
        result['二级科目名称'] = result.apply(lambda x: funshion_03(jfkm=x['JFKM'], opposite=x['交易对手方'],kmmc=x['二级科目名称']), axis=1)

        # 第二次清洗，从用户信息表中获取给定的科目取值，根据给定的科目替换取值
        # 举例 银行科目在用户信息中已给出，将该科目名称写入二级科目中
        result['二级科目'] = result.apply(lambda x: self.bank_account if x['JFKM']=='1002' else x['二级科目'], axis=1)


        # 第三次清洗，需要使用者参与，先人工查找对应关系并补充进名称对应表中，随后再次执行清洗
        # 清洗时会自动跳过交易对手方为空的数据和程序未找到对应操作的数据
        def funshion_05(jfkm,opposite):
            result = self.find_name(name=opposite, JFKM=jfkm)
            if result.empty:
                logger.info(jfkm+opposite+'未找到对应关系')
                result = '未找到对应值'
            else:
                # 更新名称对照表
                self.get_name_comparative_file()
                # 再次执行funshion_02，根据JFKM和对手方查找二级科目
                result = funshion_02(jfkm,opposite)
            return result

        # 生成此时所有未匹配操作对应的摘要列表
        NO_Matched_Action_ZY_list = self.action
        NO_Matched_Action_ZY_list = NO_Matched_Action_ZY_list.loc[NO_Matched_Action_ZY_list['操作'].str.contains('NO_Matched_Action')]
        NO_Matched_Action_ZY_list = NO_Matched_Action_ZY_list['ZY']
        NO_Matched_Action_ZY_list = NO_Matched_Action_ZY_list.tolist()

        # 执行清洗时需要去除交易对手方为空的数据和程序未找到对应操作的数据（根据摘要判断）
        result['二级科目'] = result.apply(lambda x: funshion_05(jfkm=x['JFKM'], opposite=x['交易对手方']) if x['交易对手方'] != None and x['二级科目'] == '未找到对应值' and x['ZY'] not in NO_Matched_Action_ZY_list else x['二级科目'], axis=1)

        # 根据输入的二级科目在科目余额表中查找科目名称并返回
        def funshion_04(EJKM):
            result = self.balance_file
            result = result.loc[result['JFKM'] == EJKM]
            result = result.reset_index(drop=True)
            if result.empty:
                result = '未找到对应值'
            else:
                result = result['KMMC'][0]
            return result
        result['二级科目名称'] = result.apply(lambda x: funshion_04(EJKM=x['二级科目']), axis=1)

        # 如果凭证采用默认方式编号，即一条交易一张凭证，则不用处理'HS'行数数据。
        # 如果凭证采用银行回单按天汇总的编号，则需要处理'HS'行数数据。
        # 处理逻辑为，同一编号内的行数重新编号
        if self.aapz_number_type == 'day-action-opposite':
            list_BH = result['凭证编号'].to_list()
            list_HS = [1]
            i = 1
            while i < len(list_BH):
                if list_BH[i] == list_BH[i-1]:
                    HS = list_HS[i-1] + 1
                    list_HS.append(HS)
                else:
                    HS = 1
                    list_HS.append(HS)
                i = i + 1
            result = result.drop(['HS'], axis=1)
            result['HS'] = list_HS

        # 处理摘要数据
        result['ZY'] = result.apply(lambda x: x['ZY'].format(TradeTime=x['交易时间'],交易对手方=x['交易对手方'],交易方向=x['交易方向']),axis=1)

        # 数据清洗完毕，进行格式清洗后插入记账凭证表aapz
        # 先删除已有的aapz记账凭证数据，再进行插入
        result = result[['User_full_name','Time', 'RQ', 'HS', '凭证编号', 'LSBH',  'ZY', 'JFKM', 'JFMC', 'JFJE', 'DFJE', '是否关联二级科目', '二级科目', '二级科目名称']]
        result = result.rename(columns={'凭证编号': 'BH'})
        logger.info('【生成记账凭证：完成】')
        return result

    # 开发中
    # 重新编写的记账凭证生成方法
    # 该函数用于生成凭证数据
    # 首先对交易表和操作表进行左连接
    # 随后对结果进行清洗，主要是计算金额和查找对应二级科目
    # 接着将凭证整理成记账凭证的格式插入到aapz表中

    # 该函数用于生成输出的记账凭证数据，返回结果为一个df并输出一个EXCEL
    def aapz_out_put(self, FolderName_dic):
        logger.info('【生成输出文件：开始】')
        out_put = self.aapz
        logger.info(out_put.columns)
        out_put = out_put[['RQ', 'HS', 'BH', 'LSBH', 'FJ',  'ZY', 'JFMXH', 'JFKM', 'JFMC', 'JFJE', 'JFSL', 'DFJE', 'DFSL', '是否关联二级科目', '二级科目', '二级科目名称']]
        # 如果关联二级科目，则JFKM字段取二级科目代码，否则不变
        out_put['JFKM'] = out_put.apply(lambda x: x['二级科目'] if x['是否关联二级科目'] == 'Y' else x['JFKM'], axis=1)
        # 如果关联二级科目，则JFMC字段须处理，否则不变
        # 处理方法为：连接二级科目名称和一级科目名称，中间补空格，且二级科目加空格总长不超过24个字节
        # 示例：
        # 广东金海纳实业有限公司  应付账款
        # 汉字算2个字节，空格算1个字节，‘广东金海纳实业有限公司  ’总长为24个字节
        def funshion_06(KMMC,ejkm):
            ejkm_len = len(ejkm) * 2
            space_len = 24 - ejkm_len
            total_len = space_len + len(ejkm)
            result = ejkm.ljust(total_len, ' ')
            result = result + KMMC
            return result
        out_put['JFMC'] = out_put.apply(lambda x: funshion_06(KMMC=x['JFMC'],ejkm=x['二级科目名称']) if x['是否关联二级科目'] == 'Y' else x['JFMC'], axis=1)
        df_BH_YS_COUNT = out_put.groupby(by = 'BH')
        df_BH_YS_COUNT = df_BH_YS_COUNT['HS'].count()
        out_put['YS'] = out_put.apply(lambda x: math.ceil(int(df_BH_YS_COUNT[x['BH']])/5), axis=1)

        # 如果总页数大于1页，需要处理BH字段
        # 处理结果类似‘ 1-0009( 4/16)’
        # 计算当前页数
        out_put['当前编号'] = out_put.apply(
            lambda x: ' '+ str(math.ceil(int(x['HS']) / 5)) if int(math.ceil(int(x['HS']) / 5))<10 else str(math.ceil(int(x['HS']) / 5)), axis=1)
        out_put['当前编号'] = out_put.apply(lambda x: str(x['BH'])+'('+ x['当前编号'] +'/'+str(x['YS'])+')' if int(x['YS'])>1 else x['BH'],axis=1)
        list_HS_NEW = out_put['当前编号'].to_list()
        out_put = out_put.drop(['BH'], axis=1)
        out_put['BH'] = list_HS_NEW



        # 筛选出输入小精灵需要的字段
        out_put = out_put[['RQ', 'HS', 'BH', 'LSBH', 'FJ', 'ZY', 'JFMXH', 'JFKM', 'JFMC', 'JFJE', 'JFSL', 'DFJE', 'DFSL']]
        # 生成输出路径，输出位置时本次分析创建的文件夹下，凭证输出子文件夹
        out_put_path = FolderName_dic['output_path'] + '/'+str(self.date)+'aapz.xls'
        # 生成输出数据
        out_put.to_excel(out_put_path,index=False)
        logger.info('输出记账凭证文件：%s', out_put_path)
        logger.info('【生成输出文件：完成】')
        return out_put
