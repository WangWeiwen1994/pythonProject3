# 执行该分析类下包含的所有匹配方法，用于对所有结果为未匹配的数据进行匹配
# 分析类包含如下几个方法去进行匹配操作：
# 1 工资、缴税等高频交易，但匹配元素缺失的（缴税的银行流水，无对手方）：
# 使用固定逻辑（如通过银行流水中是否包含关键词）进行匹配
# 2 根据科目余额表中的已有科目（应收账款，应付账款）进行判断的：
# 该类方法需要先查找对应关系，同时若可提炼出符合现有路径的逻辑的，需要补充到逻辑表中


from .base import Base_Action
from logger import logger

# 根据输入的条件，在分析逻辑表中筛选出相应的操作代码
    # 该函数逻辑为，根据每一笔交易中的元素，查找对应的逻辑
    # 示例：
    # 当前交易为银行流水，用户收到一笔来在XXXX公司的款项，随后查找有无逻辑可同时满足上述的用户，原始凭证类型，交易对手方，交易方向。
    # 如能够匹配，则返回逻辑记录的操作ID
    # 如不能匹配，则返回NO_Matched_Action
def action_match(analyze_file,User_full_name,type, opposite,side,symbool):
    result = analyze_file
    result = result.loc[(result['User_full_name'] == User_full_name) & (result['原始凭证类型'] == type) & (result['原始凭证类型'] == type) & (result['交易对手方'] == opposite) & (result['交易方向'] == side) & (result['交易标的'] == symbool)]
    result = result.reset_index(drop=True)
    if len(result['操作']) == 1:
        return result['操作'][0]
    else:
        result = 'NO_Matched_Action_原始凭证类型=%s_交易方向=%s_交易标的=%s' %(type, side, symbool)
        return result

class Match_action_analyze(Base_Action):
    def __init__(self,Data):
        super().__init__(Data)

    # 遍历每一笔交易,根据逻辑表判断对应的操作,在操作表中打上操作代码
    def action_match_v0001(self):
        # 遍历每一笔交易,根据逻辑表判断对应的操作
        logger.info('【匹配操作v0001：开始】')
        df = self.execution
        # 该函数逻辑为，根据每一笔交易中的元素，查找对应的逻辑
        # 示例：
        # 当前交易为银行流水，用户收到一笔来在XXXX公司的款项，随后查找有无逻辑可同时满足上述的用户，原始凭证类型，交易对手方，交易方向。
        # 如能够匹配，则返回逻辑记录的操作ID
        # 如不能匹配，则返回NO_Matched_Action
        df['操作'] = df.apply(lambda x : action_match(analyze_file=self.analyze,User_full_name=x['User_full_name'],type=x['原始凭证类型'],opposite=x['交易对手方'],side=x['交易方向'],symbool=x['交易标的']), axis=1)
        logger.info('【匹配操作v0001：结束】')
        # 更新属性值
        self.execution = df

    # 该方法根据银行回单对手方账户和备注信息是否包含关键字’工资‘来判断是否匹配工资操作
    def salary_match_v0001(self,Salary_Bankaccount):
        # 遍历每一笔交易，如果原始凭证为银行回单,对手方是工资发放账户，交易方向为付款，且备注中包含关键词'工资'，则匹配对应的操作
        logger.info('【工资匹配操作v0001：开始】')
        df = self.execution
        # 筛选原始凭证为银行回单,对手方是工资发放账户，交易方向为付款，且备注中包含关键词'工资'的数据,赋操作值ACTION_000000007
        df.loc[(df['User_full_name']==self.user) & (df['原始凭证类型'] == '1') & (df['交易方向'] == '付款') & (df['备注'].str.contains('工资')) & (df.交易对手方.isin(Salary_Bankaccount)),('操作')] = 'ACTION_000000007'
        logger.info('【工资匹配操作v0001：结束】')
        # 更新属性值
        self.execution = df

    # 该方法对凭证为银行回单，交易方向为付款，对手方为空，包含关键词‘手续费’、‘短信费’等交易，判断为支付手续费，并赋操作值ACTION_000000006
    # keyword_list为一个列表，列表元素为包含的关键词，增加关键词在base的对应属性self.keyword_list中增加。
    def Service_Charge_match_v0001(self):
        logger.info('【手续费匹配操作v0001：开始】')
        df = self.execution
        keyword_list = self.keyword_list
        for keyword in keyword_list:
            df.loc[(df['User_full_name'] == self.user) & (df['原始凭证类型'] == '1') & (df['交易方向'] == '付款') & (
                df['备注'].str.contains(keyword)) & (df['交易对手方'].isnull()), ('操作')] = 'ACTION_000000006'

        logger.info('【手续费匹配操作v0001：结束】')
        # 更新属性值
        self.execution = df