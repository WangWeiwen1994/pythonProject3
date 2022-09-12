# 建表语句，;号分隔
# 目录
# 1 用户信息表


CreateTableSqls = '''
CREATE TABLE auto_account.User_information (
	ID INT auto_increment NOT NULL COMMENT 'ID',
	User_full_name varchar(100) NOT NULL COMMENT '公司全称',
	`TaxPayer ID` varchar(100) NOT NULL COMMENT '纳税人识别号',
	CONSTRAINT User_information_un UNIQUE KEY (User_full_name,`TaxPayer ID`),
	CONSTRAINT User_information_pk PRIMARY KEY (ID)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci
COMMENT='用户信息表';
'''