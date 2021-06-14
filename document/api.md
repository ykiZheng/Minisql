# API接口要求
- 需要interpreter去除头和尾的\n, ';'和空格
- 重要：在使用其他功能前必须先调用依次 use_db() 找到一个数据库才能进行其他操作
- PS：如果
# API接口说明
- API 会检查语法错误（但是不会检查有没有分号结尾，这是intepreter要干的），如果出现错误会抛出 MinisqlError 或者 MinisqlSyntexError供 Interpreter 判断，一般错误是不会停止程序，只需要在界面上和log.txt中print()
- API 还负责语法解析，将 query 语句分解为对应的块送入子模块中，比如 create_table 中得到 tableName__, attributes,  types, priKey, ifUniques

```python
def create_db(query) # 创建数据库
def drop_db(query) # 删除数据库
def use_db(query) # 改变当前数据库
def select_db(query) # 返回当前数据库名称，如 'mydb'
def show_databases(query) # 返回当前所有数据库，形式如下['mydb','yourdb']，注意，可能返回空列表
def show_table(query) # 返回当前数据库所有表，形式如下列表：['myTable', 'lory']

def create_table(query) # 创建表
def drop_table(query) # 删除表
def create_index(query) # 创建索引
def drop_index(query) # 删除索引


def select(query) #选择数据            --等新接口
def insert(query) #插入数据
def deletr(query) #删除数据            --等新接口

参数说明：
query           --输入sql语句，假定已通过interpreter去除了头尾的\n, ';'和所有空格

```