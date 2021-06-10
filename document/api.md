# API接口要求
- 需要interpreter去除头和尾的\n, ';'和空格
- 需要 bufferManager 传入 buf 结构
- 重要：在使用其他功能前必须先调用依次 use_db() 找到一个数据库才能进行其他操作
- 考虑是否设置 global的buffer....
# API接口说明
- API 会检查语法错误（但是不会检查有没有分号结尾，这是intepreter要干的），如果出现错误会抛出 MinisqlError 或者 MinisqlSyntexError供 Interpreter 判断，一般错误是不会停止程序，只会在界面上print()
- API 还负责语法解析，将 query 语句分解为对应的块送入子模块中，比如 create_table 中得到 tableName__, attributes,  types, priKey, ifUniques

```python
def create_db(query) # 创建数据库
def drop_db(query) # 删除数据库
def use_db(query) # 改变当前数据库
def select_db(query) # 输出某数据库下所有表信息

def create_table(query, buf) # 创建表
def drop_table(query, buf) # 删除表
def create_index(query, buf) # 创建索引
def drop_index(query, buf) # 删除索引


def select(query, buf) #选择数据
def insert(query, buf) #插入数据
def deletr(query, buf) #删除数据

参数说明：
query           --输入sql语句，假定已通过interpreter去除了头尾的\n, ';'和所有空格
buf             --来自buffermanager的类

```