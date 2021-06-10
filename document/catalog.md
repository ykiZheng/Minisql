# CatalogManager接口要求
- 需要 bufferManager 传入 buf 结构
- 需要 indexManager 传入 BPlusTree 结构和结构说明
# CatalogManager接口说明
CatalogManager 会检查语法错误（但是不会检查有没有分号结尾，这是intepreter要干的），如果出现错误会抛出 MinisqlError 或者 MinisqlSyntexError供 Interpreter 判断，一般错误是不会停止程序，只会在界面上print()

```python
def createDB(DBName__) # 创建数据库
def dropDB(DBName__) # 删除数据库
def SwitchToDB(DBName__) # 改变当前数据库

def createTable(tableName__, attributes,  types, priKey, ifUniques) # 创建表
def dropTable(tableName__) # 删除表
def createIndex(indexName, tableName, attri) # 创建索引
def dropIndex(indexName__) # 删除索引

参数说明：
DBName__        --数据库名

tableName__     --表名
attributes      --属性名
types           --各属性对应类型名，包括int,char和double
priKey          --主码
ifUniques       --属性值是否唯一

indexName__     --索引名
attri           --建立索引的属性
buf             --Buffer Manager中Buffer类的一个实例，提供存储、访问接口

```