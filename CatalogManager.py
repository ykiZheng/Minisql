import os
DBs = {}
currentDB = ''
Indexs = {}
path = ''
"""
 @ 数据库
 @param DBName, Tables[]
"""
class DB():
    def __init__(self, DBName):
        self.DBName = DBName

    tables = []

"""
 @ 表
 @param tableName, priKey， Attributes[]
"""


class Table():
    Attributes = []

    def __init__(self, tableName, priKey=0):
        self.tableName = tableName
        self.priKey = priKey


"""
 @ 属性
 @param attributeName, ifPriKey, ifUnique, type, length
"""


class Attribute():
    def __init__(self, attributeName, ifPriKey, ifUnique, type='char', length=16):
        self.attributeName = attributeName
        self.ifPriKey = ifPriKey
        self.ifUnique = ifUnique
        self.type = type
        self.length = length


"""
 @ 索引
 @param indexName, tableName, attributeName
"""


class Index():
    def __init__(self, indexName, tableName, attributeName):
        self.indexName = indexName
        self.tableName = tableName
        self.attributeName = attributeName


# ---------------------------------------------------------------------------
# 语句信息结构
# ---------------------------------------------------------------------------
"""
 @ 创建表
 @param tableName, Attributes[]
"""


class CreateInfo():
    Attributes = []

    def __init__(self, tableName):
        self.tableName = tableName


"""
 @ 插入
 @param tableName, InsertNode[]
"""


class InsertInfo():
    InsertNode = []

    def __init__(self, tableName):
        self.tableName = tableName


class InsertNode():
    def __init__(self, attrName, attrValue):
        self.attrName = attrName
        self.attrValue = attrValue


"""
 @ 删除
 @param tableName, Attributes[]
"""


class DeleteInfo():
    DeleteNode = []

    def __init__(self, tableName):
        self.tableName = tableName


class DeleteNode():
    def __init__(self, attrName, attrValue):
        self.attrName = attrName
        self.attrValue = attrValue

# --------------------
# 数据库操作
# ---------------------

def createDB(DBName__):
    if existsDB(DBName__) == True:
        print('[create DB]\t创建数据库失败，当前已有该数据库名',DBName__)
    else:
        DBs[DBName__] = DB(DBName__)
        print('[create DB]\t创建数据库',DBName__,'成功')


def existsDB(DBName__):
    # if len(DBs) == 0:
    #     return False
    for i in range(0,len(DBs)):
        if DBs[i].DBName == DBName__:
            return True
    return False


def SwitchToDB(DBName__):
    global currentDB
    if existsDB(DBName__):
        currentDB = DBName__
        print('#当前切换到',DBName__,'库')
    else:
        print('#error: 当前不存在该数据库名')


# --------------------------------------
# 表操作
# --------------------------------
def createTable(tableName__, attrNames,  types, ifPris, ifUniques):
    global DBs
    global currentDB
    key = existsTable(tableName__)
    if  key == -1:
        Attributes = []
        for i in range(0, len(attrNames)):
            Attributes.append(Attribute(attrNames[i],ifPris[i],ifUniques[i],types[i]))
        new_table = Table(tableName__)
        new_table.Attributes = Attributes
        DBs[currentDB].tables.append(new_table)
        print('[Create Table]\t创建表',tableName__,'成功')
    else:
        print('[Create Table]\t已存在该表名',tableName__,'')
      

def dropTable(tableName__):
    global DBs
    key = existsTable(tableName__)
    if key == -1:
        print('[Drop Table]\t删除失败，不存在该表名',tableName__)
    else:
        del DBs[currentDB].tables[key]
        #del DBs[currentDB].tables[key].Attributes
        print('[Drop Table]\t删除表成功')


def existsTable(tableName__):
    for i in range(0,len(DBs[currentDB].tables)):
        if DBs[currentDB].tables[i].tableName == tableName__:
            print('!table already exists')
            return i
    return -1

def printTable():
    for i in range(0,len(DBs[currentDB].tables)):
        new_table = DBs[currentDB].tables[i]
        print(new_table.tableName)
        for j in range(0,len(new_table.Attributes)):
            new_attr = new_table.Attributes[j]
            print(new_attr.attributeName,'\t', new_attr.ifPriKey,'\t', new_attr.ifUnique, '\t',new_attr.type, '\t',new_attr.length)

# ------------------------
# 索引操作
# ----------------------
def IniIndex():
    a = 2

def createIndex(indexName, tableName, attributeName):
    if existsIndex(indexName):
        raise Exception('已创建该索引名'.format(indexName))
    else:
        Indexs['sys'] = Index(tableName, attributeName)


def dropIndex(indexName__):
    if existsIndex(indexName__):
        Indexs[indexName__].pop()
        print('删除索引成功')
    else:
        raise Exception('不存在该索引名'.format(indexName__))



def existsIndex(indexName__):
    for key in Indexs:
        if key == indexName__:
            return True
    return False

if __name__ == '__main__':
    createDB('myDB')
    SwitchToDB('myDB')
    createTable('myTable',['haha','nana'],['char','int'],[1,0],[0,0])
    dropTable('66')
    printTable()
    createTable('lory',['zju','cmu'],['char','int'],[1,0],[0,0])
    printTable()
    createTable('myTable',['haha','nana'],['char','int'],[1,0],[0,0])
    dropTable('myTable')
    printTable()