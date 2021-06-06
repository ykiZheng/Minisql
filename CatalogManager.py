
DBs = {}
currentDB = ''
Indexs = {}
path = ''
"""
 @ 数据库
 @param DBName, Tables[]
"""


class DB():
    Tables = []

    def __init__(self, DBName):
        self.DBName = DBName


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


def existsDB():
    a = 2


def SwitchToDB(DBName):
    global currentDB
    currentDB = DBName


# --------------------------------------
# 表操作
# --------------------------------
def createTable(statement):
    global Tables


def dropTable(tableName):
    if existsTable(tableName):
        DBs[currentDB].Tables.pop(tableName)
    else:
        raise Exception('不存在该表名'.format(tableName))


def existsTable(tableName):
    for key in DBs[currentDB].Tables():
        if key == tableName:
            #raise Exception('table already exists')
            return True
    return False


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
        DBs[currentDB].Tables = Table(tableName, attributeName)


def dropIndex(indexName):
    if existsIndex(indexName):
        DBs[currentDB].Tables.pop()
    else:
        raise Exception('不存在该索引名'.format(indexName))



def existsIndex(indexName):
    for key in Indexs():
        if key == indexName:
            return True
    return False
