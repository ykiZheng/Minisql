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

    tables = {}

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
    if len(DBs) == 0:
        return False
    for key in DBs:
        if key == DBName__:
            return True
    return False

def dropDB(DBName__):
    if existsDB(DBName__):
        del DBs[DBName__]
        print('[drop DB]\t删除数据库',DBName__,'成功')
    else:
        print('[drop DB]\t删除数据库失败，当前不存在数据库名为', DBName__)

def SwitchToDB(DBName__):
    global currentDB
    if existsDB(DBName__):
        currentDB = DBName__
        print('#当前切换到',DBName__,'库')
    else:
        print('#error: 当前不存在该数据库名')

def printDB():
    print('当前本机拥有数据库如下：')
    for i in DBs:
        print(i)
# --------------------------------------
# 表操作
# --------------------------------
def createTable(tableName__, attrNames,  types, ifPris, ifUniques):
    global DBs
    global currentDB
    if existsTable(tableName__):
        print('[Create Table]\t已存在该表名',tableName__,'')
    else:
        Attributes = []
        for i in range(0, len(attrNames)):
            Attributes.append(Attribute(attrNames[i],ifPris[i],ifUniques[i],types[i]))
        # new_table = Table(tableName__)
        # new_table.Attributes = Attributes
        DBs[currentDB].tables[tableName__] = Attributes
        print('[Create Table]\t创建表',tableName__,'成功')
             
def dropTable(tableName__):
    global DBs
    if existsTable(tableName__):
        del DBs[currentDB].tables[tableName__]
        #del DBs[currentDB].tables[key].Attributes
        print('[Drop Table]\t删除表成功')
        
    else:
        print('[Drop Table]\t删除失败，不存在该表名',tableName__)

def existsTable(tableName__):
    if len(DBs[currentDB].tables) == 0:
        return False
    for key in DBs[currentDB].tables:
        if key == tableName__:
            print('table already exists')
            return True
    return False

def printTable():
    print('当前本数据库',currentDB,'拥有表如下：')
    for i in DBs[currentDB].tables:
        print('##',i)
        new_table = DBs[currentDB].tables[i]
        for j in range(0,len(new_table)):
            new_attr = new_table[j]
            print(new_attr.attributeName,'\t', new_attr.ifPriKey,'\t', new_attr.ifUnique, '\t',new_attr.type, '\t',new_attr.length)

# ------------------------
# 索引操作
# ----------------------

def createIndex(indexName, tableName, attributeName):
    if existsIndex(indexName):
        print('[create Index]\t索引创建失败，已创建该索引名',indexName)
    else:
        if existsTable(tableName):
            if(existsAttr(tableName,attributeName)):    
                Indexs[indexName] ={'table':tableName, 'attri':attributeName}
                print('[create Index]\t索引创建成功')
            else:
                print('[create Index]\t索引创建失败，该表不存在属性',attributeName)
        else:
            print('[create Index]\t索引创建失败，当前数据库不存在表',tableName)

def dropIndex(indexName__):
    if existsIndex(indexName__):
        del Indexs[indexName__]
        print('[drop Index]\t删除索引',indexName__,'成功')
    else:
        print('[drop Index]\t删除索引失败，不存在该索引名',indexName__,)

def existsIndex(indexName__):
    for key in Indexs:
        if key == indexName__:
            return True
    return False

def printIndex():
    print('当前本数据库',currentDB,'拥有索引如下：')
    for key in Indexs:
        print(key,'\t',Indexs[key])

def existsAttr(tableName,attributeName):
    new_table = DBs[currentDB].tables[tableName]
    for i in range(0,len(new_table)):
        if new_table[i].attributeName == attributeName:
            return True
    return False


if __name__ == '__main__':
    createDB('myDB')
    createDB('YOURDB')
    SwitchToDB('myDB')
    createTable('myTable',['haha','nana'],['char','int'],[1,0],[0,0])
    dropTable('66')
    printTable()
    createTable('lory',['zju','cmu'],['char','int'],[1,0],[0,0])
    printTable()
    createTable('myTable',['haha','nana'],['char','int'],[1,0],[0,0])
    # dropTable('myTable')
    printTable()
    createIndex('zjuIndex','lory','zju')
    printIndex()
    createIndex('zjuIndex','lory','66')
    createIndex('zju','lory','66')
    createIndex('z','harri','zju')
    createIndex('cmuIndex','lory','cmu')
    printIndex()
    createIndex('hahIndex','myTable','haha')
    dropIndex('cmuIndex')
    printIndex()
    dropIndex('lal')
    printIndex()
    printDB()
    dropDB('myDB')
    printDB()
    dropDB('heyheyehey')
