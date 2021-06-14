#!/user/bin/env python
# -*- coding:utf-8 -*-
# module: CatalogManager.py
# create by 郑雨琪 on 2021/6/7

from genericpath import exists
import os
import shutil
import re
from BPlusTree import *
from typing import Tuple
from index import *
from FileOp import *
import globalValue


def clear_all():
    shutil.rmtree('Index')
    shutil.rmtree('IIndex')
    shutil.rmtree('DBFiles')
# --------------------
# 数据库操作
# ---------------------

def createDB(DBName__):
    path = DBFiles.format(DBName__)
    if os.path.exists(path):
        log('[create DB]\t创建数据库失败，当前已有该数据库名 ' + DBName__)
    else:
        if not os.path.exists("DBFiles"):
            os.mkdir("DBFiles")
        if not os.path.exists("IIndex"):
            os.mkdir("IIndex")
        if not os.path.exists("Index"):
            os.mkdir("Index")
        fileName = DBFiles.format(DBName__)
        fp = open(fileName, "w")
        fp.close()
        fileName = index_File.format(DBName__)
        fp = open(fileName, "w")
        fp.close()
        fileName = index_filepath.format(DBName__)
        fp = open(fileName, "w")
        fp.close()
        fileName = list_filepath.format(DBName__)
        fp = open(fileName, "w")
        fp.close()
        fileName = data_filepath.format(DBName__)
        fp = open(fileName, "w")
        fp.close()

        index = Index()
        index.Load_file(index_filepath.format(DBName__), list_filepath.format(DBName__), data_filepath.format(DBName__))
        log('[create DB]\t创建数据库 '+DBName__+' 成功')


def dropDB(DBName__):
    path1 = DBFiles.format(DBName__)
    path2 = index_File.format(DBName__)
    path3 = index_filepath.format(DBName__)
    path4 = list_filepath.format(DBName__)
    path5 = data_filepath.format(DBName__)

    if os.path.exists(path1) or os.path.exists(path2) or os.path.exists(path3) or os.path.exists(path4) or os.path.exists(path5):
        if os.path.exists(path1):
            os.remove(path1)
        if os.path.exists(path2):
            os.remove(path2)
        if os.path.exists(path3):
            os.remove(path3)
        if os.path.exists(path4):
            os.remove(path4)
        if os.path.exists(path5):
            os.remove(path5)

        log('[drop DB]\t\t删除数据库 ' + DBName__ + ' 成功')
    else:
        log('[drop DB]\t\t删除数据库失败，当前不存在数据库名为 ' + DBName__)


def SwitchToDB(DBName__):
    
    path = DBFiles.format(DBName__)
    if os.path.exists(path):
        globalValue.currentDB = DBName__
        print('#当前切换到', DBName__, '库')
        globalValue.currentIndex.Load_file(index_filepath.format(DBName__), list_filepath.format(DBName__), data_filepath.format(DBName__))
        
    else:
        log('#error: 切换数据库失败，当前不存在该数据库名: '+DBName__)

    if globalValue.currentDB != None:
        globalValue.currentIndex.Save_file()
    return globalValue.currentDB


def printDB():
    # print('当前本机拥有数据库如下：')
    file = []
    for root, dirs, files in os.walk("DBFiles"):
        file.append(files)
        # print("root", root)  # 当前目录路径
        # print("dirs", dirs)  # 当前路径下所有子目录
        # print("files", files)  # 当前路径下所有非目录子文件
    return files
# ---------------------------------
# 表操作
# --------------------------------

def convertToString(digit):
    if digit == -1:
        return 'int'
    elif digit == 0:
        return 'float'
    else:
        return 'char('+digit+')'
def convert(type):
    if type == 'int':
        return -1
    elif type == 'float':
        return 0
    else:
        match = re.match(r'^char\((\d+)\)$', type, re.S)
        if match:
            n = int(match.group(1))
            if n >= 1 and n <= 255:
                return n
            else:
                return MiniSQLError('char(n): n is out of range [1, 255]')
        else:
            raise MiniSQLSyntaxError('Syntax Error in type {} '.format(type))


def createTable(tableName__, attributes,  types, priKey, ifUniques):
    try:
        path = DBFiles.format(globalValue.currentDB)
        schemas = load(path)
    except FileNotFoundError:
        schemas = {}
    if tableName__ not in schemas:
        digit_types = []
        indexs = []
        for type in types:
            digit_types.append(type)
            indexs.append(False)
        schema = {'attrs': attributes, 'types': digit_types,
                  'primary_key': priKey, 'uniques': ifUniques, 'index': []}
        schemas[tableName__] = schema
        store(schemas, path)

        globalValue.currentIndex.Create_table(tableName__, priKey, attributes)
        createIndex('PriKey_'+priKey,tableName__,priKey)
        log('[Create Table]\t创建表 ' + tableName__ + ' 成功')
    else:
        log('[Create Table]\t已存在该表名 ' + tableName__)


def dropTable(tableName__):
    try:
        path = DBFiles.format(globalValue.currentDB)

        schemas = load(path)
    except FileNotFoundError:
        schemas = {}
    if tableName__ in schemas:
        index = schemas[tableName__]['index']
        Prikey = schemas[tableName__]['primary_key']
        for IndexName, col in index:
            if col != Prikey:
                dropIndex(IndexName,False)
                schemas.pop(tableName__)
                store(schemas, path)
                globalValue.currentIndex.Drop_table(tableName__, Prikey)
                log('[Drop Table]\t删除表 '+tableName__+' 成功')
            else:
                dropIndex(IndexName, True)
                log('[drop table]\t不能删去主键索引')

        

    else:
        log('[Drop Table]\t删除失败，不存在该表名 ' + tableName__)


def showTables():
    DBName__ = globalValue.currentDB
    print('当前本数据库', DBName__, '拥有表如下：')
    try:
        path = DBFiles.format(DBName__)
        # "DBFiles\\"+currentDB+".json"
        schemas = load(path)
        names = []
        for name in schemas:
            names.append(name)
            # print(name)
        return names
    except FileNotFoundError:
        schemas = None
        return False


def existsTable(tableName__):
    try:
        path = DBFiles.format(globalValue.currentDB)
        schemas = load(path)
    except FileNotFoundError:
        schemas = {}
    if tableName__ in schemas:
        return True
    else:
        return False


def getTable(tableName__):
    DBName__ = globalValue.currentDB
    try:
        path = DBFiles.format(DBName__)
        schemas = load(path)
    except FileNotFoundError:
        schema = {}
    if tableName__ in schemas:
        schema = schemas[tableName__]
    else:
        schema = {}
    return schema

# ------------------------
# 索引操作
# ------------------------


def createIndex(indexName, tableName, attri):
    indexFile = index_File.format(globalValue.currentDB)
    path = DBFiles.format(globalValue.currentDB)
    schemas = load(path)
    Indexs = load(indexFile)

    if indexName in Indexs:
        log('[create Index]\t索引创建失败，已创建该索引名 '+indexName)
        return False
    else:

        if tableName in schemas:
            if existsAttr(tableName, attri):
                # for name in Indexs:
                #     if Indexs[name]['attri'] == attri and Indexs[name]['table'] == tableName:
                #         Indexs.pop(name)
                #         break
                        # log('[create Index]\t索引 '+indexName+' 创建成功')
                        # return True

                Indexs[indexName] = {'table': tableName, 'attri': attri}

                # 更新表结构
                schemas[tableName]['index'].append([indexName, attri])

                # --------------------需要Index的接口
                BT = BPlusTree()
                BT.BuildNewBPTree()
                table = globalValue.currentIndex.normal_list[tableName]
                if attri in table:
                    keys = table[attri]['keys']
                    values = table[attri]['values']
                else:
                    keys = []
                    values = []

                for i in range(len(keys)):
                    BT.Insert_node(keys[i], values[i])
                globalValue.currentIndex.index_trees[tableName][attri] = BT.Trees
                if attri in table:
                    globalValue.currentIndex.normal_list[tableName].pop(attri)

                store(schemas, path)
                store(Indexs, indexFile)
                log('[create Index]\t索引 '+indexName+' 创建成功')

            else:
                log('[create Index]\t索引创建失败，该表不存在属性 '+attri)

        else:
            log('[create Index]\t索引创建失败，当前数据库不存在表 ' + tableName)


def dropIndex(indexName__, ifPri):
    indexFile = index_File.format(globalValue.currentDB)
    path = DBFiles.format(globalValue.currentDB)
    schemas = load(path)
    Indexs = load(indexFile)
    

    if indexName__ not in Indexs:
        log('[drop Index]\t删除索引失败，不存在该索引名 ' + indexName__,)
    else:


        tableName = Indexs[indexName__]['table']
        attri = Indexs[indexName__]['attri']
        table = schemas[tableName]

        if table['primary_key'] == attri and ifPri == False:
            log('[drop Index]\t删除索引失败，无法删除主键索引 ' + indexName__,)
            return False
        Indexs.pop(indexName__)

        
        index = table['index']
        attri = []
        if index:
            for i, pair in enumerate(index):
                if pair[0] == indexName__:
                    attri = pair[1]
                    index.pop(i)
                    table = globalValue.currentIndex.index_trees[tableName]
                    keys = table[attri]['keys']
                    values = table[attri]['values']
                    if attri in globalValue.currentIndex.index_trees[tableName]:
                        globalValue.currentIndex.index_trees[tableName].pop(attri)
                    
                    
                    normal_list = globalValue.currentIndex.normal_list
                    norm = {}
                    norm['keys'] = keys
                    norm['values'] = values
                    globalValue.currentIndex.normal_list[tableName][attri] = norm
            

                    # table['attrs'].append(attri)
                    # table['types'].append(convertToString(TypeOfAttr(tableName,attri)))
                    # table['uniques'].append(UniqueOfAttr(tableName,attri))

                    # table
                    break

            store(schemas, path)
            store(Indexs, indexFile)
            log('[drop Index]\t删除索引 ' + indexName__ + ' 成功')


def getIndexInfo():
    print('当前本数据库', globalValue.currentDB, '拥有索引如下：')
    indexFile = index_File.format(globalValue.currentDB)
    Indexs = load(indexFile)
    print(Indexs)


def existsAttr(tableName, attri):
    try:
        path = DBFiles.format(globalValue.currentDB)
        schemas = load(path)
    except FileNotFoundError:
        schemas = {}
    table = schemas[tableName]
    attris = table['attrs']
    if attris:
        for i, pair in enumerate(attris):
            if pair == attri:
                return True
    return False


def UniqueOfAttr(tableName, attr):
    try:
        path = DBFiles.format(globalValue.currentDB)
        schemas = load(path)
    except FileNotFoundError:
        schemas = {}
    table = schemas[tableName]
    attris = table['attrs']
    unique = table['uniques']
    if attris:
        for i, pair in enumerate(attris):
            if pair == attr:
                break
        return unique[i]


def TypeOfAttr(tableName, attr):
    try:
        path = DBFiles.format(globalValue.currentDB)
        schemas = load(path)
    except FileNotFoundError:
        schemas = {}
    table = schemas[tableName]
    attris = table['attrs']
    types = table['types']
    if attris:
        for i, pair in enumerate(attris):
            if pair == attr:
                break
        return types[i]

def IndexOfAttr(tableName, attr):
    try:
        path = DBFiles.format(globalValue.currentDB)
        schemas = load(path)
    except FileNotFoundError:
        schemas = {}
    table = schemas[tableName]
    index = table['index']
    for i, pair in enumerate(index):
        if pair[0] == attr:
            return True
            break
    return False
# ---------------------
# test of CatalogManager
# --------------------


def main():
    clear_all()
    createDB('myDB')
    createDB('yourDB')
    SwitchToDB('myDB')

    printDB()
    createTable('myTable', ['haha', 'nana','lalalal','hhh','111','222','333'], [
                'char(100)', 'int', 'int', 'int', 'int', 'int', 'int'], 'haha', [True, True,True, True,True, True,True])
    dropTable('66')
    showTables()
    createTable('lory', ['zju', 'cmu'], [
                'char(30)', 'int'], 'zju', [True, False])
    showTables()
    createTable('myTable', ['haha', 'nana'], ['char(25)', 'int'], 1, [0, 0])
    globalValue.currentIndex.Save_file()
    dropTable('myTable')
    globalValue.currentIndex.Save_file()
    showTables()
    UniqueOfAttr('lory', 'zju')

    # 索引测试
    createIndex('zjuIndex', 'lory', 'zju')
    createIndex('111Index', 'myTable', '111')
    createIndex('222Index', 'myTable', '222')
    
    createIndex('333Index', 'myTable', '333')
    createIndex('lalalalIndex', 'myTable', 'lalalal')
    
    dropIndex('333Index',False)
    
    getIndexInfo()
    createIndex('zjuIndex', 'lory', '66')
    createIndex('zju', 'lory', '66')
    createIndex('z', 'harri', 'zju')
    createIndex('cmuIndex', 'lory', 'cmu')
    globalValue.currentIndex.Save_file()
    print(showTables())
    # createIndex('hahIndex', 'myTable', 'haha')
    dropIndex('cmuIndex',False)
    dropIndex('zjuIndex',True)
    # getIndexInfo()
    # dropIndex('lal')
    # getIndexInfo()
    # printDB()
    # dropDB('myDB')
    # printDB()
    # dropDB('heyheyehey')
    globalValue.currentIndex.Save_file()

if __name__ == '__main__':
    main()
