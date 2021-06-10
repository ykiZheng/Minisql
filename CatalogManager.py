#!/user/bin/env python
# -*- coding:utf-8 -*-
# module: CatalogManager.py
# create by 郑雨琪 on 2021/6/7

import os
import re
from typing import Tuple
from FileOp import *
import globalValue

# from BufferManager import *
# from IndexManager import BTree


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
        fileName = DBFiles.format(DBName__)
        fp = open(fileName, "w")
        fp.close()
        fileName = index_File.format(DBName__)
        fp = open(fileName, "w")
        fp.close()
        log('[create DB]\t创建数据库 '+DBName__+' 成功')


def dropDB(DBName__):
    path1 = DBFiles.format(DBName__)
    path2 = index_File.format(DBName__)
    if os.path.exists(path1) or os.path.exists(path2):
        if os.path.exists(path1):
            os.remove(path1)
        if os.path.exists(path2):
            os.remove(path2)
        log('[drop DB]\t\t删除数据库 ' + DBName__ + ' 成功')
    else:
        log('[drop DB]\t\t删除数据库失败，当前不存在数据库名为 ' + DBName__)


def SwitchToDB(DBName__):
    path = DBFiles.format(DBName__)
    if os.path.exists(path):
        globalValue.currentDB = DBName__
        print('#当前切换到', DBName__, '库')
    else:
        log('#error: 切换数据库失败，当前不存在该数据库名: '+globalValue.currentDB)


def printDB():
    print('当前本机拥有数据库如下：')
    for root, dirs, files in os.walk("DBFiles"):
        # print("root", root)  # 当前目录路径
        # print("dirs", dirs)  # 当前路径下所有子目录
        print("files", files)  # 当前路径下所有非目录子文件
# ---------------------------------
# 表操作
# --------------------------------


def convert(type):
    if type == 'int':
        return 0
    elif type == 'float':
        return -1
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
        for type in types:
            digit_types.append(convert(type))
        schema = {'attrs': attributes, 'types': digit_types,
                  'primary_key': priKey, 'uniques': ifUniques, 'index': []}
        schemas[tableName__] = schema
        store(schemas, path)
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
        if index:
            for col, IndexName in index:
                dropIndex(IndexName)

        schemas.pop(tableName__)
        store(schemas, path)
        log('[Drop Table]\t删除表 '+tableName__+' 成功')

    else:
        log('[Drop Table]\t删除失败，不存在该表名 ' + tableName__)


def getTableInfo():
    DBName__ = globalValue.currentDB
    print('当前本数据库', DBName__, '拥有表如下：')
    try:
        path = DBFiles.format(DBName__)
        # "DBFiles\\"+currentDB+".json"
        schemas = load(path)
    except FileNotFoundError:
        schemas = {}
    print(schemas)


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

# ------------------------
# 索引操作
# ----------------------


def createIndex(indexName, tableName, attri, buf):

    indexFile = index_File.format(globalValue.currentDB)
    path = DBFiles.format(globalValue.currentDB)
    schemas = load(path)
    Indexs = load(indexFile)

    if indexName in Indexs:
        log('[create Index]\t索引创建失败，已创建该索引名 '+indexName)
    else:

        if tableName in schemas:
            table = schemas[tableName]
            if existsAttr(tableName, attri):

                Indexs[indexName] = {'table': tableName, 'attri': attri}
                table['index'].append([attri, indexName])
                # --------------------需要Index的接口
                # tree = BPlusTree(TREE_ORDER)

                # table_blocks = []
                # for i, b in enumerate(buf.header):
                #     if b['table'] == tableName:
                #         table_blocks.append(i)

                # idx = table['cols'].index(attri)

                # for block_idx in table_blocks:
                #     b = buf.get_block(block_idx)
                #     records = b.data()

                #     for i, r in enumerate(records):
                #         ptr = block_idx * MAX_RECORDS_PER_BLOCK + i
                #         tree.insert(r[idx], ptr)

                # schemas[table]['index'].append([attri, indexName])

                store(schemas, path)
                store(Indexs, indexFile)
                # store(tree, indexFile)
                log('[create Index]\t索引 '+indexName+' 创建成功')

            else:
                log('[create Index]\t索引创建失败，该表不存在属性 '+attri)

        else:
            log('[create Index]\t索引创建失败，当前数据库不存在表 ' + tableName)


def dropIndex(indexName__):
    indexFile = index_File.format(globalValue.currentDB)
    path = DBFiles.format(globalValue.currentDB)
    schemas = load(path)
    Indexs = load(indexFile)

    if indexName__ not in Indexs:
        log('[drop Index]\t删除索引失败，不存在该索引名 ' + indexName__,)
    else:
        tableName = Indexs[indexName__]['table']
        Indexs.pop(indexName__)

        table = schemas[tableName]
        index = table['index']
        if index:
            for i, pair in enumerate(index):
                if pair[1] == indexName__:
                    index.pop(i)
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

# ---------------------
# test of CatalogManager
# --------------------


def main():
    buf = []
    createDB('myDB')
    createDB('yourDB')
    SwitchToDB('myDB')
    printDB()
    createTable('myTable', ['haha', 'nana'], [
                'char(100)', 'int'], 'haha', [True, True])
    dropTable('66')
    getTableInfo()
    createTable('lory', ['zju', 'cmu'], [
                'char(30)', 'int'], 'zju', [True, False])
    getTableInfo()
    createTable('myTable', ['haha', 'nana'], ['char(25)', 'int'], 1, [0, 0])
    dropTable('myTable')
    getTableInfo()

    # 索引测试
    createIndex('zjuIndex', 'lory', 'zju', buf)
    getIndexInfo()
    createIndex('zjuIndex', 'lory', '66', buf)
    createIndex('zju', 'lory', '66', buf)
    createIndex('z', 'harri', 'zju', buf)
    createIndex('cmuIndex', 'lory', 'cmu', buf)
    getIndexInfo()
    createIndex('hahIndex', 'myTable', 'haha', buf)
    dropIndex('cmuIndex')
    getIndexInfo()
    dropIndex('lal')
    getIndexInfo()
    printDB()
    dropDB('myDB')
    printDB()
    dropDB('heyheyehey')


if __name__ == '__main__':
    main()
