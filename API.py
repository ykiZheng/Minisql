#!/user/bin/env python
# -*- coding:utf-8 -*-
# module: API.py
# create by 郑雨琪 on 2021/6/7

from abc import abstractproperty
from CatalogManager import *
import re
from FileOp import *
import globalValue

# --------------------------------数据库操作---------
"""
@ 创建新数据库
@param: 形如 create database 库名;\n
        设定传进来的已经trim并去除了多余的空格
"""


def create_db(query):
    # query = query.strip(';\n')
    match = re.match(
        r'^create\s+database\s+([a-z](\w)*)$', query, re.S)
    if match:
        tableName = match.group(1)
        createDB(tableName)
    else:
        raise MiniSQLSyntaxError('Syntax Error in: ', query)


"""
@ 删除数据库
@param: 形如 drop database 库名;\n
        设定传进来的已经trim并去除了多余的空格
"""


def drop_db(query):
    match = re.match(
        r'^drop\s+database\s+([a-z][0-9a-z_]*)$', query, re.S)
    if match:
        DBName = match.group(1)
        dropDB(DBName)
    else:
        raise MiniSQLSyntaxError('Syntax Error in: ', query)


def use_db(query):
    match = re.match(
        r'^use\s+([a-z][0-9a-z_]*)$', query, re.S)
    if match:
        DBName__ = match.group(1)
        SwitchToDB(DBName__)
    else:
        raise MiniSQLSyntaxError('Syntax Error in: ', query)


"""
@ 查看当前数据库
@param: 形如 select database();\n
        设定传进来的已经trim并去除了多余的空格
"""


def select_db(query):
    match = re.match(r'^select\s+database\s*\(\s*\)$', query, re.S)
    if match:
        DBName__ = match.group(1)
        getTableInfo(DBName__)
    else:
        raise MiniSQLSyntaxError('Syntax Error in: ', query)

# -------------------------- 表操作 -----------------------


"""
@param: 形如 CREATE TABLE 表名(
                字段名 字段类型 PRIMARY KEY, #主键
                字段名 字段类型 UNIQUE, #唯一
            );
"""


def create_table(query):
    match = re.match(
        r'^create\s+table\s+([a-z]\w*)\s*\((.+)\)$', query, re.S)
    # match = re.match(r'^create\s+table\s+\(\s+\)\s*\((.+)\);\n$',query,re.S)
    if match:
        tableName, cols = match.groups()
        attris, types, ifUniques = [], [], []
        ExistsPri = 0
        key = None
        for a in cols.split(','):
            a = a.strip()
            priKey = re.match(r'^\s*primary\s+key\s*\((.+)\)\s*$', a, re.S)
            if priKey:
                key = priKey.group(1)
                ExistsPri += 1
            else:
                a = a.split(' ')
                attri, type__ = a[0].strip(), a[1].strip()

                if len(a) == 3 and a[2].strip() == 'unique':
                    ifUniques.append(True)
                elif len(a) != 2:
                    raise MiniSQLSyntaxError('Syntax Error in: ', query)
                attris.append(attri)
                types.append(type__)
        if ExistsPri > 1:
            raise MiniSQLError('Multiple primary keys')
        else:
            createTable(tableName.strip(), attris, types, key, ifUniques)
    else:
        raise MiniSQLSyntaxError('Syntax Error in: ', query)


def drop_table(query, buf):
    match = re.match(
        r'^drop\s+database\s+([a-z][0-9a-z_]*)$', query, re.S)
    if match:
        tableName = match.group(1)
        # dropTable(tableName, buf)
        dropTable(tableName)
    else:
        raise MiniSQLSyntaxError('Syntax Error in: ', query)


def create_index(query, buf):
    match = re.match(
        r'^create\s+index\s+(.+)+on\s+(.+)\s*\((.+)\)$', query, re.S)
    # r'^create\s+index\s+([a-z][a-z0-9_]*)+on\s+([a-z][a-z0-9_]*)\s*\(([a-z][a-z0-9_]*)\)$', query, re.S)
    if match:
        indexName = match.group(1).strip()
        tableName = match.group(2).strip()
        attri = match.group(3).strip()
        # createIndex(indexName, tableName, attri, buf)
        createIndex(indexName, tableName, attri, buf)
    else:
        raise MiniSQLSyntaxError('Syntax Error in: ', query)


def drop_index(query):
    match = re.match(
        r'^drop\s+index\s+([a-z](\w)*)$', query, re.S)
    if match:
        indexName = match.group(1).strip()
        dropIndex(indexName)
    else:
        raise MiniSQLSyntaxError('Syntax Error in: ', query)
# ---------------------------------------


def insert(query, buf):
    match = re.match(
        r'^insert\s+into\s+([a-z](\w)*)+\s+values\s*\((.+)\)\s*;\s*\n*$', query, re.S)
    if match:
        tableName = match.group(1).strip()
        values = []
        for v in match.group(2).split(','):
            values.append(v.strip())
            # Insert(tableName, values, buf)
    else:
        raise MiniSQLSyntaxError('Syntax Error in: ', query)


def delete(query, buf):
    match = re.match(
        r'^delete\s+from\s+([a-z](\w)*)+\s+where\s*(.+)\s*;\s*\n*$', query, re.S)
    if match:
        tableName = match.group(1).strip()
        condition = match.group(2).strip()
        #Delete(tableName, condition,buf)
    else:
        match = re.match(
            r'^delete\s+from\s+([a-z](\w)*)+\s*;\s*\n*$', query, re.S)
        if match:
            tableName = match.group(1).strip()
            condition = None
            #Delete(tableName, condition,buf)
        else:
            raise MiniSQLSyntaxError('Syntax Error in: ', query)


def select(query, buf):
    match = re.match(
        r'^select\s+(.+)\s+from\s+([a-z](\w)*)+\s+where\s*(.+)\s*;\s*\n*$', query, re.S)
    if match:
        cols = match.group(1).strip()
        tableName = match.group(2).strip()
        condition = match.group(3).strip()
    else:
        match = re.match(
            r'^select\s+(.+)\s+from\s+([a-z](\w)*)+\s*;\s*\n*$', query, re.S)
        if match:
            cols = match.group(1).strip()
            tableName = match.group(2).strip()
            condition = None
        else:
            raise MiniSQLSyntaxError('Syntax Error in: ', query)
    if cols != '*':
        cols = cols.strip('()').split(',')
        for c in cols:
            cols.append(c.strip())
    # return Select(cols, tableName, condition,buf)


def main():
    buf = []
    create_db('create database lll')
    create_db('create database mingming')
    use_db('use yourdb')
    create_table(
        'create table gogo1(id int, stuName char(20), gender char(1), seat int)')
    create_table(
        'create table gogo2(id int, stuName char(20), gender char(1), seat int,primary key (id))')
    getTableInfo()

    print(globalValue.currentDB)
    create_index('create index id_index on gogo1(id)', buf)
    create_index('create index gender_index on gogo2(gender)', buf)
    getIndexInfo()
    getTableInfo()
    drop_index('drop index id_index')

    drop_db('drop database lll')


if __name__ == '__main__':
    main()
