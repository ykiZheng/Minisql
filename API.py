#!/user/bin/env python
# -*- coding:utf-8 -*-
# module: API.py
# create by 郑雨琪 on 2021/6/7

from abc import abstractproperty
from os import sep
from CatalogManager import *
from index import Index
import re
from FileOp import *
import globalValue
from prettytable import PrettyTable

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
        raise MiniSQLSyntaxError('Syntax Error in: '+ query)


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
        raise MiniSQLSyntaxError('Syntax Error in: '+ query)


def use_db(query):
    match = re.match(
        r'^use\s+([a-z][0-9a-z_]*)$', query, re.S)
    if match:
        DBName__ = match.group(1)
        SwitchToDB(DBName__)
        
    else:
        raise MiniSQLSyntaxError('Syntax Error in: '+ query)


"""
@ 查看当前数据库
@param: 形如 select database();\n
        设定传进来的已经trim并去除了多余的空格
"""


def select_db(query):
    match = re.match(r'^select\s+database\s*\(\s*\)$', query, re.S)
    if match:
        # log('[select database]\t当前数据库为 '+globalValue.currentDB)
        return globalValue.currentDB
    else:
        raise MiniSQLSyntaxError('Syntax Error in: '+ query)

def show_dbs(query):
    match = re.match(r'^show\s+databases)$', query, re.S)
    if match:
        return  printDB()
    else:
        raise MiniSQLSyntaxError('Syntax Error in: '+ query)

def show_tables(query):
    match = re.match(r'^show\s+tables$', query, re.S)
    if match:
        return showTables()
    else:
        raise MiniSQLSyntaxError('Syntax Error in: '+ query)
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
                type__ = convert(type__)

                if len(a) == 3 and a[2].strip() == 'unique':
                    ifUniques.append(True)
                elif len(a) != 2:
                    raise MiniSQLSyntaxError('Syntax Error in: '+ query)
                else:
                    ifUniques.append(False)
                attris.append(attri)
                types.append(type__)
        if ExistsPri > 1:
            raise MiniSQLError('Multiple primary keys')
        else:
            if key == None:
                key = attris[0]
            for i,pair in enumerate(attris):
                if pair == key:
                    ifUniques[i] = True
                    break
            createTable(tableName.strip(), attris, types, key, ifUniques)
    else:
        raise MiniSQLSyntaxError('Syntax Error in: '+ query)


def drop_table(query):
    match = re.match(
        r'^drop\s+database\s+([a-z][0-9a-z_]*)$', query, re.S)
    if match:
        tableName = match.group(1)
        # dropTable(tableName, buf)
        dropTable(tableName)
        
    else:
        raise MiniSQLSyntaxError('Syntax Error in: ', query)


def create_index(query):
    match = re.match(
        r'^create\s+index\s+(.+)+on\s+(.+)\s*\((.+)\)$', query, re.S)
    # r'^create\s+index\s+([a-z][a-z0-9_]*)+on\s+([a-z][a-z0-9_]*)\s*\(([a-z][a-z0-9_]*)\)$', query, re.S)
    if match:
        indexName = match.group(1).strip()
        tableName = match.group(2).strip()
        attri = match.group(3).strip()
        createIndex(indexName, tableName, attri)
    else:
        raise MiniSQLSyntaxError('Syntax Error in: ', query)


def drop_index(query):
    match = re.match(
        r'^drop\s+index\s+([a-z](\w)*)$', query, re.S)
    if match:
        indexName = match.group(1).strip()
        dropIndex(indexName,False)
    else:
        raise MiniSQLSyntaxError('Syntax Error in: ', query)
# ---------------------------------------


def insert(query):
    match = re.match(
        r'^insert\s+into\s+([a-z][a-z0-9_]*)\s+values\s*\((.+)\)$', query, re.S)
    if match:
        tableName = match.group(1).strip()
        Values = match.group(2).strip()
        values = []
        uniques = []
        if existsTable(tableName):
            schema = getTable(tableName)
            attrs = schema['attrs']
            types = schema['types']
            uniques = schema['uniques']

            num = 0
            # for attr in attrs:
            #     index_list.append(IndexOfAttr(tableName,attr))
            for v in Values.split(','):
                vv = v.strip()
                if types[num] == -1:
                    vv = int(vv)
                elif types[num] == 0:
                    vv = float(vv)
                num+=1
                values.append(vv)
            globalValue.currentIndex.Insert_into_table(tableName,attrs,types,values,uniques) 
        else:
            raise MiniSQLError('[insert]\t不存在该表'+tableName)
    else:
        raise MiniSQLSyntaxError('Syntax Error in: '+ query)

# where 后语句分解为 属性 attrs、操作 ops、值keys
def seperateCondition(query,condition):
    schema = {}
    attrs = []
    ops = []
    keys = []
    for subCond in condition.split('and'):
        subCond = subCond.strip()
        match = re.match(r'^([a-z0-9][a-z0-9_]*)\s+=\s+([a-z0-9]+)$', subCond, re.S)
        # 情况1：等于
        if match:
            attrs.append(match.group(1).strip())
            ops.append(0)
            keys.append(match.group(2).strip())
        else:
            match = re.match(r'^([a-z0-9][a-z0-9_]*)\s+<>\s+([a-z0-9]+)$', subCond, re.S)
            # 情况2：不等于
            if match:
                attrs.append(match.group(1).strip())
                ops.append(1)
                keys.append(match.group(2).strip())
            else:
                match = re.match(r'^([a-z0-9][a-z0-9_]*)\s+<\s+([a-z0-9]+)$', subCond, re.S)
                # 情况3：小于
                if match:
                    attrs.append(match.group(1).strip())
                    ops.append(2)
                    keys.append(match.group(2).strip())
                else:
                    match = re.match(r'^([a-z0-9][a-z0-9_]*)\s+>\s+([a-z0-9]+)$', subCond, re.S)
                    # 情况4：大于
                    if match:
                        attrs.append(match.group(1).strip())
                        ops.append(3)
                        keys.append(match.group(2).strip())
                    else:
                        match = re.match(r'^([a-z0-9][a-z0-9_]*)\s+<=\s+([a-z0-9]+)$', subCond, re.S)
                        # 情况5：小于等于
                        if match:
                            attrs.append(match.group(1).strip())
                            ops.append(4)
                            keys.append(match.group(2).strip())
                        else:
                            match = re.match(r'^([a-z0-9][a-z0-9_]*)\s+>=\s+([a-z0-9]+)$', subCond, re.S)
                            # 情况6：大于等于
                            if match:
                                attrs.append(match.group(1).strip())
                                ops.append(3)
                                keys.append(match.group(2).strip())
                            else:
                                raise MiniSQLSyntaxError('Syntax Error in: ', query)
    schema['keys'] = keys
    schema['attrs'] = attrs
    schema['ops'] = ops
    return schema


def delete(query):
    match = re.match(
        r'^delete\s+from\s+([a-z][a-z0-9_]*)\s+where\s+(.+)$', query, re.S)
    # 情况1：有 where
    if match:
        tableName = match.group(1).strip()
        condition = match.group(2).strip()
        if existsTable(tableName):
            schema = getTable(tableName)
            isPri = schema['primary_key']
            unique = []
            isindex = []
            types = []
            
            subCond = seperateCondition(query,condition.strip())
            attrs = subCond['attrs']
            for attr in attrs:
                if not existsAttr(tableName,attr):
                    raise MiniSQLError('[delete]\t表 '+tableName+' 中不存在该属性 '+attr)
                else:
                    unique.append(UniqueOfAttr(tableName,attr))
                    isindex.append(IndexOfAttr(tableName,attr))
                    types.append(TypeOfAttr(tableName,attr))
            keys = subCond['keys']
            ops = subCond['ops']

            #------------------------------有问题，待改，等待新接口
            return globalValue.currentIndex.Drop_field_from_table(tableName, attrs, isPri, keys, isindex, unique)
        else:
            raise MiniSQLError('[delete]\t不存在该表'+tableName)
    else:
        match = re.match(
            r'^delete\s+from\s+([a-z][a-z0-9_]*)$', query, re.S)
        # 情况2：无where
        if match:
            tableName = match.group(1).strip()
            condition = None
            if existsTable(tableName):
                condition = condition
                #----------------------------------等新接口
                # return globalValue.currentIndex.Drop_field_from_table(tableName, attrs, isPri, keys, isindex, unique)
            else:
                raise MiniSQLError('[delete]\t不存在该表 '+tableName)
        else:
            raise MiniSQLSyntaxError('Syntax Error in: ', query)


def select(query):
    match = re.match(
        r'^select\s+(.+)\s+from\s+([a-z][a-z0-9_]*)+\s+where\s*(.+)$', query, re.S)
    # 情况1：有where
    if match:
        cols = match.group(1).strip()
        tableName = match.group(2).strip()
        condition = match.group(3).strip()

        if existsTable(tableName):
            uniques = []
            ifindexs = []
            types = []
            subcon = seperateCondition(query,condition)
            attrs =subcon['attrs']
            for attr in attrs:
                if not existsAttr(tableName,attr):
                    raise MiniSQLError('[select]\t表 '+tableName+' 中不存在该属性 '+attr)
                else:
                    uniques.append(UniqueOfAttr(tableName,attr))
                    ifindexs.append(IndexOfAttr(tableName,attr))
                    types.append(TypeOfAttr(tableName,attr))
            schema = getTable(tableName)
            isPri = schema['primary_key']
            keys = subcon['keys']
            ops = subcon['ops']
            
        
            
            
        else:
            raise MiniSQLError('[select]\t不存在该表 '+tableName)
    else:
        
        match = re.match(
            r'^select\s+(.+)\s+from\s+([a-z](\w)*)$', query, re.S)
        # 情况2：没有 where
        if match:
            cols = match.group(1).strip()
            tableName = match.group(2).strip()
            schema = getTable(tableName)
            isPri = schema['primary_key']
            ifindexs = []
            attrs = []
            types = []
            keys = []
            ops = []

        else:
            raise MiniSQLSyntaxError('Syntax Error in: '+ query)
    if cols != '*':
        raise MiniSQLSyntaxError('Syntax Error in: '+ query)
        # cols = cols.strip('()').split(',')
        # for c in cols:
        #     cols.append(c.strip())
    else:
        select_res = globalValue.currentIndex.Select_from_table(tableName,attrs,types,keys,ifindexs)
        
        if select_res:
            output = {}
            output['attrs'] = schema['attrs']
            output['select_res'] = select_res
            return output
        else:
            log('[select]\tNot Found')
    # return Select(cols, tableName, condition,buf)


def main():
    
    clear_all()
    create_db('create database lll')
    create_db('create database yourdb')
    use_db('use yourdb')
    create_table(
        'create table gogo1(id int, stuName char(20), gender char(1), seat int)')
    create_table(
        'create table gogo2(id int, stuName char(20), gender char(1), seat int,primary key (id))')
    showTables()

    print(globalValue.currentDB)
    create_index('create index id_index on gogo1(id)')
    create_index('create index gender_index on gogo2(gender)')
    getIndexInfo()
    showTables()
    drop_index('drop index id_index')

    select_db('select database ()')
    drop_db('drop database lll')
    # print(select('select * from gogo1'))
    for i in range(1,1000):
        insert('insert into gogo1 values('+str(i)+',zyq,G,'+str(i+31)+')')

    delete('delete from gogo1 where id = 1')
    # print(select('select * from gogo1 where id = 319'))
    globalValue.currentIndex.Save_file()

if __name__ == '__main__':
    main()
