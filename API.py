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
        raise MiniSQLSyntaxError('Syntax Error in: ' + query)


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
        if DBName == globalValue.currentDB:
            log('[drop db]\t无法删除正在使用的库 '+DBName)
        else:
            dropDB(DBName)
    else:
        raise MiniSQLSyntaxError('Syntax Error in: ' + query)


def use_db(query):
    match = re.match(
        r'^use\s+([a-z][0-9a-z_]*)$', query, re.S)
    if match:
        DBName__ = match.group(1)
        SwitchToDB(DBName__)

    else:
        raise MiniSQLSyntaxError('Syntax Error in: ' + query)


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
        raise MiniSQLSyntaxError('Syntax Error in: ' + query)


def show_dbs(query):
    match = re.match(r'^show\s+databases$', query, re.S)
    if match:
        return printDB()
    else:
        raise MiniSQLSyntaxError('Syntax Error in: ' + query)


def show_tables(query):
    match = re.match(r'^show\s+tables$', query, re.S)
    if match:
        return showTables()
    else:
        raise MiniSQLSyntaxError('Syntax Error in: ' + query)
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
                    raise MiniSQLSyntaxError('Syntax Error in: ' + query)
                else:
                    ifUniques.append(False)
                attris.append(attri)
                types.append(type__)
        if ExistsPri > 1:
            raise MiniSQLError('Multiple primary keys')
        else:
            if key == None:
                key = attris[0]
            for i, pair in enumerate(attris):
                if pair == key:
                    ifUniques[i] = True
                    break
            createTable(tableName.strip(), attris, types, key, ifUniques)
    else:
        raise MiniSQLSyntaxError('Syntax Error in: ' + query)


def drop_table(query):
    match = re.match(
        r'^drop\s+table\s+([a-z][0-9a-z_]*)$', query, re.S)
    if match:
        tableName = match.group(1)
        # dropTable(tableName, buf)
        dropTable(tableName)

    else:
        raise MiniSQLSyntaxError('Syntax Error in: ' + query)


def create_index(query):
    match = re.match(
        r'^create\s+index\s+(.+)+on\s+(.+)\s*\((.+)\)$', query, re.S)

    if match:
        indexName = match.group(1).strip()
        tableName = match.group(2).strip()
        attri = match.group(3).strip()
        createIndex(indexName, tableName, attri)
    else:
        raise MiniSQLSyntaxError('Syntax Error in: '+query)


def drop_index(query):
    match = re.match(
        r'^drop\s+index\s+([a-z][a-z0-9_]*)$', query, re.S)
    if match:
        indexName = match.group(1).strip()
        dropIndex(indexName, False)
    else:
        raise MiniSQLSyntaxError('Syntax Error in: ' + query)
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
            indexs = []

            for attr in attrs:
                index = IndexOfAttr(tableName, attr)
                indexs.append(index)
                # indexs.append(IndexOfAttr(tableName,attr))

            num = 0
            # for attr in attrs:
            #     index_list.append(IndexOfAttr(tableName,attr))
            for v in Values.split(','):
                vv = v.strip()
                vv = vv.strip('"')
                vv.strip()
                if types[num] == -1:
                    vv = int(vv)
                elif types[num] == 0:
                    vv = float(vv)
                num += 1
                values.append(vv)
            globalValue.currentIndex.Insert_into_table(
                tableName, attrs, types, values, indexs)
        else:
            raise MiniSQLError('[insert]\t不存在该表'+tableName)
    else:
        raise MiniSQLSyntaxError('Syntax Error in: ' + query)

# where 后语句分解为 属性 attrs、操作 ops、值keys


def seperateCondition(query, condition):
    condition = condition.strip()
    schema = {}
    attrs = []
    ops = []
    keys = []
    for subCond in condition.split('and'):
        subCond = subCond.strip()
        match = re.match(
            r'^([a-z0-9][a-z0-9_]*)\s*=\s*(.+)$', subCond, re.S)
        
        # 情况1：等于
        if match:
            attrs.append(match.group(1).strip())
            ops.append(0)
            key = match.group(2).strip()
            key = key.strip('"')
            key = key.strip()
            keys.append(key)
        else:
            match = re.match(
                r'^([a-z0-9][a-z0-9_]*)\s*<>\s*\"*(.+)\"*$', subCond, re.S)
            # 情况2：不等于
            if match:
                attrs.append(match.group(1).strip())
                ops.append(1)
                key = match.group(2).strip()
                key = key.strip('"')
                key = key.strip()
                keys.append(key)
            else:
                match = re.match(
                    r'^([a-z0-9][a-z0-9_]*)\s*<=\s*\"*(.+)\"*$', subCond, re.S)
                # 情况3：小于等于
                if match:
                    attrs.append(match.group(1).strip())
                    ops.append(4)
                    key = match.group(2).strip()
                    key = key.strip('"')
                    key = key.strip()
                    keys.append(key)
                else:
                    match = re.match(
                        r'^([a-z0-9][a-z0-9_]*)\s*>\s*\"*(.+)\"*$', subCond, re.S)
                    # 情况4：大于等于
                    if match:
                        attrs.append(match.group(1).strip())
                        ops.append(5)
                        key = match.group(2).strip()
                        key = key.strip('"')
                        key = key.strip()
                        keys.append(key)
                    else:
                        match = re.match(
                            r'^([a-z0-9][a-z0-9_]*)\s*<=\s*\"*(.+)\"*$', subCond, re.S)
                        # 情况5：小于等于
                        if match:
                            attrs.append(match.group(1).strip())
                            ops.append(2)
                            key = match.group(2).strip()
                            key = key.strip('"')
                            key = key.strip()
                            keys.append(key)
                        else:
                            match = re.match(
                                r'^([a-z0-9][a-z0-9_]*)\s*>=\s*\"*(.+)\"*$', subCond, re.S)
                            # 情况6：大于等于
                            if match:
                                attrs.append(match.group(1).strip())
                                ops.append(3)
                                key = match.group(2).strip()
                                key = key.strip('"')
                                key = key.strip()
                                keys.append(key)
                            else:
                                raise MiniSQLSyntaxError(
                                    'Syntax Error in: ' + query)
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
            Types = schema['types']
            Attrs = schema['attrs']
            index = schema['index']
            unique = []
            isindex = []
            types = []

            subCond = seperateCondition(query, condition.strip())
            attrs = subCond['attrs']
            keys = subCond['keys']
            ops = subCond['ops']
            for i in range(0, len(attrs)):
                if not existsAttr(tableName, attrs[i]):
                    raise MiniSQLError(
                        '[delete]\t表 '+tableName+' 中不存在该属性 '+attrs[i])
                else:
                    unique.append(UniqueOfAttr(tableName, attrs[i]))
                    isindex.append(IndexOfAttr(tableName, attrs[i]))
                    Type = TypeOfAttr(tableName, attrs[i])
                    types.append(Type)
                    keys[i] = TypeChange(keys[i], Type)

            return globalValue.currentIndex.Delete_and_join(tableName, Attrs, Types, index, attrs, keys, isindex, ops)

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
                schema = getTable(tableName)
                isPri = schema['primary_key']
                Types = schema['types']
                Attrs = schema['attrs']
                index = schema['index']

                # ----------------------------------有问题
                # ----------------------------------等新接口
                IIndex = TypeChange('1', Types[0])
                ifIndex = IndexOfAttr(tableName, Attrs[0])
                flag1 = globalValue.currentIndex.Delete_and_join(
                    tableName, Attrs, Types, index, [Attrs[0]], [IIndex], [ifIndex], [0])
                flag2 = globalValue.currentIndex.Delete_and_join(
                    tableName, Attrs, Types, index, [Attrs[0]], [IIndex], [ifIndex], [1])
                return flag1 or flag2
            else:
                raise MiniSQLError('[delete]\t不存在该表 '+tableName)
        else:
            raise MiniSQLSyntaxError('Syntax Error in: ' + query)


def TypeChange(key, Type):
    if Type == -1:
        return int(key)
    elif Type == 0:
        return float(key)
    elif Type <= 255:
        return key


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
            subcon = seperateCondition(query, condition)
            attrs = subcon['attrs']
            keys = subcon['keys']
            ops = subcon['ops']
            for i in range(0, len(attrs)):
                if not existsAttr(tableName, attrs[i]):
                    raise MiniSQLError(
                        '[select]\t表 '+tableName+' 中不存在该属性 '+attrs[i])
                else:
                    uniques.append(UniqueOfAttr(tableName, attrs[i]))
                    ifindexs.append(IndexOfAttr(tableName, attrs[i]))
                    Type = TypeOfAttr(tableName, attrs[i])
                    types.append(Type)
                    keys[i] = TypeChange(keys[i], Type)

            schema = getTable(tableName)
            isPri = schema['primary_key']
            Types = schema['types']

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
            attrs = schema['attrs']
            Types = schema['types']
            ops = -1

        else:
            raise MiniSQLSyntaxError('Syntax Error in: ' + query)
    if cols != '*':
        raise MiniSQLSyntaxError('Syntax Error in: ' + query)
        # cols = cols.strip('()').split(',')
        # for c in cols:
        #     cols.append(c.strip())
    else:
        if ops == -1:
            select_res = globalValue.currentIndex.Select_all_data(
                tableName, isPri, Types)
        else:
            select_res = globalValue.currentIndex.Select_and_join(
                tableName, Types, attrs, keys, ifindexs, ops)

        if select_res:
            output = {}
            output['attrs'] = schema['attrs']
            output['select_res'] = select_res
            return output
        else:
            return False


def main():

    clear_all()  # 初始化

    # ------------------------------ 数据库操作验证
    # 创建数据库
    create_db('create database lll')
    create_db('create database yourdb')
    create_db('create database mydb')

    dbs = show_dbs('show databases')    # 查看当前所有数据库
    print("当前拥有数据库：")
    for db in dbs:
        print('\t'+db)

    use_db('use yourdb')
    print(select_db('select database()'))

    drop_db('drop database yourdb')
    dbs = show_dbs('show databases')    # 查看当前所有数据库
    print("当前拥有数据库：")
    for db in dbs:
        print('\t'+db)

    drop_db('drop database mydb')
    dbs = show_dbs('show databases')    # 查看当前所有数据库
    print("当前拥有数据库：")
    for db in dbs:
        print('\t'+db)

    # -----------------------------表操作验证
    # 创建表
    create_table(
        'create table gogo1(id int, stuName char(20), gender char(1), seat int)')   # 不指定主键，自动设置第一个属性为主键
    create_table(
        'create table gogo2(id int, stuName char(20), gender char(1), seat int,primary key (id))')  # 指定主键
    create_table(
        'create table gogo3(stuName char(30), cno int, score float,  primary key(cno))')    # 指定主键，且不为第一个
    create_table(
        'create table person(height float unique, pid int, name char(32), identity char(128) unique, age int unique, primary key(pid))')
    
    tables = show_tables('show tables')   #
    print('当前数据库有如下表：')
    for table in tables:
        print('\t'+table)

    # 删除只有主键的表
    drop_table('drop table gogo3')
    tables = show_tables('show tables')   #
    print('当前数据库有如下表：')
    for table in tables:
        print('\t'+table)

    # 创建索引
    create_index('create index stuName_index on gogo1(stuName)')    # 引创建
    create_index('create index gender_index on gogo1(gender)')  # 引创建
    create_index('create index seat_index on gogo2(seat)')
    create_index('create index g_index on gogo2(gender)')

    insert('insert into person values (171.1, 1, "Person1", "000001", 81)')
    insert('insert into person values (162.1, 2, "Person2", "000002", 19)')
    insert('insert into person values (163.3, 3, "Person3", "000003", 20)')
    insert('insert into person values (174.9, 4, "Person4", "000004", 21)')
    insert('insert into person values (175.0, 5, "Person5", "000005", 22)')
    insert('insert into person values (176.1, 6, "Person6", "000006", 23)')
    insert('insert into person values (177.2, 7, "Person7", "000007", 24)')
    insert('insert into person values (178.1, 8, "Person8", "000008", 25)')
    insert('insert into person values (179.1, 9, "Person9", "000009", 26)')
    insert('insert into person values (180.1, 10, "Person10", "000010", 27)')
    insert('insert into person values (181.1, 11, "Person11", "000011", 28)')
    insert('insert into person values (182.1, 12, "Person12", "000012", 29)')
    insert('insert into person values (183.1, 13, "Person13", "000013", 30)')
    insert('insert into person values (184.3, 14, "Person14", "000014", 31)')
    insert('insert into person values (185.1, 15, "Person15", "000015", 32)')
    insert('insert into person values (186.4, 16, "Person16", "000016", 33)')
    insert('insert into person values (187.1, 17, "Person17", "000017", 34)')
    insert('insert into person values (188.1, 18, "Person18", "000018", 35)')
    insert('insert into person values (189.1, 19, "Person19", "000019", 36)')
    insert('insert into person values (190.1, 20, "Person20", "000020", 37)')

    create_index('create index idx_height on person(height)')
    create_index('create index idx_identity on person(identity)')
    create_index('create index idx_age on person(age)')

    getIndexInfo()

    select_res = select('select * from person where age > 24')
    if select_res['select_res'][0]:
        temp = select_res['select_res'][1]
        table_student = PrettyTable(select_res['attrs'])
        for row in temp:
            table_student.add_row(row)
        print(table_student)
    else:
        print('Not Found')
    select_res = select('select * from person where identity = "Person15"')
    if select_res['select_res'][0]:
        temp = select_res['select_res'][1]
        table_student = PrettyTable(select_res['attrs'])
        for row in temp:
            table_student.add_row(row)
        print(table_student)
    else:
        print('Not Found')

    select('select * from person where height <= 176.3')
    if select_res['select_res'][0]:
        temp = select_res['select_res'][1]
        table_student = PrettyTable(select_res['attrs'])
        for row in temp:
            table_student.add_row(row)
        print(table_student)
    else:
        print('Not Found')
    # 删除主键
    # drop_index('drop index priKey_gogo1_id')
    getIndexInfo()

    # 删除索引
    drop_index('drop index seat_index')
    getIndexInfo()

    # 删除有两个以上索引的表
    drop_table('drop table gogo2')
    tables = show_tables('show tables')   #
    print('当前数据库有如下表：')
    for table in tables:
        print('\t'+table)

    # 查找空表，不含 where 语句
    # select_res = (select('select * from gogo1'))
    #     temp = select_res['select_res'][1]
    #     table_student = PrettyTable(select_res['attrs'])
    #     for row in temp:
    #         table_student.add_row(row)
    #     print(table_student)
    # else:
    #     print('Not Found')

    # 插入 1000 条语句
    # for i in range(1, 1000):
    #     insert('insert into gogo1 values('+str(i)+',zyq,G,'+str(i+31)+')')

    # select_res = select('select * from gogo1 where id < 10')
    # if select_res['select_res'][0]:
    #     temp = select_res['select_res'][1]
    #     table_student = PrettyTable(select_res['attrs'])
    #     for row in temp:
    #         table_student.add_row(row)
    #     print(table_student)
    # else:
    #     print('Not Found')

    # 删除语句含有 where，单条
    # delete('delete from gogo1 where id = 1')
    # select_res = select('select * from gogo1 where id < 10')
    # if select_res['select_res'][0]:
    #     temp = select_res['select_res'][1]
    #     table_student = PrettyTable(select_res['attrs'])
    #     for row in temp:
    #         table_student.add_row(row)
    #     print(table_student)
    # else:
    #     print('Not Found')

    # 删除语句含有 where，多条，大量
    # delete('delete from gogo1 where id > 90')

    # 查询语句含有 where，仅一返回结果
    # select_res = select('select * from gogo1 where id >= 90')
    # if select_res['select_res'][0]:
    #     temp = select_res['select_res'][1]
    #     table_student = PrettyTable(select_res['attrs'])
    #     for row in temp:
    #         table_student.add_row(row)
    #     print(table_student)
    # else:
    #     print('Not Found')

    # 删除语句含有 where，多条，含有 and
    # delete('delete from gogo1 where id < 80 and id > 30 and id <> 55')

    # 查询语句不含where，全部输出
    # select_res = select('select * from gogo1')
    # if select_res['select_res'][0]:
    #     temp = select_res['select_res'][1]
    #     table_student = PrettyTable(select_res['attrs'])
    #     for row in temp:
    #         table_student.add_row(row)
    #     print(table_student)
    # else:
    #     print('Not Found')

    # 删除语句不含 where，表内数据清空
    # delete('delete from gogo1')

    # 查询空表，不含 where
    # select_res = select('select * from gogo1')
    # if select_res['select_res'][0]:
    #     temp = select_res['select_res'][1]
    #     table_student = PrettyTable(select_res['attrs'])
    #     for row in temp:
    #         table_student.add_row(row)
    #     print(table_student)
    # else:
    #     print('Not Found')

    # 查询空表，含有 where
    # # print(select('select * from gogo1 where id <> 319'))
    # globalValue.currentIndex.Save_file()


if __name__ == '__main__':
    main()
