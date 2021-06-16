import cmd
from API import *
from CatalogManager import *
import sys
import time
import os
import globalValue
from typing import Tuple
from FileOp import *
from prettytable import PrettyTable as pt

def __initialize__():
    clear_all()
    create_db('create database mydb')
    use_db('use mydb')

def __finalize__():
    globalValue.currentIndex.Save_file()

class miniSQL(cmd.Cmd):
    # __initialize__()
    intro = 'Welcome to the MiniSQL database server.\nCreated by 颜天明 郑雨琪 刘文博 from Zhejiang University.\nType help or ? to list commands.\n'
    def do_show(self,args):
        args='show '+args
        args=args.replace(';','')
        # print(args.split(' ')[1])
        if args.split(' ')[1] == 'databases':
            try:
                time_start = time.time()
                print(show_dbs(args.replace(';','').lower()))
                time_end = time.time()
                print("Time elapsed : %fs." % (time_end - time_start))
            except MiniSQLError as e:
                log(str(e))
        elif args.split(' ')[1] == 'tables':
            try:
                time_start = time.time()
                print(show_tables(args.replace(';','').lower()))
                time_end = time.time()
                print("Time elapsed : %fs." % (time_end - time_start))
            except MiniSQLError as e:
                log(str(e))
    def do_use(self,args):
        args="use "+args
        args=args.replace(';','')
        try:
            time_start = time.time()
            use_db(args.replace(';','').lower())
            time_end = time.time()
            print("Time elapsed : %fs." % (time_end - time_start))
        except MiniSQLError as e:
            log(str(e))
    def do_select(self,args):
        args="select "+args
        args=args.replace(';','')
        if args.split(' ')[1] == 'database':
            try:
                time_start = time.time()
                print(select_db(args.replace(';','').lower()))
                time_end = time.time()
                print("Time elapsed : %fs." % (time_end - time_start))
            except MiniSQLError as e:
                log(str(e))
        else:
            try:
                time_start = time.time()
                # print(args.replace(';','').lower())
                select_res = select(args.replace(';','').lower())
                time_end = time.time()
                if select_res['select_res'][0]:
                    temp=select_res['select_res'][1]
                    table = PrettyTable(select_res['attrs'])
                    i=0
                    for row in temp:
                        i=i+1
                        table.add_row(row)
                    print(table)
                    print(i,end = '')
                    print(' rows in set ',end = '')
                    print("  Time elapsed : %fs." % (time_end - time_start))
                else:
                    table = PrettyTable(select_res['attrs'])
                    print(table)
                    print('Empty set ',end = '')
                    print("Time elapsed : %fs." % (time_end - time_start))
            except MiniSQLError as e:
                log(str(e))
    def do_create(self,args):
        # print(args)
        args="create "+args
        args=args.replace(';','')
        if args.split(' ')[1] == 'database':
            try:
                time_start = time.time()
                create_db(args.replace(';','').lower())
                time_end = time.time()
                print("Time elapsed : %fs." % (time_end - time_start))
            except MiniSQLError as e:
                log(str(e))
        elif args.split(' ')[1] == 'table':
            if args.find('primary')!=-1:
                s=args
                x=s.find('key')
                if s[x+3]!='(':
                    tmp=s.replace("("," ")
                    tmp=tmp.replace(')',' ')
                    s2=tmp.split(' ')
                    for i in range(len(s2)):
                        if s2[i] == 'primary':
                            break
                    s3=s2[i-2].strip()
                    a=s.find('primary')
                    s1=s[a:a+11]
                    s=s.replace('primary key','')
                    s=s[0:len(s)-1]
                    s=s+','+s1+'('+s3+'))'
                args=s  #处理primary key位置
            # print(args)
            try:
                time_start = time.time()
                create_table(args.replace(';','').lower())
                time_end = time.time()
                print("Time elapsed : %fs." % (time_end - time_start))
            except MiniSQLError as e:
                log(str(e))
        elif args.split(' ')[1] == 'index':
            try:
                time_start = time.time()
                create_index(args.replace(';','').lower())
                time_end = time.time()
                print("Time elapsed : %fs." % (time_end - time_start))
            except MiniSQLError as e:
                log(str(e)) 
    def do_drop(self,args):
        args="drop "+args
        args=args.replace(';','')
        if args.split(' ')[1] == 'database':
            try:
                time_start = time.time()
                drop_db(args.replace(';','').lower())
                time_end = time.time()
                print("Time elapsed : %fs." % (time_end - time_start))
            except MiniSQLError as e:
                log(str(e))
        elif args.split(' ')[1] == 'table':
            try:
                time_start = time.time()
                drop_table(args.replace(';','').lower())
                time_end = time.time()
                print("Time elapsed : %fs." % (time_end - time_start))
            except MiniSQLError as e:
                log(str(e))
        elif args.split(' ')[1] == 'index':
            try:
                time_start = time.time()
                # print(args.replace(';','').lower())
                drop_index(args.replace(';','').lower())
                time_end = time.time()
                print("Time elapsed : %fs." % (time_end - time_start))
            except MiniSQLError as e:
                log(str(e))
    def do_insert(self,args):
        args='insert '+args
        args=args.replace(';','').replace("'",'')
        try:
            time_start = time.time()
            insert(args.replace(';','').lower())
            time_end = time.time()
            print('[Insert table]  插入成功')
            print("Time elapsed : %fs." % (time_end - time_start))
        except MiniSQLError as e:
            log(str(e))

    def do_delete(self,args):
        args='delete '+args
        args=args.replace(';','')
        try:
            time_start = time.time()
            # print(args.replace(';','').lower())
            delete(args.replace(';','').lower())
            time_end = time.time()
            print("Time elapsed : %fs." % (time_end - time_start))
        except MiniSQLError as e:
            log(str(e))
    
    def do_execfile(self,args):
        time_start = time.time()
        args=args.strip().replace(';','')
        exec_from_file(args)
        time_end = time.time()
        print("Running all commands in the file costs %fs." % (time_end - time_start))

    def do_quit(self,args):
        __finalize__()
        print('Goodbye.')
        sys.exit()

    def emptyline(self):
        pass

    def help_show(self):
        print()
        print("Show databases:show databases")

    def help_quit(self):
        print()
        print('Quit the program and write changes to local file.')

    def help_insert(self):
        print()
        print("insert into student values ( 1,'Bob','male','2001.1.1');")

    def help_delete(self):
        print()
        print("delete from students")
        print("delete from student where sno = '88888888';")

    def help_use(self):
        print()
        print("Change a database ")
        print("use 数据库名;")
    
    def help_select(self):
        print()
        print("Select a database to show its information:select database()")
        print("Select tuples from tables")

    def help_create(self):
        print()
        print("Create a database: create database 数据库名;")
        print("Create a new table: CREATE TABLE 表名(字段名 字段类型 PRIMARY KEY, 字段名 字段类型 UNIQUE);")
        print("Create a new index: create index 索引名 on 表名 (字段名，字段名);")

    def help_drop(self):
        print()
        print("Drop a database: drop database 数据库名;")
        print("Drop a table: drop table 表名;")
        print("Drop an index: drop index 索引名;")

    def help_show(self):
        print()
        print('Show all tables in current database:show tables;')
        print('Show all databases: show databases')

    def help_execfile(self):
        print()
        print('Impord orders from a file:execfile 文件名')
    

def exec_from_file(filename):#从文件读取命令
    f = open(filename)
    text = f.read()
    f.close()
    comands = text.split(';')
    comands = [i.strip().replace('\n','') for i in comands]
    # __initialize__()
    for comand in comands:
        comand_=comand.lower()
        if comand_ == '':
            continue
        if comand_[0] == '#':
            continue
        if comand_.split(' ')[0] == 'insert':
            try:
                timestart = time.time()
                insert(comand_.replace("'",""))
                timeend = time.time()
                print("Time elapsed : %fs." % (timeend - timestart))
            except MiniSQLError as e:
                log(str(e))
        elif comand_.split(' ')[0] == 'select':
            if comand_.split(' ')[1] == 'database':
                try:
                    timestart = time.time()
                    select_db(comand_)
                    timeend = time.time()
                    print("Time elapsed : %fs." % (timeend - timestart))
                except MiniSQLError as e:
                    log(str(e))
            else:        
                try:
                    timestart = time.time()
                    # print(comand_)
                    select_res = select(comand_)
                    timeend = time.time()
                    if select_res['select_res'][0]:
                        temp=select_res['select_res'][1]
                        table = PrettyTable(select_res['attrs'])
                        i=0
                        for row in temp:
                            i=i+1
                            table.add_row(row)
                        print(table)
                        print(i,end = '')
                        print(' rows in set ',end = '')
                        print("  Time elapsed : %fs." % (timeend - timestart))
                    else:
                        table = PrettyTable(select_res['attrs'])
                        print(table)
                        print('Empty set ',end = '')
                        print("Time elapsed : %fs." % (timeend - timestart))
                except MiniSQLError as e:
                    log(str(e))
        elif comand_.split(' ')[0] == 'use':
            try:
                timestart = time.time()
                use_db(comand_)
                timeend = time.time()
                print("Time elapsed : %fs." % (timeend - timestart))
            except MiniSQLError as e:
                log(str(e))
        elif comand_.split(' ')[0] == 'delete':
            try:
                timestart = time.time()
                # print(comand_)
                delete(comand_)
                timeend = time.time()
                print("Time elapsed : %fs." % (timeend - timestart))
            except MiniSQLError as e:
                log(str(e))
        elif comand_.split(' ')[0] == 'drop':
            if comand_.split(' ')[1] == 'table':
                try:
                    timestart = time.time()
                    drop_table(comand_)
                    timeend = time.time()
                    print("Time elapsed : %fs." % (timeend - timestart))
                except MiniSQLError as e:
                    log(str(e))
            elif comand_.split(' ')[1] == 'index':
                try:
                    timestart = time.time()
                    # print(comand_)
                    drop_index(comand_)
                    timeend = time.time()
                    print("Time elapsed : %fs." % (timeend - timestart))
                except MiniSQLError as e:
                    log(str(e))
            elif comand_.split(' ')[1] == 'database':
                try:
                    timestart = time.time()
                    drop_db(comand_)
                    timeend = time.time()
                    print("Time elapsed : %fs." % (timeend - timestart))
                except MiniSQLError as e:
                    log(str(e))
        elif comand_.split(' ')[0] == 'create':
            if comand_.split(' ')[1] == 'table':
                if comand_.find('primary')!=-1:
                    s=comand_
                    x=s.find('key')
                    if s[x+3]!='(':
                        tmp=s.replace("(","")
                        tmp=tmp.replace(')','')
                        s2=tmp.split(' ')
                        for i in range(len(s2)):
                            if s2[i] == 'primary':
                                break
                        s3=s2[i-2].strip()
                        a=s.find('primary')
                        s1=s[a:a+11]
                        s=s.replace('primary key','')
                        s=s[0:len(s)-1]
                        s=s+','+s1+'('+s3+'))'
                    comand_=s  #处理primary key位置
                # print(comand_)
                try:
                    timestart = time.time()
                    create_table(comand_)
                    timeend = time.time()
                    print("Time elapsed : %fs." % (timeend - timestart))
                except MiniSQLError as e:
                    log(str(e))
            elif comand_.split(' ')[1] == 'index':
                try:
                    timestart = time.time()
                    create_index(comand_)
                    timeend = time.time()
                    print("Time elapsed : %fs." % (timeend - timestart))
                except MiniSQLError as e:
                    log(str(e))
            elif comand_.split(' ')[1] == 'database':
                try:
                    timestart = time.time()
                    create_db(comand_)
                    timeend = time.time()
                    print("Time elapsed : %fs." % (timeend - timestart))
                except MiniSQLError as e:
                    log(str(e))


if __name__ == '__main__':
    errortext = '''
MiniSQL -u [username] -p [password] 
\tLogin operators : 
\t\t-u username\tusername for MiniSQL.
\t\t-p password\tpassword for MiniSQL.\n
'''
    if len(sys.argv) < 5:
        print('ERROR : Unsupported syntax, please login.\n',errortext)
        sys.exit()
    if sys.argv[1] != '-u' or sys.argv[3] != '-p':
        print('ERROR : Unsupported syntax, please login.\n',errortext)
        sys.exit()
    login=0
    if sys.argv[2] == 'root' and sys.argv[4] == '123456' or sys.argv[2] == 'Ytm' and sys.argv[4] == '654321' or sys.argv[2] == 'Zyq' and sys.argv[4] == '112233'or sys.argv[2] == 'Lwb' and sys.argv[4] == '445566':
        login=1
    else:
        print('Error: ID or password is not correct, please check again\n',errortext)
        sys.exit()
    
    if len(sys.argv) > 5:
        if sys.argv[5] != '-execfile':
            print('ERROR : Unsupported syntax.\n',errortext)
        exec_from_file(sys.argv[6])
        sys.exit()

    miniSQL.prompt = '(%s)' % sys.argv[2] + 'MiniSQL > '
    miniSQL().cmdloop()
