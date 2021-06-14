#!/user/bin/env python
# -*- coding:utf-8 -*-
# module: FileOp.py
# create by 郑雨琪 on 2021/6/8

import time
import json
import os

DBFiles = 'DBFiles/{}.json'  # 表路径
index_File = 'index/{}_index.json'  # 索引路径
index_filepath = 'IIndex/{}_index.json' 
list_filepath = 'IIndex/{}_list.json'
data_filepath = 'IIndex/{}_data.txt'
log_file = 'log.txt'  # log路径
localtime = time.asctime(time.localtime(time.time()))  # 当前时间

# 异常类型


class MiniSQLError(Exception):
    pass


class MiniSQLSyntaxError(MiniSQLError):
    pass

# log 信息保存


def log(msg):
    print(msg)
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(localtime+': '+msg+'\n')

# json格式导入


def load(path):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            if os.path.getsize(path):
                data = json.load(f)
            else:
                data = {}
            return data
    except EOFError:
        return []

# json格式保存


def store(obj, path):
    # print(obj)
    with open(path, 'w', encoding='utf-8') as fw:
        json.dump(obj, fw)


def main():
    path = "DBFiles\myDB.json"
    schemas = load(path)
    print(schemas)


if __name__ == "__main__":
    main()
