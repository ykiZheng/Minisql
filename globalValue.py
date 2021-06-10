#!/user/bin/env python
# -*- coding:utf-8 -*-
# module: globalValue.py
# create by 郑雨琪 on 2021/6/9


# currentDB在不同文件中读取，共享同一空间
# 由于 from globalValue import * 只会取值并在该文件中创建临时变量而无法改变当前值
# 为了方便单独搞了一个文件
# import globalValue 即可使用
currentDB = ""