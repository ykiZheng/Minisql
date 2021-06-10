# FileOp接口说明
没有buffer的权宜之策
```python
def log(msg)    #存 log 信息
def load(path)  #取该文件结构
def store(data, path) #存

DBFiles # 表路径
index_File  # 索引路径
# 可以使用形如 path = DBFiles.format(dbName)得到表或索引文件相对路径, 如 path = DBFiles.format('yourDB')

参数说明：
msg             --log输出信息
path            --文件路径，形如'\DBFiles.myDB'
```


# json文件结构说明：
总体结构如下：
```
--DBFiles
    --yourDB.json
    --mingming.json
--Index
    --yourDB_index.json
    --mingming_index.json
--log.txt
```
其中 DBFiles 内每个文件名为数据库名，每个json文件保存该数据库的表结构。

## json保存表结构
表结构如下：
```json
{
    tableName: {
        "attrs": [],
        "types": [],
        "primary_key": "",
        "uniques": [],
        "index": []
    },
    tableName: {
        "attrs": [],
        "types": [],
        "primary_key": "",
        "uniques": [],
        "index": []
    }
}
```
注意 types 使用数字替代实际int、float、char(n)
| types | actual_type |
| ----- | ----------- |
| -1    | float       |
| 0     | int         |
| 1~255 | char(n)     |

可以直接查看yourDB.json文件熟悉

## json保存index信息
结构如下：
```json
{
    indexName: {
        "table": "",
        "attri": ""
    }
}
```

可以直接查看 yourDB_index.json 文件熟悉

# log说明
没啥好说的，直接传入 message 就行了
既在屏幕上打印，又保存在 log.txt 里