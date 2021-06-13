from index import Index

def GetAttribute(data):
    res = []
    for item in data:
        if isinstance(item, int):
            res.append(-1)
        elif isinstance(item, float):
            res.append(0)
        else:
            res.append(len(item))
    return res

if __name__ == '__main__':
    index = Index()
    index_filepath = 'dbfiles_ytm/index.json'
    list_filepath = 'dbfiles_ytm/list.json'
    data_filepath = 'dbfiles_ytm/data.txt'
    index.Load_file(index_filepath, list_filepath, data_filepath)

    table_name = 'student'
    column = ['ID', 'name', 'school', 'year', 'teacher', 'price']
    attribute = [-1, 20, 20, 20, 10, 0]
    primary_key = 'ID'
    index_list = [True, False, False, False, False, False]

    # index.Create_table(table_name, primary_key, column)

    # values_1 = [1, 'ytm', 'zhejiang', '2019', 'chengang', 9.9999]
    # values_2 = [2, 'zyq', 'university', '2019', 'ChenGang', 10.001]
    # values_3 = [3, 'lwb', 'zhe university', '2019-09-01', 'GangChen', 6.66]

    # index.Insert_into_table(table_name, column, attribute, values_1, index_list)
    # index.Insert_into_table(table_name, column, attribute, values_2, index_list)
    # index.Insert_into_table(table_name, column, attribute, values_3, index_list)

    #index.Drop_field_from_table(table_name, column, primary_key, 1, True, index_list)

    res = index.Select_from_table(table_name, primary_key, 1, attribute, True)
    if res[0]:
        print(res[1])
    else:
        print('Not Found')

    index.Save_file()