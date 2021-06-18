from index import Index
from prettytable import PrettyTable

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
    select_column = ['ID', 'name']
    attribute = [-1, 20, 20, 20, 20, 0]
    primary_key = 'ID'
    index_list = [True, False, False, False, False, False]

    index.Create_table(table_name, primary_key, column)

    # values_1 = [1, 'ytm', 'zhejiang', '2019', 'chengang', 9.9999]
    # values_2 = [2, 'zyq', 'university', '2019', 'ChenGang', 10.001]
    # values_3 = [3, 'lwb', 'zhe university', '2019-09-01', 'GangChen', 6.66]
    # values_0 = [0, 'test', 'test', 'test', 'test', 1.010101]
    # index.Insert_into_table(table_name, column, attribute, values_1, index_list)
    # index.Insert_into_table(table_name, column, attribute, values_2, index_list)
    # index.Insert_into_table(table_name, column, attribute, values_3, index_list)
    # for i in range(4, 30):
    #     values_0[0] = i
    #     index.Insert_into_table(table_name, column, attribute, values_0, index_list)
    
    # index.Drop_table(table_name, primary_key)

    # delete_res = index.Drop_field_from_table(table_name, column, primary_key, 57, True, index_list)
    # if not delete_res:
    #     print('You can\'t delete for the key doesn\'t exist')
    # else:
    #     print('Delete Successfully!')

    # values_0 = values_1
    # values_0[0] = 60
    # update_res = index.Update_field_from_table(table_name, column, primary_key, 60, True, attribute, values_0, index_list)
    
    # column_list = ['ID','name','ID']
    # key_list = [20, 'test', 0]
    # select_index_list = [True, False, True]
    # condition_list = [2, 0, 5]
    # delete = index.Delete_and_join(table_name,column,attribute,index_list,column_list,key_list,select_index_list,condition_list)
    # # select_res = index.Select_and_join(table_name,attribute,column_list,key_list,select_index_list,condition_list)
    # select_res = index.Select_all_data(table_name,primary_key,attribute)
    # if select_res[0]:
    #     temp = select_res[1]
    #     table_student = PrettyTable(column)
    #     for row in temp:
    #         table_student.add_row(row)
    #     print(table_student)
    # else:
    #     print('Not Found')

    # select_res = index.Select_from_table(table_name, 'name', 'ytm', attribute, False)
    # if select_res[0]:
    #     print(select_res[1])
    # else:
    #     print('Not Found')

    index.Save_file()