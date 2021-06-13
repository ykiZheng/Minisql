from BPlusTree import BPlusTree
from NormalList import NormalList
from buffer import Buffer
import json

class Index():
    def __init__(self):
        self.index_trees = {}
        self.normal_list = {}
        self.buffer = Buffer()

    def Load_file(self, index_filepath, list_filepath, data_filepath):
        self.index_filepath = index_filepath
        self.list_filepath = list_filepath
        with open(index_filepath,'r') as Ifp:
            self.index_trees = json.load(Ifp)
            Ifp.close()
        with open(list_filepath,'r') as Lfp:
            self.normal_list = json.load(Lfp)
            Lfp.close()
        self.buffer.Load_data_file(data_filepath)
    
    def Save_file(self):
        with open(self.index_filepath,'w') as Ifp:
            Ifp.write(json.dumps(self.index_trees))
            Ifp.close()
        with open(self.list_filepath, 'w') as Lfp:
            Lfp.write(json.dumps(self.normal_list))
            Lfp.close()
        self.buffer.Buffer_close_save()

    def Create_table(self, table_name, primary_key, column):
        BT = BPlusTree()
        BT.BuildNewBPTree()
        self.index_trees[table_name] = {}
        self.index_trees[table_name][primary_key] = BT.Trees
        self.normal_list[table_name] = {}
        for item in column:
            if item == primary_key:
                continue
            NL = NormalList()
            self.normal_list[table_name][item] = {}
            self.normal_list[table_name][item]['keys'] = NL.keys
            self.normal_list[table_name][item]['values'] = NL.values

    def Drop_table(self, table_name, primary_key):
        BT = BPlusTree()
        BT.Trees = self.index_trees[table_name][primary_key]
        BT.Fetch_all_nodes(self.Trees)
        all_data_offset = BT.all_values
        for it in all_data_offset:
            self.buffer.Delete_data(it)
        self.Drop_field_from_table
        self.index_trees.pop(table_name)
        self.normal_list.pop(table_name)

    def Create_index(self, table_name, column_name):
        BT = BPlusTree()
        BT.BuildNewBPTree()
        keys = self.normal_list[table_name][column_name]['keys']
        values = self.normal_list[table_name][column_name]['values']
        for i in range(len(keys)):
            BT.Insert_node(keys[i], values[i])
        self.index_trees[table_name][column_name] = BT.Trees
        self.normal_list[table_name].pop(column_name)

    def Delete_index(self, table_name, column_name):
        pass

    def Select_from_table(self, table_name, column_name, key, attribute, isindex):
        if isindex:
            BT = BPlusTree()
            BT.Trees = self.index_trees[table_name][column_name]
            res = BT.Search_key(key)
            if res[0]:
                offset = res[1]
                data_res = self.buffer.Search_data(offset, attribute)
                res[1] = data_res
            return res
        else:
            NL = NormalList()
            NL.Load_list(self.normal_list[table_name][column_name])
            res = NL.Search_key(key)
            if res[0]:
                offset = res[1]
                data_res = self.buffer.Search_data(offset, attribute)
                res[1] = data_res
            return res

    def Insert_into_table(self, table_name, column, attribute, values, index_list):
        offset = self.buffer.Insert_data(values, attribute)
        for i in range(len(index_list)):
            if index_list[i] == True:
                BT = BPlusTree()
                BT.Trees = self.index_trees[table_name][column[i]]
                BT.Insert_node(values[i], offset)
            else:
                NL = NormalList()
                NL.Load_list(self.normal_list[table_name][column[i]])
                NL.Insert_node(values[i], offset)

    def Drop_field_from_table(self, table_name, column, column_name, key, isindex, index_list):
        if isindex:
            BT = BPlusTree()
            BT.Trees = self.index_trees[table_name][column_name]
            res = BT.Search_key(key)
            if res[0]:
                offset = res[1]
                BT.Delete_key(key)
                return True
            else:
                return False
        else:
            NL = NormalList()
            NL.Load_list(self.normal_list[table_name][column_name])
            res = NL.Search_key(key)
            if res[0]:
                offset = res[1]
                NL.Delete_key(key)
            else:
                return False
        field_values = self.buffer.Search_data(offset)
        self.buffer.Delete_data(offset)
        for i in range(len(index_list)):
            if column[i] != column_name:
                if index_list[i]:
                    BT = BPlusTree()
                    BT.Trees = self.index_trees[table_name][column[i]]
                    BT.Delete_key(field_values[i])
                else:
                    NL = NormalList()
                    NL.Load_list(self.normal_list[table_name][column[i]])
                    NL.Delete_key(key)


    def Update_from_table(self, table_name, column, key, attribute):
        pass
