import sys
import json
import time
import math as mt
import copy as cp

sys.setrecursionlimit(1000000)

class BPlusTree():
    def __init__(self):
        self.Trees = {}
        self.degree = 4
        self.fetch_nodes = []

    def GetFileBTree(self, file_path):
        with open(file_path, 'r') as fp:
            self.Trees = json.load(fp)

    def BuildNewBPTree(self):
        self.Trees['leaf'] = True
        self.Trees['num'] = 0
        self.Trees['keys'] = []
        self.Trees['values'] = []

    def SaveBPTree(self, file_path):
        with open(file_path, 'w') as fp:
            fp.write(json.dumps(self.Trees, indent=2))

    #插入
    def Insert_node(self, key, value):
        current_node = self.Trees
        search_node_list = []
        search_index_list = []
        #查找
        while current_node['leaf'] == False:
            child_index = 0
            search_node_list.append(current_node)
            while True:
                left_child_key = current_node['keys'][child_index]
                right_child_key = current_node['keys'][child_index+1]
                if key >= left_child_key and key < right_child_key:
                    break 
                child_index = child_index + 1
                if child_index == current_node['num'] - 1:
                    break
            search_index_list.append(child_index)
            current_node = current_node['children'][child_index]
        #插入
        insert_index = 0
        for item_key in current_node['keys']:
            if key < item_key:
                break
            insert_index = insert_index + 1
        current_node['num'] = current_node['num'] + 1
        current_node['keys'].insert(insert_index, key)
        current_node['values'].insert(insert_index, value)
        #如果不满足B+树，由下而上更新
        if current_node['num'] == self.degree + 1:
            #叶子节点分裂
            right_split_node = {}
            right_split_node['leaf'] = True
            right_split_node['num'] = mt.ceil(current_node['num']/2)
            right_split_node['keys'] = []
            right_split_node['values'] = []
            for i in range(right_split_node['num']):
                right_split_node['keys'].append(current_node['keys'].pop())
                right_split_node['values'].append(current_node['values'].pop())
            right_split_node['keys'].reverse()
            right_split_node['values'].reverse()
            floating_key = right_split_node['keys'][0]
            current_node['num'] = mt.floor(current_node['num']/2)
            #叶子父节点更新内容
            if len(search_node_list) != 0:
                father_node = search_node_list.pop()
                father_node['num'] = father_node['num'] + 1
                new_node_index = search_index_list.pop()
                father_node['children'].insert(new_node_index+1, right_split_node)
                father_node['keys'].insert(new_node_index+1, floating_key) 
            else:
                father_node = {}
                father_node['leaf'] = False
                father_node['num'] = 2
                father_node['keys'] = [current_node['keys'][0], floating_key]
                father_node['children'] = [current_node, right_split_node]
            current_node = father_node
            #普通父节点递归更新
            while current_node['num'] == self.degree:
                #子节点分裂新节点
                right_split_node = {}
                right_split_node['leaf'] = False
                right_split_node['num'] = mt.ceil(current_node['num']/2)
                right_split_node['keys'] = []
                right_split_node['children'] = []
                #分裂
                for i in range(right_split_node['num']):
                    right_split_node['keys'].append(current_node['keys'].pop())
                    right_split_node['children'].append(current_node['children'].pop())
                right_split_node['keys'].reverse()
                right_split_node['children'].reverse()
                floating_key = right_split_node['keys'][0]
                current_node['num'] = mt.floor(current_node['num']/2)
                #父节点更新
                if len(search_node_list) != 0:
                    father_node = search_node_list.pop()
                    father_node['num'] = father_node['num'] + 1
                    new_node_index = search_index_list.pop()
                    father_node['children'].insert(new_node_index+1, right_split_node)
                    father_node['keys'].insert(new_node_index+1, floating_key)
                    current_node = father_node
                else:
                    father_node = {}
                    father_node['leaf'] = False
                    father_node['num'] = 2
                    father_node['keys'] = [current_node['keys'][0], floating_key]
                    father_node['children'] = [current_node, right_split_node]
                    current_node = father_node
                    break
            # while len(search_node_list):
            #     rest_father_node = search_node_list.pop()
            #     rest_child_index = search_index_list.pop()
            #     rest_father_node['children'][rest_child_index] = current_node
            #     current_node = rest_father_node
            if not len(search_node_list):
                self.Trees = current_node  

    #条件搜索  
    def Condition_search_node(self, key, condition):
        res = [False]
        if condition == 0: #等于
            temp = self.Search_key(key)
            if temp[0]:
                res.append([temp[1]])
        else: 
            self.fetch_nodes = []
            self.Fetch_nodes_condition(self.Trees, key, condition)
            if self.fetch_nodes:
                res.append(self.fetch_nodes)
        return res

    #条件删除
    def Condition_delete_node(self, key, condition):
        res = self.Condition_search_node(key, condition)
        if res[0]:
            delete_nodes = res[1]
            for delete_key in delete_nodes:
                self.Delete_key(delete_key)
            return True
        else:
            return False

    #搜索
    def Search_key(self, key):
        res = [False]
        current_node = self.Trees
        while current_node['leaf'] == False:
            child_index = 0
            while True:
                left_child_key = current_node['keys'][child_index]
                right_child_key = current_node['keys'][child_index+1]
                if key >= left_child_key and key < right_child_key:
                    break
                child_index = child_index + 1
                if child_index == current_node['num'] - 1:
                    break
            current_node = current_node['children'][child_index]
        #到达叶子结点
        value_index = 0
        for item_key in current_node['keys']:
            if item_key == key:
                res[0] = True
                res.append(current_node['values'][value_index])
            value_index = value_index + 1
        #返回
        return res

    #删除
    def Delete_key(self, key):
        current_node = self.Trees
        search_node_list = []
        search_index_list = []
        #查找
        while current_node['leaf'] == False:
            child_index = 0
            search_node_list.append(current_node)
            while True:
                left_child_key = current_node['keys'][child_index]
                right_child_key = current_node['keys'][child_index+1]
                if key >= left_child_key and key < right_child_key:
                    break 
                child_index = child_index + 1
                if child_index == current_node['num'] - 1:
                    break
            search_index_list.append(child_index)
            current_node = current_node['children'][child_index]
        #查找删除结点
        delete_index = 0
        for leaf_key in current_node['keys']:
            if key == leaf_key:
                break
            delete_index = delete_index + 1
        #删除
        if delete_index == current_node['num']:
            return False
        if current_node['num'] > mt.floor(self.degree/2) or not search_index_list:
            current_node['num'] = current_node['num'] - 1
            current_node['keys'].pop(delete_index)
            current_node['values'].pop(delete_index)
            if not delete_index and search_index_list:
                self.Recursion_update(current_node, search_node_list, search_index_list)
        else:
            search_node_list.append(current_node)
            search_index_list.append(delete_index)
            self.Borrow_delete_update(search_node_list, search_index_list, True)
    #删除时候递归更新
    def Recursion_update(self, current_node, node_list, index_list):
        update_key = current_node['keys'][0]
        if not node_list:
            return
        father_node = node_list.pop()
        update_index = index_list.pop()
        while update_index == 0:
            if not index_list:
                break
            father_node['keys'][update_index] = update_key
            update_index = index_list.pop()
            father_node = node_list.pop()
        father_node['keys'][update_index] = update_key
        pass
    #删除时候更新父节点
    def Update_delete_father(self, father_list, index_list):
        current_node = father_list[-1]
        remove_index = index_list[-1]
        if current_node['num'] > mt.floor(self.degree):
            current_node['num'] = current_node['num'] - 1
            current_node['keys'].pop(remove_index)
            current_node['children'].pop(remove_index)
        else:
            if len(father_list) == 1:
                if current_node['num'] > 2:
                    current_node['num'] = current_node['num'] - 1
                    current_node['keys'].pop(remove_index)
                    current_node['children'].pop(remove_index)
                else:
                    self.Trees = current_node['children'][1-remove_index]
            else:
                self.Borrow_delete_update(father_list, index_list, False)
    #删除从兄弟节点借元素的情况
    def Borrow_delete_update(self, search_node_list, search_index_list, isleaf):
        current_node = search_node_list.pop()
        delete_index = search_index_list.pop()
        current_node['num'] = current_node['num'] - 1
        current_node['keys'].pop(delete_index)
        if isleaf:
            current_node['values'].pop(delete_index)
        else:
            current_node['children'].pop(delete_index)
        #向兄弟节点借元素
        father_node = search_node_list[-1]
        borrow_index = search_index_list[-1] - 1
        #左借
        if borrow_index >= 0:
            if father_node['children'][borrow_index]['num'] > mt.floor(self.degree/2):
                slibing_node = father_node['children'][borrow_index]
                slibing_node['num'] = slibing_node['num'] - 1
                current_node['num'] = current_node['num'] + 1
                current_node['keys'].insert(0, slibing_node['keys'].pop())
                if isleaf:
                    current_node['values'].insert(0, slibing_node['values'].pop())
                else:
                    current_node['children'].insert(0, slibing_node['children'].pop())
                self.Recursion_update(current_node, search_node_list, search_index_list)
            #合并
            else:
                slibing_node = father_node['children'][borrow_index]
                slibing_node['num'] = slibing_node['num'] + current_node['num']
                for i in range(current_node['num']):
                    slibing_node['keys'].append(current_node['keys'][i])
                    if isleaf:
                        slibing_node['values'].append(current_node['values'][i])
                    else:
                        slibing_node['children'].append(current_node['children'][i])
                self.Update_delete_father(search_node_list, search_index_list)
        #右借
        else:
            borrow_index = borrow_index + 2
            if father_node['children'][borrow_index]['num'] > mt.floor(self.degree/2):
                slibing_node = father_node['children'][borrow_index]
                slibing_node['num'] = slibing_node['num'] - 1
                current_node['num'] = current_node['num'] + 1
                current_node['keys'].append(slibing_node['keys'][0])
                slibing_node['keys'].pop(0)
                if isleaf:
                    current_node['values'].append(slibing_node['values'][0])
                    slibing_node['values'].pop(0)
                else:
                    current_node['children'].append(slibing_node['children'][0])
                    slibing_node['children'].pop(0)
                update_key = slibing_node['keys'][0]
                update_index = search_index_list[-1] + 1
                father_node['keys'][update_index] = update_key
                if not delete_index:
                    self.Recursion_update(current_node, search_node_list, search_index_list)
                return True
            #合并节点
            else:
                slibing_node = father_node['children'][borrow_index]
                current_node['num'] = current_node['num'] + slibing_node['num']
                for i in range(slibing_node['num']):
                    current_node['keys'].append(slibing_node['keys'][i])
                    if isleaf:
                        current_node['values'].append(slibing_node['values'][i])
                    else:
                        current_node['children'].append(slibing_node['children'][i])
                if not delete_index:
                    node_list = cp.copy(search_node_list)
                    index_list = cp.copy(search_index_list)
                    self.Recursion_update(current_node, node_list, index_list)
                search_index_list[-1] = search_index_list[-1] + 1
                self.Update_delete_father(search_node_list, search_index_list)

        #获取树的所有节点

    #条件遍历元素
    def Fetch_nodes_condition(self, node, key, condition):
        if not node:
            return
        if node['leaf']:
            for i in range(node['num']):
                if condition == -1:
                    self.fetch_nodes.append(node['values'][i])
                elif condition == 1:
                    if node['keys'][i] != key:
                        self.fetch_nodes.append(node['values'][i])
                elif condition == 2:
                    if node['keys'][i] < key:
                        self.fetch_nodes.append(node['values'][i])
                elif condition == 3:
                    if node['keys'][i] > key:
                        self.fetch_nodes.append(node['values'][i])
                elif condition == 4:
                    if node['keys'][i] <= key:
                        self.fetch_nodes.append(node['values'][i])
                elif condition == 5:
                    if node['keys'][i] >= key:
                        self.fetch_nodes.append(node['values'][i])
        else:
            for child in node['children']:
                self.Fetch_nodes_condition(child, key, condition)
            
    def Fetch_all_nodes(self):
        self.fetch_nodes = []
        self.Fetch_nodes_condition(self.Trees, 0, -1)
        return self.fetch_nodes


# def search(key, condition):
#     t1 = time.perf_counter()            
#     result = BT.Condition_search_node(key, condition)
#     t2 = time.perf_counter()
#     if result[0]:
#         print('Find key-value:',result[1])
#     else:
#         print('Not Found')
#     print('Running time:',1000*(t2-t1),'ms')

# if __name__ == '__main__':
#     global BT
#     BT = BPlusTree()
#     #BT.GetFileBTree()
#     BT.BuildNewBPTree()
#     # for i in range(10):
#     #     BT.Insert_node(i, i)
#     for i in range(20):
#         BT.Insert_node(i, i)
    
    # search(12, 0)
    # search(12, 1)
    # search(12, 2)
    # search(12, 3)
    # search(12, 4)
    # search(12, 5)

    # BT.Condition_delete_node(11, 0)
    # BT.Condition_delete_node(11, 1)
    # BT.Condition_delete_node(11, 2)
    # BT.Condition_delete_node(11, 3)
    # BT.Condition_delete_node(11, 4)
    # BT.Condition_delete_node(11, 5)

    # res = BT.Fetch_all_nodes()
    # print(res)

    # BT.SaveBPTree('BT.json')