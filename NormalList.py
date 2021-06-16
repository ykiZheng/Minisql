
class NormalList():
    def __init__(self):
        self.keys = []
        self.values = []

    def Load_list(self, list):
        self.keys = list['keys']
        self.values = list['values']

    def Condition_search_node(self, key, condition):
        res = [False]
        temp = []
        for i in range(0,len(self.keys)):
            if condition == 0:
                if self.keys[i] == key:
                    temp.append(self.values[i])
            elif condition == 1:
                if self.keys[i] != key:
                    temp.append(self.values[i])
            elif condition == 2:
                if self.keys[i] < key:
                    temp.append(self.values[i])
            elif condition == 3:
                if self.keys[i] > key:
                    temp.append(self.values[i])
            elif condition == 4:
                if self.keys[i] <= key:
                    temp.append(self.values[i])
            elif condition == 5:
                if self.keys[i] >= key:
                    temp.append(self.values[i])
        if temp:
            res[0] = True
            res.append(temp)
        return res

    def Insert_node(self, key, value):
        self.keys.append(key)
        self.values.append(value)

    def Condition_delete_node(self, key, condition):
        flag = False
        for i in range(len(self.keys)):
            if condition == 0:
                if self.keys[i] == key:
                    self.keys.pop(i)
                    self.values.pop(i)
                    flag = True
            elif condition == 1:
                if self.keys[i] != key:
                    self.keys.pop(i)
                    self.values.pop(i)
                    flag = True
            elif condition == 2:
                if self.keys[i] < key:
                    self.keys.pop(i)
                    self.values.pop(i)
                    flag = True
            elif condition == 3:
                if self.keys[i] > key:
                    self.keys.pop(i)
                    self.values.pop(i)
                    flag = True
            elif condition == 4:
                if self.keys[i] <= key:
                    self.keys.pop(i)
                    self.values.pop(i)
                    flag = True
            elif condition == 5:
                if self.keys[i] >= key:
                    self.keys.pop(i)
                    self.values.pop(i)
                    flag = True
        return flag
    
    def Delete_key(self, key):
        for i in range(len(self.keys)):
            if self.keys[i] == key:
                self.keys.pop(i)
                self.values.pop(i)
                return