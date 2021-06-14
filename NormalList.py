
class NormalList():
    def __init__(self):
        self.keys = []
        self.values = []

    def Load_list(self, list):
        self.keys = list['keys']
        self.values = list['values']

    def Search_key(self, key):
        res = [False]
        for i in range(len(self.keys)):
            if key == self.keys[i]:
                res[0] = True
                res.append(self.values[i])
                break
        return res

    def Insert_node(self, key, value):
        self.keys.append(key)
        self.values.append(value)

    def Delete_key(self, key):
        for i in range(len(self.keys)):
            if key == self.keys[i]:
                self.keys.pop(i)
                self.values.pop(i)
                return True
        return False