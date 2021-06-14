import struct as st
import sys

class Buffer():
    def __init__(self):
        self.block_size = 256
        self.miss_table_size =  1024
        self.miss_table = []
        self.total_size = 1024

    #读取数据文件
    def Load_data_file(self, file_path):
        self.fp = open(file_path, 'rb+')
        size = self.fp.read(4)
        self.total_size = st.unpack('i', size)[0] #unsigned long long
        num = self.fp.read(4)
        num = st.unpack('i',num)[0]
        for i in range(num):
            miss_index = self.fp.read(4)
            miss_index = st.unpack('i',miss_index)[0]
            self.miss_table.append(miss_index)

    #关闭前保存
    def Buffer_close_save(self):
        #保存信息头
        size = st.pack('i', self.total_size)
        self.fp.write(size)
        num = st.pack('i', len(self.miss_table))
        self.fp.write(num)
        for item in self.miss_table:
            temp = st.pack('i', item)
            self.fp.write(temp)
        # self.fp.close()

    #根据偏移量查找数据
    def Search_data(self, offset, attribute):
        self.fp.seek(self.block_size*offset, 0)
        res = self.fp.read(self.block_size)
        return self.unCompress(res, attribute)

    #插入数据
    def Insert_data(self, data, attribute):
        byte_data = self.Compress(data, attribute)
        insert_index = self.total_size
        if self.miss_table:
            #t填空
            insert_index = self.miss_table.pop()
        else:
            self.total_size += self.block_size
        self.fp.seek(insert_index)
        self.fp.write(byte_data)
        return int(insert_index / self.block_size)

    #内存删除
    def Delete_data(self, offset):
        self.miss_table.append(self.block_size * offset)

    #字节二进制对齐压缩
    def Compress(self, Udata, attribute):
        write_buffer = ''.encode('utf-8')
        data_size = 0
        for i in range(len(attribute)):
            temp = Udata[i]
            if attribute[i] == -1:
                format = 'i'
                data_size += 4
            elif attribute[i] == 0:
                format = 'd'
                data_size +=  8
            else:
                format = '%ss' % attribute[i]
                data_size += attribute[i]
                temp = temp.encode('utf-8')
            write_buffer += st.pack(format, temp)
        #末尾填充
        format = '%ss' % (self.block_size-data_size)
        write_buffer += st.pack(format, ''.encode('utf-8'))
        return write_buffer

    #二进制解压
    def unCompress(self, Bdata, attribute):
        res = []
        data_size = 0
        for it in attribute:
            if it == -1:
                format = 'i'
                temp = Bdata[data_size: data_size+4]
                item = st.unpack(format, temp)[0]
                data_size += 4
            elif it == 0:
                format = 'd'
                temp = Bdata[data_size: data_size+8]
                item = st.unpack(format, temp)[0]
                data_size += 8
            else:
                format = '%ss' % it
                temp = Bdata[data_size: data_size+it]
                item = st.unpack(format, temp)[0]
                item = item.decode('utf-8')
                data_size += it
            res.append(item)
        return res

#测试
# def GetAttribute(data):
#     res = []
#     for item in data:
#         if isinstance(item, int):
#             res.append(-1)
#         elif isinstance(item, float):
#             res.append(0)
#         else:
#             res.append(len(item))
#     return res

# if __name__ == '__main__':
#     bf = Buffer()
#     data_1 = [1,2,3.14,'hahaha',5.16789]
#     data_2 = ['xixixixi','ytm',123,3.333]
#     attribute_1 = GetAttribute(data_1)
#     attribute_2 = GetAttribute(data_2)
#     pe_1 = bf.Compress(data_1, attribute_1)
#     pe_2 = bf.Compress(data_2, attribute_2)
#     # print(sys.getsizeof(pe))
#     # with open('t1.txt','wb') as fp:
#     #     fp.write(pe_1)
#     #     fp.write(pe_2)
#     #     fp.close()
#     with open('t1.txt','rb') as fp:
#         fp.seek(256)
#         pt_2 = fp.read(256)
#         fp.close()
#     pd_2 = bf.unCompress(pt_2, attribute_2)
#     print(pd_2)