from BloomFilter.GeneralHashFunctions import *
import redis


class BloomFilterRedis(object):
    """
    利用redis进行分布式持久化去重
    """
    hash_list = [rs_hash, js_hash, pjw_hash, elf_hash, bkdr_hash, sdbm_hash, djb_hash, dek_hash]

    def __init__(self, key, host='127.0.0.1', port=6379, hash_list=hash_list):
        self.key = key
        self.redis = redis.StrictRedis(host=host, port=port, charset='utf-8')
        # 哈希函数列表
        self.hash_list = hash_list

    def random_generator(self, hash_value):
        '''
        将hash函数得出的函数值映射到[0, 2^32-1]区间内
        '''
        return hash_value % (1 << 32)

    def do_filter(self, item, save=True):
        """
        过滤，判断是否存在
        :param item:
        :return:
        """
        flag = True  # 默认存在
        for hash in self.hash_list:
            # 计算哈希值
            hash_value = hash(item)
            # 获取映射到位数组的下标值
            index_value = self.random_generator(hash_value)
            # 判断指定位置标记是否为 0
            if self.redis.getbit(self.key, index_value) == 0:
                # 如果不存在需要保存，则写入
                if save:
                    self.redis.setbit(self.key, index_value, 1)
                flag = False
        return flag

bloom = BloomFilterRedis('redis_key')

#　用法
# 用法if __name__ == '__main__':
#     bloom = BloomFilterRedis("bloom_url")
#     ret = bloom.do_filter("http://www.baidu.com")
#     print(ret)
#     ret = bloom.do_filter("http://www.baidu.coms")
#     print(ret)

