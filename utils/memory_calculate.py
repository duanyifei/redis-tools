#coding:utf8
"""
redis key 内存占用计算
"""

import random


def cal_list(conn, key, le=-1):
    """
    计算规则
    抽取 10个数据
    """
    if not le < 0:
        le = conn.llen(key)
    index_list = range(10)
    if le > 10:
        index_list = random.sample(range(le), 10)
    data_list = [conn.lindex(key, x) for x in index_list]
    data = "".join(data_list)
    print len(data)

    return



if __name__ == "__main__":
    import redis
    conn = redis.StrictRedis.from_url('')
    key = 'test_list'
    # 测试纯数字
    conn.lpush(key, *[1111111111 for x in range(10000)])
    print conn.info('memory')['used_memory']
    print(cal_list(conn, key))

    conn.lpush(key + '1', "1111111111" * 1000000)
