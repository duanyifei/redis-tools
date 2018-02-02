# coding:utf8
"""
测试
"""
import test_setting
import unittest

import redis_tools

redis_uri = test_setting.redis_uri

class RedisTest(unittest.TestCase):
    def setUp(self):

        self.redis_tools = redis_tools.RedisTools(redis_uri=redis_uri)
        # todo  测试其他数据类型copy 删除
        self.key_types = [
            # "str",
            "list",
            # "hash",
            # "set",
            # "zset",
        ]

        self.delete_key_base = "_test_delete_{}"
        self.delete_keys = [self.delete_key_base.format(x) for x in self.key_types]

        self.copy_key_base = "_test_copy_{}"
        self.copy_keys = [self.copy_key_base.format(x) for x in ['str', 'list', 'hash', 'set', 'zset']]

    def tearDown(self):
        # 清理由于测试失败遗留的数据
        self.redis_tools.redis_client.delete(*self.delete_keys)
        # copy
        self.redis_tools.redis_client.delete(*self.copy_keys)
        self.redis_tools.redis_client.delete(*["{}_copy".format(x) for x in self.copy_keys])
        return

    def test_lazy_delete(self):
        # 准备list数据
        self.redis_tools.redis_client.lpush(self.delete_key_base.format("list"), *range(8893))
        # 测试lazy删除
        delete_result = self.redis_tools.lazy_delete(*self.delete_keys)
        # 判断函数返回值
        self.assertEqual(delete_result, len(self.delete_keys))
        # 判断是否删除干净
        for key in self.delete_keys:
            self.assertFalse(self.redis_tools.redis_client.exists(key))
        return

    def test_copy(self):
        for _type in self.key_types:
            src_key = self.copy_key_base.format(_type)
            dst_key = self.copy_key_base.format(_type) + "_copy"
            if _type == 'list':
                self.redis_tools.redis_client.lpush(src_key, *range(100))
            # 测试lazy删除
            copy_result = self.redis_tools.copy_key(src_key=src_key)
            self.assertTrue(copy_result)
            self.assertTrue(self.redis_tools.redis_client.exists(dst_key))
            self.assertEqual(self.redis_tools.get_key_len(src_key), self.redis_tools.get_key_len(dst_key))
        return

    def test_parse_uri(self):
        """测试 _parse_uri函数"""
        test_case = {
            "redis://afs": "ValueError",
            "redis://123.123.123.123:80": ("redis://123.123.123.123:80/0", ""),
            "redis://123.123.123.123/0": ("redis://123.123.123.123/0", ""),
            "redis://123.123.123.123/0/data": ("redis://123.123.123.123/0", "data"),
            "redis://123.123.123.123/0/data/data": "ValueError",
            "123.123.123.123/0/data/data": "ValueError",
            "abc.abc.com/0/data/data": "ValueError",
            "fasdfaicneianf:80": "ValueError",
            "fasdfawefa:80/data": "ValueError",
        }
        for uri, resp in test_case.items():
            try:
                new_resp = self.redis_tools._parse_uri(uri)
            except Exception as e:
                print(e)
                self.assertIn(resp, repr(e))
                continue
            self.assertEqual(resp, new_resp)
        return

if __name__ == '__main__':
    unittest.main()
