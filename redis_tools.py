# coding:utf8
"""
功能
    常用redis辅助代码集合

第一步 先不考虑代码结构
"""
import sys
import argparse
import logging
import redis
import six

if six.PY2:
    from urlparse import urlparse
else:
    from urllib.parse import urlparse

    raw_input = input

"""
redis类型
    str
    list
    hash
    set
    zset
"""


def get_logger(name, level=logging.DEBUG):
    logger = logging.getLogger(name)

    std = logging.StreamHandler(stream=sys.stdout)

    formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] [%(filename)s] [%(lineno)d] - %(message)s")
    std.setFormatter(formatter)

    logger.addHandler(std)

    logger.setLevel(level)
    return logger


class RedisTools(object):
    def __init__(self, **kwargs):
        redis_uri = kwargs.get("redis_uri")
        redis_uri, key = self._parse_uri(redis_uri)
        if redis_uri:
            self.redis_client = redis.StrictRedis.from_url(redis_uri)
            self.redis_uri_key = key
        else:
            self.redis_client = None
            self.redis_uri_key = None
        self.mode = "debug"  # quite, debug, normal
        self.logger = get_logger("RedisTools %s" % id(self))

    def __repr__(self):
        return "{}".format(self.redis_client)

    def _parse_uri(self, uri):
        """
        解析不同格式的 uri
        examples:
            [redis://]host[:port][/db][/key]
        :param uri:
        :return:
        """
        error = "redis uri format error: {}".format(uri)
        p = urlparse(uri.strip("/"))
        path_count = p.path.count("/")
        data = {
            "uri": "",
            "key": "",
        }
        if "." not in p.netloc:
            raise ValueError(error)
        if path_count == 2:
            key = p.path.split("/")[2]
            db = p.path.split("/")[1]
            if not db:
                raise ValueError(error)
            data['key'] = key
            data['uri'] = "redis://{}/{}".format(p.netloc, db)
        elif path_count == 1:
            db = p.path.split("/")[1]
            if not db:
                raise ValueError(error)
            data['uri'] = "redis://{}/{}".format(p.netloc, db)
        elif path_count == 0:
            data['uri'] = "redis://{}/{}".format(p.netloc, 0)
        else:
            raise ValueError(error)
        return (data['uri'], data['key'])

    def _lazy_delete_hash(self, key):
        """
        use hscan and hdel cmd to delete a large hash
        :param key:
        :return:
        """
        limits = 1000
        cursor, infos = self.redis_client.hscan(key, cursor=0, count=limits)
        if infos:
            self.redis_client.hdel(key, *infos.keys())
        while cursor:
            cursor, infos = self.redis_client.hscan(key, cursor=cursor, count=limits)
            if infos:
                self.redis_client.hdel(key, *infos.keys())
        self._print_log("delete hash \"{}\" ok !!!".format(key))
        return 1

    def _lazy_delete_list(self, key):
        """
        use rpop cmd to delete a large list
        :param key:
        :return:
        """
        pipe = self.redis_client.pipeline()
        limits = 1000
        while 1:
            # todo  use lua script
            for i in range(limits):
                pipe.rpop(key)
            pipe.execute()
            le = self.redis_client.llen(key)
            if not le:
                break
            else:
                pass
                # self._print_log("delete list \"{}\" size {}".format(key, le))
        self._print_log("delete list \"{}\" ok !!!".format(key))
        return 1

    def _lazy_delete_set(self, key):
        """
        use sscan and srem cmd to delete a large set
        :param key:
        :return:
        """
        limits = 1000
        cursor, values = self.redis_client.sscan(key, cursor=0, count=limits)
        if values:
            self.redis_client.srem(key, *values)
        while cursor:
            cursor, values = self.redis_client.sscan(key, cursor=cursor, count=limits)
            if values:
                self.redis_client.srem(key, *values)
        self._print_log("delete set \"{}\" ok !!!".format(key))
        return 1

    def _lazy_delete_string(self, key):
        """
        use hscan and hdel cmd to delete a large hash
        :param key:
        :return:
        """
        self._print_log("delete string \"{}\" ok !!!".format(key))
        return 1

    def _lazy_delete_zset(self, key):
        """
        use zremrangebyrank cmd to delete a large zset
        :param key:
        :return:
        """
        limits = 10000
        while 1:
            rem_count = self.redis_client.zremrangebyrank(key, 0, limits)
            if not rem_count:
                break
        self._print_log("delete zset \"{}\" ok !!!".format(key))
        return 1

    def _print_log(self, _log):
        if self.mode not in ['quite']:
            self.logger.debug(_log)
        return

    def copy_key(self, src=None, src_key=None, dst=None, dst_key=None):
        """
        复制 key
        :param src:
        :param src_key:
        :param dst:
        :param dst_key:
        :return:
        """
        if not src:
            src = self
        if not src_key:
            raise ValueError("copy src_key is {}".format(src_key))
        if not dst:
            dst = src
        if not dst_key:
            dst_key = "{}_copy".format(src_key)
        key_type = src.get_key_type(src_key)
        for data in src.get_value(src_key, _type=key_type):
            dst.put_value(dst_key, _type=key_type, data=data)
        dst_key_len = dst.get_key_len(dst_key)
        self._print_log("copy operation ok!! new key {} len {}".format(dst_key, dst_key_len))
        return True

    def copy_keys(self, src=None, src_key=None, dst=None, dst_key=None, keys=0):
        """
        复制 keys  支持批量指定
        :param src:
        :param src_key:
        :param dst:
        :param dst_key:
        :param keys:
        :return:
        """
        if not keys:
            return False
        for key in src.redis_client.keys(src_key):
            self.copy_key(src, key, dst, key)
        return True

    def get_key_len(self, key, _type=None):
        """
        获取数据量
        :param key:
        :param _type:
        :return:
        """
        if not _type:
            _type = self.get_key_type(key)
        if _type == 'list':
            le = self.redis_client.llen(key)
        elif _type == 'zset':
            le = self.redis_client.zcard(key)
        elif _type == 'set':
            le = self.redis_client.scard(key)
        elif _type == 'hash':
            le = self.redis_client.hlen(key)
        elif _type == 'string':
            le = self.redis_client.strlen(key)
        elif _type == 'none':
            le = 0
        else:
            raise TypeError("unknow type {} for key \"{}\"".format(_type, key))
        return le

    def get_key_type(self, key):
        """
        get key type
        :param key:
        :return:
        """
        return self.redis_client.type(key).lower().decode()

    def get_value(self, key, _type=None):
        """
        scan 获取数据
        :param key:
        :param type:
        :return:
        """
        le = self.get_key_len(key, _type)
        if _type == "list":
            start = 0
            step = 1000
            while start < le:
                data = self.redis_client.lrange(key, start, start + step - 1)
                start += step
                yield data
        elif _type == "hash":
            batch_size = 1000
            cursor, infos = self.redis_client.hscan(key, cursor=0, count=batch_size)
            yield infos
            while cursor:
                cursor, infos = self.redis_client.hscan(key, cursor=cursor, count=batch_size)
                yield infos
        elif _type == 'set':
            batch_size = 100
            cursor, infos = self.redis_client.sscan(key, cursor=0, count=batch_size)
            yield infos
            while cursor:
                cursor, infos = self.redis_client.sscan(key, cursor=cursor, count=batch_size)
                yield infos
        elif _type == 'zset':
            batch_size = 100
            cursor, infos = self.redis_client.zscan(key, cursor=0, count=batch_size)
            yield infos
            while cursor:
                cursor, infos = self.redis_client.zscan(key, cursor=cursor, count=batch_size)
                yield infos
        elif _type == 'string':
            yield self.redis_client.get(key)

    def put_value(self, key, _type, data=None):
        """
        scan 获取数据
        :param key:
        :param type:
        :return:
        """
        if not data:
            return
        if _type == 'list':
            if not isinstance(data, (list, tuple)):
                data = [data]
            if data:
                self.redis_client.rpush(key, *data)
        elif _type == 'hash':
            self.redis_client.hmset(key, data)
        elif _type == "set":
            self.redis_client.sadd(key, *data)
        elif _type == "zset":
            data = [(x[0].decode(), x[1]) for x in data]
            self.redis_client.zadd(key, **dict(data))
        elif _type == 'string':
            self.redis_client.set(key, data)
        return

    def lazy_delete(self, *keys):
        """
        lazy delete to avoid redis blocking
        :param keys:
        :return:
        """
        for key in keys:
            _type = self.get_key_type(key)
            if _type == 'none':
                self._print_log("key \"{}\" not exists".format(key))
                continue
            getattr(self, "_lazy_delete_{}".format(_type))(key)
        return len(keys)

    # 统计
    def statis_keys(self):
        """统计redis中key情况"""
        keys_len = []
        keys = self.redis_client.keys("*")
        for key in keys:
            _type = self.get_key_type(key)
            _le = self.get_key_len(key, _type)
            if _le < 2000:
                continue
            keys_len.append((key, _le))

        keys_len.sort(key=lambda x: x[1])
        for item in keys_len:
            print("{} {}".format(*item))
        return


def main():
    parser = argparse.ArgumentParser(
        description=u"redis_tools 命令帮助",
        add_help=False,
    )
    parser.add_argument('--help', action="help", help="%(prog)s help info")
    copy_help = u"""
        python redis_tools.py --copy src dst
        src/dst: redis://ip:port/0/data
    """
    copy_keys_help = u"""
            support pattern:  keys *
            python redis_tools.py --copy_keys src dst
            src/dst: redis://ip:port/0/data
        """
    parser.add_argument("--uri", action="store", help=copy_help)

    parser.add_argument("--copy", action="store", nargs="*", help=copy_help)
    parser.add_argument("--copy_keys", action="store", nargs="*", help=copy_keys_help)
    parser.add_argument("--delete", action="store", nargs="*", help=copy_help)
    parser.add_argument("--statis_keys", action="store_true", help=copy_help)

    cmd_args = parser.parse_args()

    redis_uri = ""
    if cmd_args.uri:
        redis_uri = cmd_args.uri
    redis_tools = RedisTools(redis_uri=redis_uri)
    if cmd_args.copy or cmd_args.copy_keys:
        pattern_flag = 0
        if cmd_args.copy_keys:
            pattern_flag = 1
            copy_args = cmd_args.copy_keys
        elif cmd_args.copy:
            copy_args = cmd_args.copy
        args_count = len(copy_args)
        if args_count > 2:
            parser.exit(status=1, message="type --help to have more info")
        else:
            src = copy_args[0]
            if src.startswith("redis://"):
                src = RedisTools(redis_uri=src)
                src_key = src.redis_uri_key
            if args_count == 2:
                dst = copy_args[1]
                dst = RedisTools(redis_uri=dst)
                dst_key = dst.redis_uri_key
            else:
                dst = src
                dst_key = None
            if pattern_flag:
                redis_tools.copy_keys(src=src, src_key=src_key, dst=dst, dst_key=dst_key, keys=pattern_flag)
            else:
                redis_tools.copy_key(src=src, src_key=src_key, dst=dst, dst_key=dst_key)
    elif cmd_args.delete:
        keys = cmd_args.delete
        redis_tools.lazy_delete(*keys)
    elif cmd_args.statis_keys:
        redis_tools.statis_keys()
    else:
        pass
    return


if __name__ == '__main__':
    main()
