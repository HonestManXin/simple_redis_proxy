# -*- coding:utf-8 -*-
import random
import types
import redis
import zlib

COUNT_KEY = "counter"

LUA_SCRIPT_TEMPLATE = """
if redis.call("EXISTS", KEYS[1]) == 0 then
    redis.call("SETNX", '%s')
    local id = redis.call("INCRBY", KEYS[2], 100)
    redis.call("SET", KEYS[1], "1")
    return 1
else
    return 0
end
"""


class KeyHashUtil(object):
    @classmethod
    def get_index(cls, key, num):
        index = zlib.crc32(key) % num
        return index


class _MethodProxy(object):
    def __init__(self, name, proxy_obj):
        self.name = name
        self.proxy_obj = proxy_obj

    def __call__(self, *args, **kwargs):
        if kwargs.get("name"):
            key = kwargs["name"]
        elif args is None or len(args) == 0:
            raise Exception("method: %s need key" % self.name)
        else:
            key = args[0]
        client = self.proxy_obj.get_client(key)
        func = getattr(client, self.name)
        self.proxy_obj.client_callback(client)
        return func(*args, **kwargs)


class _PipelineProxy(object):
    def __init__(self, pipelines):
        self.pipelines = pipelines
        self.num = len(pipelines)
        self._cmd_counter = 0
        self.pipeline_cmd_map = {}  # 用于记录每个pipeline在整个commands序列中的位置
        self._reset()

    def get_client(self, key):
        index = KeyHashUtil.get_index(key, self.num)
        return self.pipelines[index]

    def client_callback(self, pipeline):
        self.pipeline_cmd_map[pipeline].append(self._cmd_counter)
        self._cmd_counter += 1

    def __getattr__(self, name):
        attr = getattr(self.pipelines[0], name)
        if type(attr) is not types.MethodType:
            raise AttributeError("not support method call")
        return _MethodProxy(name, self)

    def _reset(self):
        self._cmd_counter = 0
        for pipeline in self.pipelines:
            self.pipeline_cmd_map[pipeline] = []

    def execute(self):
        if self._cmd_counter == 0:
            return []
        result = [None] * self._cmd_counter
        for pipeline in self.pipelines:
            cmd_squence = self.pipeline_cmd_map[pipeline]
            p_result = pipeline.execute()
            for i, value in enumerate(p_result):
                index = cmd_squence[i]
                result[index] = value
        self._reset()
        return result


class _MultiplyProxy(object):
    def __init__(self, multiplies):
        self.multiplies = multiplies
        self.num = len(multiplies)

    def __call__(self, keys=None, args=None, client=None):
        """
        注意我们只根据keys中第一个元素进行hash，
        因此需要确保lua脚本中的所有key在一个redis instance上。
        或者仅仅只是用单key的指令。
        """
        if keys is None and len(self.multiplies) != 1:
            raise Exception("keys must be set when there are more than one redis instance")
        hash_key = keys[0]

        if isinstance(client, _PipelineProxy):
            proxy = client
            client = client.get_client(hash_key)
            proxy.client_callback(client)
        elif isinstance(client, SimpleRedisProxy):
            proxy = client
            client = client.get_client(hash_key)
            proxy.client_callback(client)

        index = KeyHashUtil.get_index(hash_key, self.num)
        multiply = self.multiplies[index]
        return multiply(keys, args, client)


class SimpleRedisProxy(object):
    def __init__(self, redis_hosts):
        """
            example: redis_hosts = [("127.0.0.1", 6379), ("127.0.0.1", 6380)]
        """
        self.num = len(redis_hosts)
        self.clients = []
        for host, port in redis_hosts:
            client = redis.Redis(host, port)
            self.clients.append(client)

    def __getattr__(self, name):
        attr = getattr(self.clients[0], name)
        if type(attr) is not types.MethodType:
            raise AttributeError("not support method call")
        return _MethodProxy(name, self)

    def client_callback(self, pipeline):
        pass

    def flushall(self):
        result = True
        for client in self.clients:
            result = result and client.flushall()
        return result

    def keys(self):
        result = []
        for client in self.clients:
            result.extend(client.keys())
        return result

    def pipeline(self):
        pipelines = []
        for c in self.clients:
            p = c.pipeline()
            pipelines.append(p)
        return _PipelineProxy(pipelines)

    def register_script(self, template):
        multplies = []
        for i in xrange(self.num):
            script = template % i
            multply = self.clients[i].register_script(script)
            multplies.append(multply)
        return _MultiplyProxy(multplies)

    def get_client(self, key):
        index = KeyHashUtil.get_index(key, self.num)
        return self.clients[index]


def test_normal_method(proxy, count):
    for i in xrange(count):
        proxy.set(str(i), str(i))


def test_pipeline_method(proxy, count):
    pipeline = proxy.pipeline()
    for i in xrange(count):
        key = str(random.randint(0, count))
        pipeline.exists(key)
        pipeline.incr(COUNT_KEY)
        pipeline.set(key, "1")
        if (i+1) % 5000 == 0:
            pipeline.execute()
    pipeline.execute()


def test(proxy):
    count = 10000
    # proxy.flushall()
    import time
    start_time = time.time()
    # test_normal_method(proxy, count)
    test_pipeline_method(proxy, count)
    cost_time = time.time() - start_time
    per_commands = (3 * count) / cost_time
    print "cost time: %s, %s commands per seconds" % (cost_time, per_commands)


def main():
    # host = "10.16.66.97"
    host = "10.211.55.15"
    ports = [6379, 6380, 6381]
    addrs = []
    for port in ports:
        addrs.append((host, port))
    proxy = SimpleRedisProxy(addrs)
    test(proxy)


if __name__ == "__main__":
    main()
