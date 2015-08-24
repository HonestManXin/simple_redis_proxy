# -*- coding:utf-8 -*-
import types
import redis
import zlib
from redis_proxy import KeyHashUtil


class RedisPort(object):
    """
    将已有的redis instances中的数据迁移到其他节点上。
    注意添加的redis instance的顺序一定要和以后利用SimpleRedisPort的顺序一样。

    hosts格式: [("127.0.0.1", 6379), ("127.0.0.1", 6380)]
    """
    def __init__(self, redis_hosts, counter_keyname):
        self.redis_hosts = redis_hosts
        self.counter_keyname = counter_keyname

    @classmethod
    def _create_redis_client(cls, host, port):
        client = redis.Redis(host, port)
        client.ping()
        return client

    @classmethod
    def _execute_pipelines(cls, pipelines):
        for pipeline in pipelines:
            pipeline.execute()

    def migrate_keys(self, from_client, from_index, to_clients):
        num = len(to_clients)
        command_limit = num * 1000
        from_pipeline = from_client.pipeline()
        to_pipelines = []
        for client in to_clients:
            to_pipelines.append(client.pipeline())
        keys = from_client.keys()
        count = 0
        for key in keys:
            if key == self.counter_keyname:
                continue
            index = KeyHashUtil.get_index(key, num)
            if index != from_index:
                dump_data = from_client.dump(key)
                to_pipelines[index].restore(key, 0, dump_data)
                count += 1
                if count % command_limit == 0:
                    self._execute_pipelines(to_pipelines)
        self._execute_pipelines(to_pipelines)
        # remove key from from_client
        count = 0
        for key in keys:
            index = KeyHashUtil.get_index(key, num)
            if index != from_index:
                from_pipeline.delete(key)
                count += 1
                if count % 1000:
                    from_client.execute()
        from_pipeline.execute()

    def add_instances(self, add_hosts):
        for host in add_hosts:
            if host in self.redis_hosts:
                raise Exception("host: %s already exists" % host)
        current_clients = []
        extend_clients = []
        for host in self.redis_hosts:
            client = self._create_redis_client(*host)
            current_clients.append(client)
            extend_clients.append(client)
        for host in add_hosts:
            client = self._create_redis_client(*host)
            extend_clients.append(client)
        for index, from_client in enumerate(current_clients):
            self.migrate_keys(from_client, index, extend_clients)

    @classmethod
    def remove_instance(cls, remove_hosts):
        """
            目前对于remove instance的操作很难支持，
            主要是删除之后剩余的instance的counter不方便处理。
        """
        pass


def main():
    # redis_port = RedisPort()
    # redis_port.add_instances()
    pass


if __name__ == '__main__':
    main()
