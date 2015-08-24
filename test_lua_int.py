import redis
import sys

counter_key = "counter"

LUA_SCRIPT = """
    local id = redis.call("incr", "counter")
    local realId = 100*2^32 + id
    redis.call("set", KEYS[1], realId)
"""


def add_key(client, count, index):
    multiply = client.register_script(LUA_SCRIPT)
    pipeline = client.pipeline()
    for i in xrange(1, count+1):
        key = (index << 32) + i
        multiply(keys=[key], client=pipeline)
        if i % 1000 == 0:
            pipeline.execute()
    pipeline.execute()
    print "set over"


def check_result(client, count, index):
    print "checking"
    for i in xrange(1, count+1):
        key = str((index << 32) + i)
        data = client.get(key)
        if key != data:
            raise


def main():
    client = redis.Redis("10.16.66.97", 6380)
    client.flushall()
    count, index = 100000, 100
    add_key(client, count, index)
    check_result(client, count, index)

main()
