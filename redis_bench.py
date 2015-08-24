import random
import redis
import time

COUNT_KEY = "counter"

LUA_SCRIPT = """
if redis.call("EXISTS", KEYS[1]) == 0 then
    redis.call("INCR", KEYS[2])
    redis.call("SET", KEYS[1], "1")
    return 1
else
    return 0
end
"""

def test_single_command(count, client):
    for _ in xrange(count):
        key = str(random.randint(0, count))
        if not client.exists(key):
            pass
        client.incr(COUNT_KEY)
        client.set(key, "1")


def test_pipeline(count, client):
    pipeline = client.pipeline()
    for i in xrange(1, count+1):
        key = str(random.randint(0, count))
        pipeline.exists(key)
        pipeline.incr(COUNT_KEY)
        pipeline.set(key, "1")
        if i % 2000 == 0:
            pipeline.execute()


def test_lua_script(count, client):
    multiply = client.register_script(LUA_SCRIPT)
    pipeline = client.pipeline()
    for i in xrange(1, count+1):
        key = str(random.randint(0, count))
        multiply(keys=[key, COUNT_KEY], client=pipeline)
        if i % 2000 == 0:
            pipeline.execute()


def test_start(func, client):
    client.flushall()
    count = 10000
    start_time = time.time()
    func(count, client)
    end_time = time.time()
    cost_time = end_time - start_time
    per_command = (count * 3) / cost_time
    print "%s : cost time: %s, commands: %s per seconds" % (func.__name__, cost_time, per_command)


def main():
    client = redis.Redis('10.211.55.15')

    # test_start(test_single_command, client)
    test_start(test_pipeline, client)
    # test_start(test_lua_script, client)


if __name__ == "__main__":
    main()
