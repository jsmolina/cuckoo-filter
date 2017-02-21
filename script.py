from cuckoofilter import CuckooFilter
from random import randint
import timeit

cuckoo_redis = CuckooFilter(1000, bits_per_item=32, use_redis=True)
print("Add 2, 200, and 300")
cuckoo_redis.add(2)
cuckoo_redis.add(200)
cuckoo_redis.add(300)
print("2 is present? {0}".format(cuckoo_redis.contain(2)))
print("200 is present? {0}".format(cuckoo_redis.contain(2)))
cuckoo_redis.delete(2)
print("Delete 2, it should be false? {0}".format(cuckoo_redis.contain(2)))

cuckoo_redis.delete(200)
print("Delete 200, it should be false? {0}".format(cuckoo_redis.contain(200)))
cuckoo_redis.delete(999)

print("in redis")
cuckoo_redis = CuckooFilter(1000, bits_per_item=32, use_redis=True)
print(timeit.Timer(lambda: cuckoo_redis.add(randint(0, 1000))).timeit(number=200))


cuckoo = CuckooFilter(1000, bits_per_item=32, use_redis=False)

print("in memory (not storing at all the value!)")
print(timeit.Timer(lambda: cuckoo.add(randint(0, 1000))).timeit(number=200))