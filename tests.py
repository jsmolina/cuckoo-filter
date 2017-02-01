from cuckoofilter import CuckooFilter, NotEnoughSpaceException
from tableimpl import Table32Bits
import unittest


class TestCuckoo(unittest.TestCase):
    TOTAL_ITEMS = 10000

    def test_32bit_table(self):

        table = Table32Bits(10)
        for i in range(10):
            tag = i * 10
            table.insert_tag_to_bucket(i, tag=tag, kickout=True)

        for i in range(10):
            tag = i * 10
            self.assertTrue(table.find_tag_in_bucket(i, tag) is not False)

        self.assertFalse(table.find_tag_in_buckets(2, 4, tag=80))
        self.assertTrue(table.find_tag_in_buckets(2, 4, tag=40))

    def test_cuckoo_simple(self):
        cuckoo = CuckooFilter(self.TOTAL_ITEMS, bits_per_item=32, use_redis=False)
        cuckoo.add(2)
        cuckoo.add(200)
        self.assertTrue(cuckoo.contain(2))
        self.assertTrue(cuckoo.contain(200))

        cuckoo.delete(2)
        self.assertFalse(cuckoo.contain(2))

    def disabled_test_cuckoo_redis(self):
        cuckoo = CuckooFilter(self.TOTAL_ITEMS, bits_per_item=32, use_redis=True)
        cuckoo.add(2)
        cuckoo.add(200)
        self.assertTrue(cuckoo.contain(2))
        self.assertTrue(cuckoo.contain(200))

        cuckoo.delete(2)
        self.assertFalse(cuckoo.contain(2))

    def test_cuckoo_overflow(self):
        # Insert items to this cuckoo filter
        cuckoo = CuckooFilter(self.TOTAL_ITEMS, bits_per_item=32, use_redis=False)
        i = 0
        try:
            for i in range(100):
                cuckoo.add(i)
        except NotEnoughSpaceException:
            print("stop in {0}".format(i))

        # Check if previously inserted items are in the filter, expected
        # true for all items
        num_inserted = i
        for i in range(num_inserted):
            self.assertEquals(cuckoo.contain(i), True, msg="Not found {0}".format(i))

        # Check non-existing items, a few false positives expected
        total_queries = 0
        false_queries = 0
        for i in range(2 * self.TOTAL_ITEMS):
            if cuckoo.contain(i):
                false_queries += 1
            total_queries += 1

        false_positive_rate = 100.0 * false_queries / total_queries

    def test_hash(self):
        cuckoo = CuckooFilter(self.TOTAL_ITEMS, bits_per_item=32, use_redis=False)
        i, h = cuckoo._generate_index_tag_hash(0)
        self.assertEquals(h, 213)
