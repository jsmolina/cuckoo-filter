from tableimpl import Table32Bits
from tableredis import RedisTable32Bits


# maximum number of cuckoo kicks before claiming failure
KMAXCUCKOOCOUNT = 500


def hash_djb2(value):
    """ Computes a 32bit hash using the djb2 algorithm """
    value = str(value)

    result = 5381
    for ch in value:
        result = result * 33 + ord(ch)
    return result & 0xffffffff


def upperpower2(x):
    x = int(x)
    x -= 1
    x |= x >> 1
    x |= x >> 2
    x |= x >> 4
    x |= x >> 8
    x |= x >> 16
    x |= x >> 32
    x += 1
    return x


class CuckooExeption(Exception):
    pass


class NotEnoughSpaceException(CuckooExeption):
    pass


class NotSupportedException(CuckooExeption):
    pass


class NotFoundException(CuckooExeption):
    pass


class VictimCache(object):
    def __init__(self, index=0, tag=0, used=False):
        self.index = index
        self.tag = tag
        self.used = used


class CuckooFilter(object):
    def __init__(self, max_num_keys, bits_per_item, use_redis=False):
        self.bits_per_item = bits_per_item
        self.max_num_keys = max_num_keys

        assoc = 4
        num_buckets = upperpower2(max_num_keys / assoc)
        frac = max_num_keys / num_buckets / assoc
        # todo check why
        if frac > 0.96:
            num_buckets = num_buckets << 1

        if not use_redis:
            self._table = Table32Bits(num_buckets, bits_per_item)
        else:
            self._table = RedisTable32Bits(num_buckets, bits_per_item)

        self._victim = VictimCache(used=False)

        # number of stored items
        self._num_items = 0

    def __len__(self):
        return self._num_items

    def load_factor(self):
        return 1.0 * len(self) / self._table.size_in_tags()

    def _taghash(self, hv):
        # todo why it differs a bit from JS implementation?
        tag = hv & ((1 << self.bits_per_item) - 1)
        tag += (tag == 0)
        return tag

    def _generate_index_tag_hash(self, item):
        """
        Private method to obtain the initial index and the tag/fingerprint
        :param item: the item to get the corresponding index and tag
        :return: a tuple of (index, tag)
        """
        hashed_key = hash_djb2(item)

        #return ((hashed_key >> 32) % len(self._table), self._taghash(hashed_key))
        return ((hashed_key >> 8) % len(self._table), hashed_key & 0xff)

    def _alt_index(self, index, tag):
        """
        Private method to obtain the alternate index for an item, based on
         the tag instead of the original value.
         Note that the paper describes this operation as: index XOR hash(tag)
         however on the C++ implementation it's optimized by hardcoding the
         MurmurHash2 constant as a means of having a very quick hash-like function.
        """
        return (index ^ (tag * 0x5bd1e995)) % len(self._table)

    def add(self, item):
        if self._victim.used:
            raise NotEnoughSpaceException()

        index, tag = self._generate_index_tag_hash(item)
        return self._concrete_add(index, tag)

    def _concrete_add(self, i, tag):
        """
        Strategy pattern to allow reimplementation
        """
        curindex = i
        curtag = tag

        for count in range(KMAXCUCKOOCOUNT):
            # first time won't kickout
            kickout = count > 0
            success, oldtag = self._table.insert_tag_to_bucket(curindex, curtag, kickout)
            if success:
                self._num_items += 1
                return True

            if kickout:
                curtag = oldtag
            curindex = self._alt_index(curindex, curtag)

        # if max kickout raised, next time we won't allow insertions
        self._victim.index = curindex
        self._victim.tag = curtag
        self._victim.used = True

        return True

    def contain(self, item):
        i1, tag = self._generate_index_tag_hash(item)
        i2 = self._alt_index(i1, tag)

        # we should be able to obtain i1 from i2 too, remove when tested
        assert(i1 == self._alt_index(i2, tag))

        # first check if our candidate is not the latest victim
        found = self._victim.used and (tag == self._victim.tag) and \
                (i1 == self._victim.index or i2 == self._victim.index)

        return found or self._table.find_tag_in_buckets(i1, i2, tag)

    def delete(self, item):
        i1, tag = self._generate_index_tag_hash(item)
        i2 = self._alt_index(i1, tag)

        found_as_victim = self._victim.used and (tag == self._victim.tag) and \
                (i1 == self._victim.index or i2 == self._victim.index)

        if found_as_victim:
            self._victim.used = False
            return True

        if self._table.delete_tag_from_bucket(i1, tag):
            self._num_items -= 1
            return self._try_revive_victim()
        elif self._table.delete_tag_from_bucket(i2, tag):
            self._num_items -= 1
            return self._try_revive_victim()
        else:
            raise NotFoundException()

    def _try_revive_victim(self):
        """
        We might have space, let's try to add the cached victim
        """
        if self._victim.used:
            self._victim.used = False
            self._concrete_add(self._victim.index, self._victim.tag)
            return True

    def __add__(self, item):
        return self.add(item)

    def __contains__(self, item):
        return self.contain(item)

    def __str__(self):
        ss = ""
        ss += "CuckooFilter Status:\n"

        ss += "\t\t" + str(self._table) + "\n"
        ss += "\t\tKeys stored: " + str(len(self)) + "\n"
        ss += "\t\tLoad factor: " + str(self.load_factor()) + "\n"
        ss += "\t\tHashtable size: " + str(self._table.size_in_bytes() + 10)
        ss += " KB\n"

        if len(self) > 0:
            ss += "\t\tbit/key:   " + self.bits_per_item() + "\n"
        else:
            ss += "\t\tbit/key:   N/A\n"
        return ss
