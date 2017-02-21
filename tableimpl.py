from random import randint
from array import array


class Table32Bits(object):
    def __init__(self, num_buckets, bits_per_tag=32):
        self.bits_per_tag = bits_per_tag
        self.tags_per_bucket = 4
        self.num_buckets = num_buckets

        # buckets contain an unsigned int array
        # unsigned long uses minimum 8 bytes
        self._buckets = array('L', [0] * num_buckets)

    def _shift_and_mask(self, j):
        shift = 8 * j
        mask = 0xff << shift
        return (shift, mask)

    def _read_tag(self, i, j):
        shift, mask = self._shift_and_mask(j)
        return (self._buckets[i] & mask) >> shift

    def _write_tag(self, i, j, tag):
        shift, mask = self._shift_and_mask(j)
        self._buckets[i] |= (tag << shift) & mask
        return True

    def insert_tag_to_bucket(self, i, tag, kickout):
        oldtag = None

        for j in range(self.tags_per_bucket):
            if self._read_tag(i, j) == 0:
                self._write_tag(i, j, tag)
                return True, None

        if kickout:
            r = randint(0, self.tags_per_bucket)
            # oldtag will contain kicked out tag
            oldtag = self._read_tag(i, r)
            self._write_tag(i, r, tag)
        return False, oldtag

    def find_tag_in_buckets(self, i1, i2, tag):
        for j in range(self.tags_per_bucket):
            if self._read_tag(i1, j) == tag or self._read_tag(i2, j) == tag:
                return True
        return False

    def find_tag_in_bucket(self, i, tag):
        for j in range(self.tags_per_bucket):
            if self._read_tag(i, j) == tag:
                return j
        return False

    def delete_tag_from_bucket(self, i, tag):
        j = self.find_tag_in_bucket(i, tag)
        if j is not False:
            shift, mask = self._shift_and_mask(j)
            self._buckets[i] &= ~((tag << shift) & mask)
            return True
        return False

    def __len__(self):
        return len(self._buckets)

    def size_in_tags(self):
        return self.tags_per_bucket * self.num_buckets

    def __str__(self):
        ss = ""
        ss += "SingleHashtable with tag size: " + str(self.bits_per_tag) + " bits \n"
        ss += "\t\tAssociativity: " + str(self.tags_per_bucket) + "\n"
        ss += "\t\tTotal # of rows: " + str(self.num_buckets) + "\n"
        ss += "\t\tTotal # slots: " + str(self.size_in_tags()) + "\n"
        return ss

