from redis import Redis
from random import randint
from array import array


class LuaRedisClient(Redis):

    def __init__(self, *args, **kwargs):
        super(LuaRedisClient, self).__init__(*args, **kwargs)

        for name, snippet in self._get_lua_funcs():
            self._create_lua_method(name, snippet)

    def _get_lua_funcs(self):
        """
        Returns the name / code snippet pair for each Lua function
        in the atoms.lua file.
        """
        with open("bitwise.lua", "r") as f:
            for func in f.read().strip().split("local function "):
                if func:
                    bits = func.split("\n", 1)
                    name = bits[0].split("(")[0].strip()
                    snippet = bits[1].rsplit("end", 1)[0].strip()
                    yield name, snippet

    def _create_lua_method(self, name, snippet):
        """
        Registers the code snippet as a Lua script, and binds the
        script to the client as a method that can be called with
        the same signature as regular client methods, eg with a
        single key arg.
        """
        script = self.register_script(snippet)

        method = lambda *args: script(keys=args)
        setattr(self, name, method)

    def __getattr__(self, name):
        raise NotImplemented()


class RedisTable32Bits(object):
    def __init__(self, buckets, bits_per_tag=32):
        self.bits_per_tag = bits_per_tag
        self.tags_per_bucket = 4
        self.num_buckets = buckets

        self.redis = LuaRedisClient()

        # buckets contain an unsigned int array
        # unsigned long uses minimum 4 bytes
        self.redis.cleanup(self.num_buckets)

    def _shift_and_mask(self, j):
        shift = 8 * j
        mask = 0xff << shift
        return (shift, mask)

    def _read_tag(self, i, j):
        return self.redis.read_tag(i, j)

    def _write_tag(self, i, j, tag):
        return self.redis.write_tag(i, j, tag)

    def insert_tag_to_bucket(self, i, tag, kickout):
        oldtag = int(self.redis.insert_tag_in_bucket(i, tag, self.tags_per_bucket))

        if oldtag:
            return False, int(oldtag)
        else:
            return True, None

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
        return self.redis.delete_tag_from_bucket(i, tag, self.tags_per_bucket)

    def __len__(self):
        return self.num_buckets

    def size_in_tags(self):
        return self.tags_per_bucket * self.num_buckets

    def __str__(self):
        ss = ""
        ss += "RedisHashtable with tag size: " + str(self.bits_per_tag) + " bits \n"
        ss += "\t\tAssociativity: " + str(self.tags_per_bucket) + "\n"
        ss += "\t\tTotal # of rows: " + str(self.num_buckets) + "\n"
        ss += "\t\tTotal # slots: " + str(self.size_in_tags()) + "\n"
        return ss
