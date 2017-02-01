local function cleanup(buckets)
    redis.call("del", "cuckoo-table")
    for i=0, buckets do
        redis.call("lpush", "cuckoo-table", 0)
    end
    --[[ consider race conditions --]]
    --[[more packed using struct?
    -- redis.call("set", "cuckoo-table", struct.pack("I4I4I4I4", 0, 0, 0, 0)) --]]
    --[[ eval setbit with multiple calls  --]]
end

local function read_tag(i, j)
    --[[ http://bitop.luajit.org/api.html]]
    local shift = 8 * j
    local mask = bit.lshift(0xff, shift)

    --[[ local storage = struct.unpack("I4I4I4I4", redis.call("get", "cuckoo-table")) --]]
    local item = tonumber(redis.call("lindex", "cuckoo-table", i))

    local result = bit.rshift(bit.band(item, mask), shift)

    return result
end

local function write_tag(i, j, tag)
    local shift = 8 * j
    local mask = bit.lshift(0xff, shift)
    local item = tonumber(redis.call("lindex", "cuckoo-table", i))

    item = bit.bor(item, bit.band(bit.lshift(tag, shift), mask))
    return redis.call("lset", "cuckoo-table", i, item)
end

local function tag_hash(hv, bits_per_item)
    local tag = bit.band(hv, (bit.lshift(1, bits_per_item) - 1))
    tag = tag + tag == 0
    return tag
end

cleanup(8)

write_tag(0, 0, 999)
--[[ 231 expected --]]
print(read_tag(0, 0))