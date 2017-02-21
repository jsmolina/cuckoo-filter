--[[
-- Bitwise cuckoo filter implementation with LUA
-- http://bitop.luajit.org/api.html
-- ]]

local function cleanup()
    local buckets = KEYS[1]

    redis.call("del", "cuckoo-table")
    for i=0, buckets do
        redis.call("lpush", "cuckoo-table", 0)
    end
    --[[ TODO consider race conditions --]]
end

local function read_tag()
    local i = KEYS[1]
    local j = KEYS[2]

    local shift = 8 * j
    local mask = bit.lshift(0xff, shift)

    local item = tonumber(redis.call("lindex", "cuckoo-table", i))

    local result = bit.rshift(bit.band(item, mask), shift)

    return result
end

local function write_tag()
    local i = KEYS[1]
    local j = KEYS[2]
    local tag = KEYS[3]

    local shift = 8 * j
    local mask = bit.lshift(0xff, shift)
    local item = tonumber(redis.call("lindex", "cuckoo-table", i))

    item = bit.bor(item, bit.band(bit.lshift(tag, shift), mask))
    return redis.call("lset", "cuckoo-table", i, item)
end

local function delete_tag_from_bucket()
    local i = KEYS[1]
    local tag = KEYS[2]
    local tags_per_bucket = KEYS[3]

    for j=0, tags_per_bucket do
       local shift = 8 * j
       local mask = bit.lshift(0xff, shift)
       local item = tonumber(redis.call("lindex", "cuckoo-table", i))

       if item == bit.rshift(bit.band(item, mask), shift) then
            item = bit.band(item, bit.bnot(bit.band(bit.lshift(tag, shift), mask)))
            return redis.call("lset", "cuckoo-table", i, item)
       end
    end

    return false
end

local function insert_tag_in_bucket()
    local i = KEYS[1]
    local tag = KEYS[2]
    local tags_per_bucket = KEYS[3]
    local item = tonumber(redis.call("lindex", "cuckoo-table", i))

    for j = 0, tags_per_bucket do
       local shift = 8 * j
       local mask = bit.lshift(0xff, shift)

       if bit.rshift(bit.band(item, mask), shift) == 0 then
           item = bit.bor(item, bit.band(bit.lshift(tag, shift), mask))
           redis.call("lset", "cuckoo-table", i, item)
           return 0
       end
    end

    --[[ no empty places, kickout an item randomly --]]
    local r = math.random(0, tags_per_bucket)
    local shift = 8 * r
    local mask = bit.lshift(0xff, shift)

    local kicked_out_item = bit.rshift(bit.band(item, mask), shift)
    item = bit.bor(item, bit.band(bit.lshift(tag, shift), mask))
    redis.call("lset", "cuckoo-table", i, item)

    return kicked_out_item
end

local function tag_hash()
    local hv = KEYS[1]
    local bits_per_item = KEYS[2]

    local tag = bit.band(hv, (bit.lshift(1, bits_per_item) - 1))
    tag = tag + tag == 0
    return tag
end
