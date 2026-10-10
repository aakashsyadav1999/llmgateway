-- Inputs from Go
if #KEYS ~= 1 or #ARGV ~= 2 then
    return redis.error_reply('ERR expected one key, capacity, and refill rate')
end

local key      = KEYS[1]           -- "ratelimit:<SHA-256 client key digest>"
local capacity = tonumber(ARGV[1]) -- like b.capacity
local rate     = tonumber(ARGV[2]) -- like b.refillRate (tokens/sec)

if not capacity or not (capacity > 0 and capacity < math.huge) then
    return redis.error_reply('ERR capacity must be finite and positive')
end
if not rate or not (rate > 0 and rate < math.huge) then
    return redis.error_reply('ERR refill rate must be finite and positive')
end

local full_after = capacity / rate
if full_after > 9007199254740 - 1 then
    return redis.error_reply('ERR bucket refill time exceeds the supported TTL')
end

-- "now": use Redis's clock so every gateway instance agrees on the time
local t        = redis.call('TIME') -- {seconds, microseconds}
local now      = tonumber(t[1]) + tonumber(t[2]) / 1000000

-- Load the bucket (a Redis hash with two fields)
local data     = redis.call('HMGET', key, 'tokens', 'last_refill')
local tokens   = tonumber(data[1])
local last     = tonumber(data[2])

-- New client: start full (same as your NewLimiter's tokens: l.capacity)
if not data[1] and not data[2] then
    tokens = capacity
    last   = now
elseif not tokens or not (tokens >= 0 and tokens < math.huge)
    or not last or not (last >= 0 and last < math.huge) then
    return redis.error_reply('ERR invalid bucket state')
end

-- Refill: same formula as your Go code
-- Preserve a future timestamp if the Redis clock moves backwards, rather
-- than crediting the same elapsed interval again when the clock catches up.
local updated = math.max(now, last)
local elapsed = updated - last

tokens = math.min(capacity, tokens + elapsed * rate)

-- Consume
local allowed = 0
if tokens >= 1 then
    tokens  = tokens - 1
    allowed = 1
end

-- Save. tostring() keeps the decimals (0.73 tokens is a real state)
local ttl = math.ceil(full_after + updated - now) + 1
if ttl > 9007199254740 then
    return redis.error_reply('ERR bucket expiry exceeds the supported TTL')
end
redis.call('HSET', key, 'tokens', tostring(tokens), 'last_refill', string.format('%.17g', updated))

-- Expire buckets for inactive clients once they would have refilled completely
redis.call('EXPIRE', key, string.format('%.0f', ttl))

return allowed -- 1 = allowed, 0 = denied
