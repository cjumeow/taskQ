-- Reap: move tasks whose lease expired without an ack back to ready.
-- KEYS[1] = queue:leased
-- KEYS[2] = queue:ready
-- ARGV[1] = max number of tasks to reap per call
-- Returns: list of reclaimed task ids

local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000

local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, tonumber(ARGV[1]))

for _, task_id in ipairs(expired) do
  redis.call('ZREM', KEYS[1], task_id)
  -- RPUSH to the right end so it is the next one RPOP takes: reclaimed tasks jump the line
  redis.call('RPUSH', KEYS[2], task_id)
  redis.call('HSET', 'task:' .. task_id, 'state', 'pending')
  redis.call('HDEL', 'task:' .. task_id, 'worker')
end

return expired
