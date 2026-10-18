-- Heartbeat: extend the current lease holder's lease on a task.
-- KEYS[1] = queue:leased
-- ARGV[1] = task id
-- ARGV[2] = fencing token received from lease
-- ARGV[3] = lease duration in seconds
-- Returns: 1 = extended; 0 = the caller's lease is stale (reaped, or re-leased by someone else)

local task_key = 'task:' .. ARGV[1]

-- The ZSET only knows task ids, so check the token in the hash before touching the lease
if redis.call('HGET', task_key, 'fencing_token') ~= ARGV[2] then
  return 0
end
if not redis.call('ZSCORE', KEYS[1], ARGV[1]) then
  return 0
end

-- Use Redis server time, never the worker's local clock
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000

redis.call('ZADD', KEYS[1], 'XX', now + tonumber(ARGV[3]), ARGV[1])
redis.call('HSET', task_key, 'heartbeat_at', tostring(now))

return 1
