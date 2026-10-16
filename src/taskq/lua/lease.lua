-- Lease: pop a task from ready, record its lease expiry, and update its state in one atomic step.
-- KEYS[1] = queue:ready
-- KEYS[2] = queue:leased
-- ARGV[1] = worker id
-- ARGV[2] = lease duration in seconds
-- Returns: task id, or nil if the queue is empty

local task_id = redis.call('RPOP', KEYS[1])
if not task_id then
  return false
end

-- Use Redis server time, never the worker's local clock
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000

redis.call('ZADD', KEYS[2], now + tonumber(ARGV[2]), task_id)

local task_key = 'task:' .. task_id
redis.call('HSET', task_key, 'state', 'running', 'worker', ARGV[1], 'leased_at', tostring(now))
redis.call('HINCRBY', task_key, 'attempts', 1)

return task_id
