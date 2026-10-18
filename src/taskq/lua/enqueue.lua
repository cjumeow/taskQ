-- Enqueue: create the task record and put it on ready, or on delayed if it should run later.
-- KEYS[1] = queue:ready
-- KEYS[2] = queue:delayed
-- ARGV[1] = task id
-- ARGV[2] = delay in seconds (0 = run now)
-- ARGV[3] = max retries
-- Returns: 1

local task_key = 'task:' .. ARGV[1]
local delay = tonumber(ARGV[2])

-- Use Redis server time, never the producer's local clock
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000

redis.call('HSET', task_key, 'max_retries', ARGV[3], 'retries', 0, 'created_at', tostring(now))

if delay > 0 then
  redis.call('ZADD', KEYS[2], now + delay, ARGV[1])
  redis.call('HSET', task_key, 'state', 'delayed', 'run_at', tostring(now + delay))
else
  redis.call('LPUSH', KEYS[1], ARGV[1])
  redis.call('HSET', task_key, 'state', 'pending')
end

return 1
