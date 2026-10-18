-- Promote: move delayed tasks (retry backoff or scheduled) whose time has come onto ready.
-- KEYS[1] = queue:delayed
-- KEYS[2] = queue:ready
-- ARGV[1] = max number of tasks to promote per call
-- Returns: list of promoted task ids

-- Use Redis server time, never the worker's local clock
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000

local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, tonumber(ARGV[1]))

for _, task_id in ipairs(due) do
  redis.call('ZREM', KEYS[1], task_id)
  redis.call('LPUSH', KEYS[2], task_id)
  redis.call('HSET', 'task:' .. task_id, 'state', 'pending')
  redis.call('HDEL', 'task:' .. task_id, 'run_at')
end

return due
