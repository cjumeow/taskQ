-- Ack: remove the task from leased and mark it completed, but only for the current lease holder.
-- KEYS[1] = queue:leased
-- ARGV[1] = task id
-- ARGV[2] = fencing token received from lease
-- Returns: 1 = completed; 0 = rejected, the caller's lease is stale and nothing was changed

local task_key = 'task:' .. ARGV[1]

-- A newer lease was issued, so this caller is a worker that paused or ran past its lease
if redis.call('HGET', task_key, 'fencing_token') ~= ARGV[2] then
  return 0
end
-- Reaped but not re-leased yet: the task is back in ready and will run again, so this ack must not complete it
if not redis.call('ZSCORE', KEYS[1], ARGV[1]) then
  return 0
end

redis.call('ZREM', KEYS[1], ARGV[1])
local t = redis.call('TIME')
redis.call('HSET', task_key, 'state', 'completed', 'completed_at', t[1])

return 1
