-- Release: give a leased task back to ready without counting it as a failure (e.g. on graceful shutdown).
-- KEYS[1] = queue:leased
-- KEYS[2] = queue:ready
-- ARGV[1] = task id
-- ARGV[2] = fencing token received from lease
-- Returns: 1 = released; 0 = rejected, the caller's lease is stale and nothing was changed

local task_key = 'task:' .. ARGV[1]

-- Same checks as ack: only the current lease holder may give the task back
if redis.call('HGET', task_key, 'fencing_token') ~= ARGV[2] then
  return 0
end
if not redis.call('ZSCORE', KEYS[1], ARGV[1]) then
  return 0
end

redis.call('ZREM', KEYS[1], ARGV[1])
-- RPUSH to the right end so it is the next one RPOP takes: it already waited its turn once
redis.call('RPUSH', KEYS[2], ARGV[1])
redis.call('HSET', task_key, 'state', 'pending')
redis.call('HDEL', task_key, 'worker')

return 1
