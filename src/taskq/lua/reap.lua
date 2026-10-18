-- Reap: handle tasks whose lease expired without an ack or fail.
-- An expired lease counts as a failed attempt, so a task that keeps crashing its worker
-- (and so never gets to call fail) still ends up in the DLQ instead of looping forever.
-- KEYS[1] = queue:leased
-- KEYS[2] = queue:ready
-- KEYS[3] = queue:dlq
-- ARGV[1] = max number of tasks to reap per call
-- Returns: list of {task id, 'ready' | 'dead'}

local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000

local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, tonumber(ARGV[1]))
local result = {}

for _, task_id in ipairs(expired) do
  local task_key = 'task:' .. task_id
  redis.call('ZREM', KEYS[1], task_id)
  redis.call('HDEL', task_key, 'worker')
  redis.call('HSET', task_key, 'last_error', 'lease expired')

  local retries = redis.call('HINCRBY', task_key, 'retries', 1)
  -- Tasks created without max_retries are never retried
  local max_retries = tonumber(redis.call('HGET', task_key, 'max_retries')) or 0

  if retries > max_retries then
    redis.call('LPUSH', KEYS[3], task_id)
    redis.call('HSET', task_key, 'state', 'dead', 'dead_at', tostring(now))
    result[#result + 1] = {task_id, 'dead'}
  else
    -- RPUSH to the right end so it is the next one RPOP takes: reclaimed tasks jump the line
    redis.call('RPUSH', KEYS[2], task_id)
    redis.call('HSET', task_key, 'state', 'pending')
    result[#result + 1] = {task_id, 'ready'}
  end
end

return result
