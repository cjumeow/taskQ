-- Enqueue: create the task record and put it on ready, or on delayed if it should run later.
-- With an idempotency key, a repeated enqueue (e.g. a producer retrying after a timeout) returns the
-- task that the key already points to instead of creating a second one.
-- KEYS[1] = queue:ready
-- KEYS[2] = queue:delayed
-- ARGV[1] = task id
-- ARGV[2] = delay in seconds (0 = run now)
-- ARGV[3] = max retries
-- ARGV[4] = payload as a JSON string ('' = none)
-- ARGV[5] = idempotency key ('' = none)
-- ARGV[6] = how long the idempotency key is remembered, in seconds
-- Returns: {task id, 1} if created; {existing task id, 0} if the idempotency key was already used

local task_id = ARGV[1]

if ARGV[5] ~= '' then
  local idem_key = 'idem:' .. ARGV[5]
  -- NX: only the first enqueue with this key wins; EX: forget the key eventually so memory doesn't grow forever
  if not redis.call('SET', idem_key, task_id, 'NX', 'EX', tonumber(ARGV[6])) then
    return {redis.call('GET', idem_key), 0}
  end
end

local task_key = 'task:' .. task_id
local delay = tonumber(ARGV[2])

-- Use Redis server time, never the producer's local clock
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000

redis.call('HSET', task_key, 'max_retries', ARGV[3], 'retries', 0, 'created_at', tostring(now))
if ARGV[4] ~= '' then
  redis.call('HSET', task_key, 'payload', ARGV[4])
end
if ARGV[5] ~= '' then
  redis.call('HSET', task_key, 'idempotency_key', ARGV[5])
end

if delay > 0 then
  redis.call('ZADD', KEYS[2], now + delay, task_id)
  redis.call('HSET', task_key, 'state', 'delayed', 'run_at', tostring(now + delay))
else
  redis.call('LPUSH', KEYS[1], task_id)
  redis.call('HSET', task_key, 'state', 'pending')
end

return {task_id, 1}
