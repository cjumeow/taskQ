-- Fail: the handler raised. Schedule a retry with exponential backoff, or move the task to the DLQ.
-- KEYS[1] = queue:leased
-- KEYS[2] = queue:delayed
-- KEYS[3] = queue:dlq
-- ARGV[1] = task id
-- ARGV[2] = fencing token received from lease
-- ARGV[3] = error message
-- ARGV[4] = backoff base in seconds
-- ARGV[5] = backoff cap in seconds
-- ARGV[6] = random number in [0, 1) for jitter (Lua's math.random is not reliably random inside Redis)
-- Returns: {'retry', retries, delay} | {'dead', retries} | {'stale'}

local task_key = 'task:' .. ARGV[1]

-- Same checks as ack: only the current lease holder may report the outcome
if redis.call('HGET', task_key, 'fencing_token') ~= ARGV[2] then
  return {'stale'}
end
if not redis.call('ZSCORE', KEYS[1], ARGV[1]) then
  return {'stale'}
end

local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000

redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HDEL', task_key, 'worker')
redis.call('HSET', task_key, 'last_error', ARGV[3])

local retries = redis.call('HINCRBY', task_key, 'retries', 1)
-- Tasks created without max_retries are never retried
local max_retries = tonumber(redis.call('HGET', task_key, 'max_retries')) or 0

if retries > max_retries then
  redis.call('LPUSH', KEYS[3], ARGV[1])
  redis.call('HSET', task_key, 'state', 'dead', 'dead_at', tostring(now))
  return {'dead', retries}
end

-- Exponential backoff with "equal jitter": wait between 50% and 100% of base * 2^(retries - 1),
-- so workers that failed at the same moment don't all retry at the same moment
local backoff = math.min(tonumber(ARGV[4]) * 2 ^ (retries - 1), tonumber(ARGV[5]))
local delay = backoff * (0.5 + 0.5 * tonumber(ARGV[6]))

redis.call('ZADD', KEYS[2], now + delay, ARGV[1])
redis.call('HSET', task_key, 'state', 'delayed', 'run_at', tostring(now + delay))

-- Lua numbers are truncated to integers in replies, so return the delay as a string
return {'retry', retries, tostring(delay)}
