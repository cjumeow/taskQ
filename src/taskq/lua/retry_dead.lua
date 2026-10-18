-- Retry dead: move a task from the DLQ back onto ready with a fresh retry budget.
-- KEYS[1] = queue:dlq
-- KEYS[2] = queue:ready
-- ARGV[1] = task id
-- Returns: 1 = moved; 0 = task was not in the DLQ

if redis.call('LREM', KEYS[1], 1, ARGV[1]) == 0 then
  return 0
end

-- Keep last_error so the history of why it died is still visible
redis.call('HSET', 'task:' .. ARGV[1], 'state', 'pending', 'retries', 0)
redis.call('HDEL', 'task:' .. ARGV[1], 'dead_at')
redis.call('LPUSH', KEYS[2], ARGV[1])

return 1
