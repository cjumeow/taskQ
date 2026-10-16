-- Ack: remove the task from leased and mark it completed.
-- KEYS[1] = queue:leased
-- ARGV[1] = task id
-- Returns: 1 = completed normally; 0 = task was no longer leased (lease expired and was reaped)
--
-- Note: on 0 the task is still marked completed. This is a deliberate hole, closed by fencing tokens in M2.

local removed = redis.call('ZREM', KEYS[1], ARGV[1])

local t = redis.call('TIME')
redis.call('HSET', 'task:' .. ARGV[1], 'state', 'completed', 'completed_at', t[1])

return removed
