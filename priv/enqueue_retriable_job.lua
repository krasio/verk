local job = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1], "limit", 0, 1)[1]

if job then
  local decoded = cjson.decode(job)
  local queue = decoded["queue"]
  if queue then
    local queue_key = string.format('queue:%s', queue)
    local reencoded = job
    local populate_enueued_at = ARGV[2]
    if populate_enueued_at then
      decoded["enqueued_at"] = ARGV[1]
      reencoded = cjson.encode(decoded)
    end
    redis.call("LPUSH", queue_key, reencoded)
    redis.call("ZREM", KEYS[1], job)
    job = reencoded
  end
end

return job

