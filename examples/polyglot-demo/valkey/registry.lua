-- Charger registry, shared by the Java, Rust and Go implementations.
--
-- The OCPP workers call this script for every charger request they process,
-- so the registry is shared state that any number of stateless instances
-- (workers, command APIs, dashboards) read and write. Running the logic
-- inside Valkey keeps every update atomic, and keeps the three
-- implementations consistent by construction.
--
-- Data, all under one hash tag so a Valkey cluster keeps it on one shard:
--   <p>c:<id>          hash: v (protocol), sp (security profile), on (0/1),
--                      seen (ms), c:<n> (connector status), t:<n> (its timestamp)
--   <p>known           set of every charge point id ever seen
--   <p>online          set of connected charge points
--   <p>online:v:<ver>  connected charge points by protocol (ocpp16, ocpp21...)
--   <p>online:sp:<n>   connected charge points by security profile
--   <p>status          hash: connector status -> count, over connected chargers
--
-- Competing workers may process a charger's frames out of order, so an
-- online/offline transition is never decided from event order alone: when
-- one looks due, the script answers "check" and the caller asks the broker
-- whether the charger's queue (ocpp.<id>) has a consumer, then reports the
-- answer with the "presence" operation. Transitions are rare (connects and
-- disconnects), so the extra round trip is cheap.
--
-- KEYS[1] = <p>c:<id>
-- ARGV[1] = prefix <p>, e.g. "csms:{csms-go}:"
-- ARGV[2] = operation: "event" | "offline" | "presence"
-- ARGV[3] = charge point id
-- ARGV[4] = now, epoch milliseconds
-- event:    ARGV[5] protocol, ARGV[6] security profile, ARGV[7] connector ("" = none),
--           ARGV[8] connector status, ARGV[9] status timestamp (RFC 3339, may be "")
-- presence: ARGV[5] "1" when the broker reports the charger connected, else "0"
--
-- Returns "check" when the caller must confirm presence, "ok" otherwise.

local p, op, id, now = ARGV[1], ARGV[2], ARGV[3], ARGV[4]
local c = KEYS[1]

-- Adds or removes a charger from the online indexes and status counts.
local function index(add, version, sp)
  local cmd = add and 'SADD' or 'SREM'
  redis.call(cmd, p .. 'online', id)
  if version and version ~= '' then
    redis.call(cmd, p .. 'online:v:' .. version, id)
  end
  if sp and sp ~= '' then
    redis.call(cmd, p .. 'online:sp:' .. sp, id)
  end
  local fields = redis.call('HGETALL', c)
  for i = 1, #fields, 2 do
    if string.sub(fields[i], 1, 2) == 'c:' then
      redis.call('HINCRBY', p .. 'status', fields[i + 1], add and 1 or -1)
    end
  end
end

if op == 'event' then
  local version, sp, connector, status, ts = ARGV[5], ARGV[6], ARGV[7], ARGV[8], ARGV[9]
  redis.call('SADD', p .. 'known', id)
  local cur = redis.call('HMGET', c, 'v', 'on')
  local online = cur[2] == '1'
  if online and cur[1] and cur[1] ~= version then
    -- Reconnected with another protocol version without us seeing it go.
    redis.call('SREM', p .. 'online:v:' .. cur[1], id)
    redis.call('SADD', p .. 'online:v:' .. version, id)
  end
  redis.call('HSET', c, 'v', version, 'sp', sp, 'seen', now)

  if connector ~= '' then
    local previous = redis.call('HGET', c, 'c:' .. connector)
    local previousTs = redis.call('HGET', c, 't:' .. connector)
    -- Ignore a status older than the one we hold (charger timestamps).
    if ts == '' or not previousTs or ts >= previousTs then
      if previous ~= status then
        redis.call('HSET', c, 'c:' .. connector, status)
        if online then
          if previous then
            redis.call('HINCRBY', p .. 'status', previous, -1)
          end
          redis.call('HINCRBY', p .. 'status', status, 1)
        end
      end
      if ts ~= '' then
        redis.call('HSET', c, 't:' .. connector, ts)
      end
    end
  end
  return online and 'ok' or 'check'

elseif op == 'offline' then
  redis.call('HSET', c, 'seen', now)
  return redis.call('HGET', c, 'on') == '1' and 'check' or 'ok'

elseif op == 'presence' then
  local connected = ARGV[5] == '1'
  local cur = redis.call('HMGET', c, 'v', 'sp', 'on')
  local online = cur[3] == '1'
  if connected and not online then
    redis.call('HSET', c, 'on', '1')
    index(true, cur[1], cur[2])
  elseif online and not connected then
    redis.call('HSET', c, 'on', '0')
    index(false, cur[1], cur[2])
  end
  return 'ok'
end

return redis.error_reply('unknown operation ' .. tostring(op))
