#!/usr/bin/env tarantool
local json = require('json')
local fiber = require('fiber')
local log = require('log')
local socket = require('socket')

local subscription_channel = fiber.channel(1000)
local write_commands_channel = fiber.channel(1000)
local read_commands_channel = fiber.channel(100)

local statistics = { writes = 0, reads = 0, time = fiber.time()}

--the fiber print RPS every 5 seconds when the read or write commands was produced
fiber.create(function()
    while true do
        fiber.sleep(5)
        if statistics.reads > 0 or statistics.writes > 0 then
            local delay = fiber.time() - statistics.time
            --print RPS of reads/writes operations and unique instance ID in short format
            log.info('RPS: reads=%s, writes=%s, uuid=%s', math.floor(statistics.reads * 1.0 / delay), math.floor(statistics.writes * 1.0 / delay), box.info().uuid:sub(1,8))
            statistics = { writes = 0, reads = 0, time = fiber.time()}
        end
    end
end)

box.cfg {
    wal_mode = 'fsync'
}

box.schema.space.create('fs', {
    format = {
        {
            name = 'path',
            type = 'string',
            is_nullable = false
        },
        {
            name = 'value',
            type = 'string',
            is_nullable = false
        },
        {
            name = 'last_modified',
            type = 'number'
        },
        {
            name = 'updated_count',
            type = 'unsigned'
        }
    },
    if_not_exists = true
})

box.space.fs:create_index('primary', {
    parts = {'path'},
    --type = 'HASH',
    unique = true,
    if_not_exists = true
})

box.space.fs:on_replace(function(old, new, s, operation)
    subscription_channel:put({ operation = operation, old = old, new = new})
end)

SubscriptionObserver = { subscribers = {}}
function SubscriptionObserver:new()
    return self
end
function SubscriptionObserver:subscribe(id, key, callback)
    local s = self.subscribers[key]
    if s == nil then
        s = {}
        self.subscribers[key] = s
    end
    s[id] = callback
end
function SubscriptionObserver:unsubscribe(id, key)
    local s = self.subscribers[key]
    if s ~= nil then
        s[id] = nil
    end
end
function SubscriptionObserver:unsubscribeAll(id)
    for k, s in pairs(self.subscribers) do
        s[id] = nil
    end
end
function SubscriptionObserver:fire_event(key, event)
    for k, s in pairs(self.subscribers) do
        if k == '/' or key == k or string.sub(key,1, string.len(k)) == k then
            for i, callback in pairs(s) do
                callback(event)
            end
        end
    end
end

SimpleObserver = { listeners = {}}
function SimpleObserver:new()
    return self;
end
function SimpleObserver:subscribe(id, listener)
    self.listeners[id] = listener
end
function SimpleObserver:unsubscribe(id)
    self.listeners[id] = nil
end
function SimpleObserver:fire_event(id, event)
    local listener = self.listeners[id]
    if listener ~= nil then
        listener(event)
    end
end


local subscription_observer = SubscriptionObserver:new()
local write_observer = SimpleObserver:new()

for i = 1, 500 do
    fiber.create(function()
        while true do
            local data = subscription_channel:get()

            local operation = data.operation
            local old = data.old
            local new = data.new

            if operation == 'INSERT' then
                subscription_observer:fire_event(new[1], { command = 'set', key = new[1], value = new[2], last_modified = new[3], updated_count = 0})
            elseif operation == 'UPDATE' then
                subscription_observer:fire_event(new[1], { command = 'set', key = new[1], value = new[2], last_modified = new[3], updated_count = new[4]})
            elseif operation == 'DELETE' then
                subscription_observer:fire_event(old[1], { command = 'del', key = old[1], value = old[2], last_modified = old[3], updated_count = old[4]})
            end
        end
    end)
end

function parse_command(str)
    local tokens = {}
    for token in string.gmatch(str, "[^ ]+") do
        table.insert(tokens, token)
    end
    local command = {options = {}, name = tokens[1]}
    if #tokens > 1 then
        if string.sub(tokens[2], 1, 14) == "--routing-key=" then
            command.options.routing_key = string.sub(tokens[2], 15)
            command.key = tokens[3]
            command.value = tokens[4]
        else
            command.key = tokens[2]
            command.value = tokens[3]
        end
    end
    return command
end

local space = box.space.fs

for i = 1, 500 do
    fiber.create(function()
        while true do
            local request = write_commands_channel:get()
            local client_id = request.client_id
            local routing_key = request.routing_key
            local command = request.command
            local time = request.time

            statistics.writes = statistics.writes + 1

            local last_modified = math.floor(fiber.time() * 1000)


            local result = space:update(command.key, { {'+', 4, 1}, {'=', 3, last_modified}, { '=', 2, command.value}})
            if result == nil then
                result = space:insert{ command.key, command.value, last_modified, 0}
                write_observer:fire_event(client_id, { key = command.key, command = 'set', updated_count = 0, last_modified = last_modified, value = command.value, routing_key = routing_key, time = time})
            else
                write_observer:fire_event(client_id, {key = command.key, command = 'set', updated_count = result[4], last_modified = result[3], value = command.value, routing_key = routing_key, time = time})
            end
        end
    end)
end

for i = 1, 100 do
    fiber.create(function()
        while true do
            local request = read_commands_channel:get()
            local client_id = request.client_id
            local routing_key = request.routing_key
            local command = request.command
            local time = request.time

            statistics.reads = statistics.reads + 1

            local result = space:get(command.key)
            if result == nil then
                local response = {error_code = 404, message = string.format("value entry doesn't exists by key '%s'", command.key), routing_key = routing_key, time = time}
                write_observer:fire_event(client_id, response)
            else
                local response = {key = command.key, updated_count = result[4], last_modified = result[3], value = result[2], routing_key = routing_key, time = time}
                write_observer:fire_event(client_id, response)
            end
        end
    end)
end

local function socket_handler(s, from)
    --log.info('client %s connected', from.host)
    local client_id = s:fd()

    function on_notify_factory_handler(routing_key)
        return function(event)
            local send_event = {routing_key = routing_key}
            for k, v in pairs(event) do
                send_event[k] = v
            end
            send_event.time = nil
            s:write(json.encode(send_event)..'\n')
        end
    end
    write_observer:subscribe(client_id, function(event)
        local send_event = {latency = (fiber.time() - event.time)*1000}
        for k, v in pairs(event) do
            send_event[k] = v
        end
        send_event.time = nil
        s:write(json.encode(send_event)..'\n')
    end)

    s:write(json.encode({name = 'tarantool-vs-zookeeper', version = '1.0'}).."\n")
    while true do
        local msg = s:read('\n')
        if msg == nil or msg == "" then
            break -- error or eof
        end
        local time = fiber.time()
        --local time = fiber.time()
        local command = parse_command(string.sub(msg, 1, string.len(msg) - 1))
        if command.name ~= nil then
            --log.info('executing command %s with key %s', command.name, command.key)
            local routing_key = command.options.routing_key
            if command.name == "set" then
                write_commands_channel:put({ client_id = client_id, command = command, routing_key = routing_key, time = time})
            elseif command.name == 'get' then
                read_commands_channel:put({client_id = client_id, command = command, routing_key = routing_key, time = time})
            elseif command.name == 'exists' then
                statistics.reads = statistics.reads + 1
                local result = space:get(command.key)
                local response = {key = command.key, command = 'exists', value = (result ~= nil), routing_key = routing_key, latency = (fiber.time() - time)*1000}
                if not s:write(json.encode(response)..'\n') then
                    break;
                end
            elseif command.name == 'del' then
                statistics.writes = statistics.writes + 1
                local result = space:delete(command.key)
                if result == nil then
                    local response = {error_code = 404, message = string.format("value entry doesn't exists by key '%s'", command.key), routing_key = routing_key, latency = (fiber.time() - time)*1000}
                    if not s:write(json.encode(response)..'\n') then
                        break;
                    end
                else
                    local response = {key = command.key, command = 'del', updated_count = result[4], last_modified = result[3], value = result[2], routing_key = routing_key, latency = (fiber.time() - time)*1000}
                    if not s:write(json.encode(response)..'\n') then
                        break;
                    end
                end
            elseif command.name == 'sub' then
                subscription_observer:subscribe(client_id, command.key, on_notify_factory_handler(routing_key))
                local response = {key = command.key, command = 'sub', routing_key = routing_key, latency = (fiber.time() - time)*1000}
                if not s:write(json.encode(response)..'\n') then
                    break;
                end
            elseif command.name == 'unsub' then
                subscription_observer:unsubscribe(client_id, command.key)
                local response = {key = command.key, command = 'unsub', routing_key = routing_key, latency = (fiber.time() - time)*1000}
                if not s:write(json.encode(response)..'\n') then
                    break;
                end
            elseif command.name == 'exit' then
                break
            else
                local response = {error_code = 400, message = string.format("command '%s' doesn't exists", command.name), routing_key = routing_key, latency = (fiber.time() - time)*1000}
                if not s:write(json.encode(response)..'\n') then
                    break
                end
            end
        end
    end
    --log.info('client %s disconnected', from.host)
    subscription_observer:unsubscribeAll(client_id)
    write_observer:unsubscribe(client_id)
end

socket.tcp_server('0.0.0.0', 3311, socket_handler)
