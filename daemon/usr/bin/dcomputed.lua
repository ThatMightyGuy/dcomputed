local comp = require("component")
local event = require("event")
local serial = require("serialization")

local _cfg = {}
local _state = {requests = 0}
local _tasks, _pendingTasks = {}, {}
local _log
local _servers, _busyServers = {}, {}
local _internalModem, _externalModem
local _requests = 0

local function readFile(path)
    local f, reason = io.open(path)
    if not f then return nil, reason end
    local str = f:read("a")
    f:close()
    return str
end
local function createLog()
    local log = io.open("/var/log/dcompute/dcomputed/latest.log")
    if not log then
        return io.open("/var/log/dcompute/dcomputed/latest.log", "a")
    else
        log:close()
        os.remove("/var/log/dcompute/dcomputed/previous.log")
        os.rename("/var/log/dcompute/dcomputed/latest.log", "/var/log/dcompute/dcomputed/previous.log")
        return io.open("/var/log/dcompute/dcomputed/latest.log", "a")
    end
end
local function log(msg, src, severity)
    src = string.upper(src or "MAIN")
    severity = string.upper(severity or "INFO")
    _log:write(string.format("[%s] [%s/%s] %s\n", os.date("%x %X"), src, severity, msg))
end
local function isShellExecuted()
    return pcall(debug.getlocal, 4, 1)
end
local function splitBytes(str, len)
    if #str <= len then return {str} end
    local str_t = {}
    local sz = math.ceil(str:len() / len)
    local i = 1
    repeat
        str_t[i] = str:sub(sz * i, sz)
        i = i + 1
    until str_t[i]:len() < len
    return str_t
end
local function receiveMdm(modem, to)
    to = to or 0
    local addr, from, port, message, data
    repeat
        _, addr, from, port, _, message, data = event.pull(to, "modem_message")
    until addr == modem.address or not message
    return from, port, message, data
end
local function receiveMdmAddr(modem, addr, to)
    local from, port, message, data
    repeat
        from, port, message, data = receiveMdm(modem, to)
    until from == addr
    return port, message, data
end
local function invokeAction(modem, addr, message, msgdata)
    local result, data, addresses = {}, {}, {}
    if not addr then
        local from, res, dat
        modem.broadcast(_cfg.port, message, msgdata)
        repeat
            from, res, dat = receiveMdm(_internalModem, _cfg.timeout)
            table.insert(addresses, from)
            table.insert(data, dat)
            table.insert(result, res)
        until not res
    else 
        modem.send(addr, _cfg.port, message, msgdata)
        _, result, data = receiveMdmAddr(_internalModem, addr, _cfg.timeout)
        addresses = {addr}
    end
    if not result then return "Connection timed out" end
    return result, data, addresses
end
local function contains(tbl, val)
    for v in pairs(tbl) do
        if v == val then return true end
    end
    return false
end
local function find(tbl, val)
    for k, v in pairs(tbl) do
        if v == val then return k end
    end
    return nil
end
local function push(tbl, val)
    table.insert(tbl, 1, val)
end
local function dequeue(tbl)
    local e = tbl[#tbl]
    table.remove(tbl, #tbl)
    return e
end
local function getNicestTask(tasks)
    local nicest
    for _, user in pairs(tasks) do
        for _, task in pairs(user) do
            if not nicest or nicest.nice < task.nice then nicest = task end
        end
    end
    return nicest
end
local function allocateServer(task, server)
    log("Trying to allocate another server to a task", "TASKSPOOLER", "INFO")
    if contains(_busyServers, server) then log("Target server is already busy", "TASKSPOOLER", "ERROR") return nil, "Server is already busy" end
    local code = splitBytes(task.code.code, _internalModem.maxPacketSize() - 32) -- can't be bothered calculating packet size
    invokeAction(_internalModem, server, "RUN BEGIN", code[1])
    for i = 2, #code - 1 do
        invokeAction(_internalModem, server, "RUN MID", code[i])
    end
    if #code == 1 then invokeAction(_internalModem, server, "RUN END", "")
    else invokeAction(_internalModem, server, "RUN END", code[#code]) end
end
local function runTask()
    log("Trying to run a task", "TASKSPOOLER", "INFO")
    local task = dequeue(_pendingTasks)
    if contains(_tasks, task) then
        log("Queued task is already running, dequeued", "TASKSPOOLER", "ERROR")
        return nil, "Queued task is already running, dequeued"
    end
    for i = 1, #_busyServers do
        if contains(task.code.servers, _busyServers[i]) then
            push(_pendingTasks, task)
            log("The servers are not free", "TASKSPOOLER", "WARN")
            return nil, "The servers are not free"
        end
        table.insert(_tasks, task)
    end
    local code = splitBytes(task.code.code, _internalModem.maxPacketSize() - 32) -- can't be bothered calculating packet size
    for _, v in ipairs(task.code.servers) do
        invokeAction(_internalModem, v, "RUN BEGIN", code[1])
        for i = 2, #code - 1 do
            invokeAction(_internalModem, v, "RUN MID", code[i])
        end
        if #code == 1 then invokeAction(_internalModem, v, "RUN END", {})
        else invokeAction(_internalModem, v, "RUN END", code[#code]) end
    end
    task.status = "Running"
end
local function serverFreed(server)
    table.remove(_busyServers, find(_busyServers, server))
    if #_pendingTasks == 0 then
        local lateTasks = {}
        for uk, user in pairs(_tasks) do
            for tk, task in pairs(user) do
                if _tasks[uk][tk].code.runWithLateThreads then
                    lateTasks[uk][tk] = task
                end
            end
        end
        local brk = false
        for _, user in pairs(_tasks) do
            if brk then break end
            for _, task in pairs(user) do
                if #task.servers < task.code.threads or task.code.threads == 0 then
                    allocateServer(getNicestTask(lateTasks), server)
                    brk = true
                    break
                end
            end
        end
    end
end
local function enqueueTask(task)
    if not task then return nil, "No task specified" end
    task.code = load(task.code)()
    local _freeServers = {}
    for i = 1, #_busyServers do
        if not contains(_servers, _busyServers[i]) then
            table.insert(_freeServers, _busyServers[i])
        end
    end
    if task.code.threads == 0 or task.code.threads <= #_freeServers then
        local threads = {}
        for i = 1, task.code.threads do
            threads[i] = _freeServers[i]
        end
        task.servers = threads
        table.insert(_pendingTasks, task)
        runTask()
    else
        table.insert(_pendingTasks, task)
    end
    task.status = "Queued"
end
local function stopTask(task, hard)
    task.status = "Stopping"
    for _, v in ipairs(task.code.servers) do
        if hard then
            invokeAction(_internalModem, v, "SIGKILL")
        else
            invokeAction(_internalModem, v, "SIGTERM")
        end
    end
    task.running = "Stopped"
end
local function serverWorker(from, msg, data)
    local index, task
    local brk = false
    for _, v in pairs(_tasks) do
        if not brk then
            for _, tv in pairs(v) do
                index = find(tv.servers, from)
                if index then
                    task = tv
                    brk = true
                    break
                end
            end
        end
    end
    local date = os.date("%x %X")
    local split = {}
    for _, v in ipairs(msg:gmatch("[%S]+")) do
        table.insert(split, v)
    end
    if split[1] == "FREE" and split[4] then
        event.push("dcomputed_server_freed", from)
    elseif split[1] == "EXCEPT" then
        local err, reason = io.open(task.origin.."_"..task.name.."("..index..")_error-"..date, "w")
        if not err then
            log("Unable to open error file for writing: "..reason, from, "ERROR")
            return
        end
        err:write("["..date.."] The task resulted in an error, see stacktrace below\n"..data)
        err:close()
        event.push("dcomputed_server_freed", from)
    elseif split[1] == "YIELD" and data then
        local ret = splitBytes(task.code.yield(data), _internalModem.maxPacketSize() - 32) -- still can't be bothered calculating packet size
        invokeAction(_internalModem, from, "YIELD BEGIN", ret[1])
        for i = 2, #ret - 1 do
            invokeAction(_internalModem, from, "YIELD MID", ret[i])
        end
        if #ret == 1 then invokeAction(_internalModem, from, "RUN END", "")
        else invokeAction(_internalModem, from, "YIELD END", ret[#ret]) end
    elseif split[1] == "RETURN" and split[3] and data then
        if split[2] == "BEGIN" then
            task.returnValues[split[3]] = task.returnValues[split[3]]..data
        elseif split[2] == "MID" then
            task.returnValues[split[3]] = task.returnValues[split[3]]..data
        elseif split[2] == "END" then
            task.returnValues[split[3]] = task.returnValues[split[3]]..data
            if split[4] == "HALT" then 
                if #task.servers == 1 then task.status = "Stopped"
                else task.status = "Partially stopped" end
                event.push("dcomputed_server_freed", from)
            end
            local f, reason = io.open(task.origin.."_"..task.name.."("..index..")_"..split[3].."-"..date, "w")
                if not f then log("Unable to open return file for writing: "..reason, from, "ERROR") return else
                f:write(task.returnValues[split[3]])
                f:close()
            end
        else _internalModem.send(from, _cfg.port, "ERR", "Malformed packet '"..msg.."'") return end
    end
end
local function clientWorker(from, msg, data)
    local split = {}
    for _, v in ipairs(msg:gmatch("[%S]+")) do
        table.insert(split, v)
    end
    if split[1] == "NEW" and split[3] and data then
        if split[2] == "BEGIN" then
            _tasks[from][split[3]] = {origin = from, name = split[3], status = "Downloading", servers = {}, outputFiles = {}, nice = 0, def = data}
        elseif split[2] == "MID" then
            _tasks[from][split[3]].def = _tasks[from][split[3]].def..data
        elseif split[2] == "END" then
            _tasks[from][split[3]].def = load(_tasks[from][split[3]].def..data)()
            if split[4] == "QUEUE" then enqueueTask(_tasks[from][split[3]])
            else _tasks[from][split[3]].status = "Dequeued" end
            event.push("dcomputed_task_loaded", from, split[2])
        else _externalModem.send(from, _cfg.port, "ERR", "Malformed packet '"..msg.."'") return end
    elseif split[1] == "ENQUEUE" and split[2] then
        enqueueTask(_tasks[from][split[2]])
    elseif split[1] == "STOP" and split[2] then
        local task = _tasks[from][split[2]]
        if task then
            stopTask(_tasks[from][split[2]])
        else
            _externalModem.send(from, _cfg.port, "ERR", from..":"..split[2].." does not exist")
            return
        end
    elseif split[1] == "STATUS" and split[2] then
        local task
        if not split[3] then
            task = _tasks[from][split[2]]
        else
            task = _tasks[split[2]][split[3]]
        end
        if task then
            _externalModem.send(from, _cfg.port, "STATUS", string.format("%s:%s status %s nice %d", from, split[2], task.status, task.nice)) return
        else
            _externalModem.send(from, _cfg.port, "ERR", from..":"..split[2].." does not exist")
            return
        end
    elseif split[1] == "RESULT" and split[2] then
        local task
        if not split[3] then
            task = _tasks[from][split[2]]
        else
            task = _tasks[split[2]][split[3]]
        end
        if task then
            _externalModem.send(from, _cfg.port, "STATUS", string.format("%s:%s status %s nice %d", from, split[2], task.status, task.nice)) return
        else
            _externalModem.send(from, _cfg.port, "ERR", from..":"..split[2].." does not exist")
            return
        end
    else
        _externalModem.send(from, _cfg.port, "ERR", "Malformed packet '"..msg.."'") return
    end
    _externalModem.send(from, _cfg.port, "ACK")
end
local function mdmMessageEvent(e, addr, from, _, _, message, data)
    if addr == _internalModem.address then
        serverWorker(from, message, data)
    elseif addr == _externalModem.address then
        clientWorker(from, message, data)
    end
end

function status(ostream)
    ostream = ostream or io.stdout
    ostream:write(
        string.format(
            "%s, started on %s (shell = %s)\n%d servers, %d busy\n%d requests handled\n",
            _state.running,
            _state.startedOn,
            tostring(_state.shell),
            #_servers,
            #_busyServers,
            _state.requests
        )
    )
end

function start()
    _state.startedOn = os.date("%x %X")
    _state.shell = isShellExecuted()
    _log = createLog()
    log("Starting dcomputed")
    local cfgstr, reason = readFile("/etc/dcompute/dcomputed.cfg")
    if not cfgstr then
        log("Unable to read config file: "..reason, nil, "ERROR")
        stop(false, false)
    end
    _cfg = serial.unserialize(cfgstr)
    log("Config read successfully")
    _state.running = "Loading"
    log("Attempting server connection")
    -- initialize modems
    _internalModem = comp.proxy(_cfg.internalModem)
    if not _internalModem then
        log("Invalid internal modem specified", nil, "MDMINT")
        stop(false, false) 
    end
    _externalModem = comp.proxy(_cfg.externalModem)
    if not _externalModem then
        log("Invalid external modem specified", nil, "MDMEXT")
        stop(false, false)
    end
    _internalModem.open(_cfg.port)
    _externalModem.open(_cfg.port)
    -- get server list, even if no servers specified
    -- because we all know you won't type out
    -- the full NIC addresses at a gunpoint
    local res, addresses
    if #_cfg.servers == 0 then
        res, _, addresses = invokeAction(_internalModem, nil, "CONN")
        for i, v in ipairs(res) do
            if v == "ACK" then table.insert(_servers, addresses[i]) else log("Error: "..v.." on "..addresses[i], "MDMINT", "ERROR") end
        end
    else
        for _, v in ipairs(_cfg.servers) do
            res = invokeAction(_internalModem, v, "CONN")
            if res == "ACK" then table.insert(_servers, v) else log("Error: "..res.." on "..v, "MDMINT", "ERROR") end
        end
    end
    if #_servers == 0 then log("No servers found, shutting down") stop(false, true) end
    log(#_servers.." servers connected")
    -- -- -- -- --
    event.listen("dcomputed_server_freed", runTask)
    event.listen("dcomputed_server_freed", serverFreed)
    event.listen("modem_message", mdmMessageEvent)
    _state.running = "Ready"
end

function stop(disconnect, close)
    if disconnect == nil then disconnect = true end
    if close == nil then close = true end
    log("Shutting down...")
    if disconnect then
        log("Disconnecting servers")
        for _, v in ipairs(_servers) do
            local res = invokeAction(_internalModem, v, "DCON")
            if res ~= "ACK" then log("Error: "..res.." on "..v, "MDMINT", "ERROR") end
        end
        log("Servers disconnected")
    end
    _state.running = "Stopped"
    log("Done, ".._requests.." requests handled")
    if close then
        _internalModem.close()
        _externalModem.close()
    end
    event.ignore("dcomputed_server_freed", runTask)
    event.ignore("dcomputed_server_freed", serverFreed)
    event.ignore("modem_message", mdmMessageEvent)
end
_state.running = "Stopped"
