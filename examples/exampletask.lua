local task = {}
task.threads = 1 -- 0 would be all the threads
task.runWithLateThreads = true
task.code = [[
    local yieldable = {}
    local function generateTable(num)
        for i = 1, num do
            table.insert(yieldable, i)
        end
    end
    function start(num)
        generateTable()
        yield("done", "newfile", yieldable)
    end
    function stop(hard)
        if hard then signal.push("task_kill") yield("stop", "hard")
        else yield("stop", "soft")
    end
]]
function task.yield(data)
    return data
end
function task.stop(hard)
    if hard then error("well, shit") end
end
return task