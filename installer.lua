local fs = require("filesystem")
local sh = require("shell")
local args = {...}
local function copyDir(path, newPath) -- don't think this supports root directories
    for p, f in fs.list(path) do
        if f[#f] == "/" then
            fs.makeDirectory(p..f)
            copyDir(p..f, newPath..f)
        else
            print(p.."/"..f)
            fs.copy(p..f, newPath..f)
        end
    end
end
if args[1] == "daemon" then
    copyDir(fs.path(args[0]).."/"..args[1], "/")
    print("Create an rc service? (Y/n)")
    if io.read() ~= "\n" then
        local f = io.open("~/.shrc", "a")
        f:write("ln /usr/bin/dcomputed.lua /etc/rc.d/dcomputed.lua")
        f:close()
        sh.execute("rc enable dcomputed")
    end
    return 0
elseif #args == 0 then print("Choose daemon, server or client installation\nby typing 'installer (type)'") return 1 end