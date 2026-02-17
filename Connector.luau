    if not game:IsLoaded() then game.Loaded:Wait() end

    -- Expose executor in _G.ExecutorAPI for MCP-invoked code.
    _G.ExecutorAPI = _G.ExecutorAPI or {}
    if not _G.ExecutorAPI._initialized then
        local genv = getgenv and getgenv() or {}
        for name, value in pairs(genv) do _G.ExecutorAPI[name] = value end
        for name, value in pairs(_G) do
            if not _G.ExecutorAPI[name] then _G.ExecutorAPI[name] = value end
        end
        _G.ExecutorAPI._initialized = true
    end

    -- execEnv: loadstring runs in this so executor APIs are in scope.
    local function createExecutionContext()
        local baseEnv = getfenv and getfenv() or _G
        local env = {}
        setmetatable(env, { __index = baseEnv })
        local genv = getgenv and getgenv() or {}
        for name, value in pairs(genv) do
            if type(value) == "function" then env[name] = value end
        end
        for name, value in pairs(_G) do
            if type(value) == "function" and not env[name] then env[name] = value end
        end
        env.ExecutorAPI = _G.ExecutorAPI
        return env
    end
    local execEnv = createExecutionContext()

    local BridgeURL = (function()
        local g = getgenv and getgenv()
        if g and g.BridgeURL then return g.BridgeURL end
        return "localhost:16384"
    end)()
    local LuaEncode = loadstring(game:HttpGet("https://raw.githubusercontent.com/chadhyatt/LuaEncode/refs/heads/master/src/LuaEncode.lua"))()
    local cloneref = cloneref or function(x) return x end
    local getnilinstances = getnilinstances or function() return {} end

    local RobloxReplicatedStorage = cloneref(game:GetService("RobloxReplicatedStorage"))
    local MarketplaceService = cloneref(game:GetService("MarketplaceService"))
    local HttpService = cloneref(game:GetService("HttpService"))
    local LogService = cloneref(game:GetService("LogService"))

    -- WebSocket preferred; fallback to HTTP poll.
    local _g = getgenv and getgenv()
    local WebSocketAvailable = (
        typeof(WebSocket) ~= "nil" and
        typeof(WebSocket.connect) == "function"
    ) and (not _g or _g.DisableWebSocket ~= true)

    local BaseBridge = {}; do
        BaseBridge.__index = BaseBridge
        
        function BaseBridge:BindToType(type, callback)
            self.Callbacks[type] = callback
        end
        
        function BaseBridge:HandleMessage(data)
            if self.Callbacks[data.type] then
                local success, result = pcall(function()
                    return self.Callbacks[data.type](data)
                end)
        
                if success and result == nil then
                    return nil
                end
        
                return self:FormatResponse(result, data.id, not success)
            end
            return nil
        end
        
        function BaseBridge:FormatResponse(message, id, isError)
            if isError then
                return {
                    error = tostring(message),
                    success = false,
                    id = id
                }
            end
            
            local output = message
            if typeof(output) ~= "table" then
                output = { output }
            end
        
            return {
                output = LuaEncode(output, {
                    Prettify = true
                }),
                success = true,
                id = id
            }
        end
        
        function BaseBridge:IsAlive()
            local success, result = pcall(function()
                local response
                local thread = task.spawn(function()
                    local success, data = pcall(request, {
                        Url = "http://" .. BridgeURL,
                        Method = "GET"
                    })

                    if success then
                        response = data
                    end
                end)

                local start = os.clock()
                repeat task.wait() until response ~= nil or os.clock() - start > 5

                if response == nil then
                    task.cancel(thread)
                    return false
                end
        
                -- 426 = Upgrade Required (WebSocket), 200 = HTTP mode
                if response.StatusCode == 426 or response.StatusCode == 200 then
                    return true
                end
        
                return false
            end)
        
            if not success then
                return false
            end
        
            return result
        end
    end

    local WebSocketBridge = setmetatable({}, {__index = BaseBridge}); do
        WebSocketBridge.__index = WebSocketBridge
        
        function WebSocketBridge.new()
            local self = setmetatable({}, WebSocketBridge)
            while not self:IsAlive() do task.wait(0.1) end
            self.WebSocket = WebSocket.connect("ws://" .. BridgeURL)
            self.Connected = true
            self.Callbacks = {}
            self.AliveThread = task.spawn(function()
                while task.wait(1) do
                    if self:IsAlive() or not self.Connected then
                        continue
                    end
                    
                    self.Connected = false
                    break
                end
        
                pcall(function()
                    self.WebSocket:Close()
                end)
            end)
        
            self.WebSocket.OnMessage:Connect(function(message)
                local ok, data = pcall(HttpService.JSONDecode, HttpService, message)
                if not ok or not data then return end
                local response = self:HandleMessage(data)
                if response then
                    pcall(function()
                        self.WebSocket:Send(HttpService:JSONEncode(response))
                    end)
                end
            end)
        
            self.WebSocket.OnClose:Connect(function()
                self.Connected = false
            end)
        
            return self
        end
        
        function WebSocketBridge:WaitForDisconnect()
            while self.Connected do
                task.wait(0.1)
            end
        end
    end

    local HTTPBridge = setmetatable({}, {__index = BaseBridge}); do
        HTTPBridge.__index = HTTPBridge
        
        function HTTPBridge.new()
            local self = setmetatable({}, HTTPBridge)
            while not self:IsAlive() do task.wait(0.1) end
            self.Connected = true
            self.Callbacks = {}
            self.PollInterval = 0.1
        
            self.PollThread = task.spawn(function()
                local consecutiveFailures = 0
                
                while self.Connected do
                    local success, _ = pcall(function()
                        local response = request({
                            Url = "http://" .. BridgeURL .. "/poll",
                            Method = "GET"
                        })

                        if response.StatusCode ~= 204 and response.StatusCode ~= 200 then
                            consecutiveFailures += 1
                            if consecutiveFailures >= 3 then
                                self.Connected = false
                            end

                            return
                        end
                        
                        consecutiveFailures = 0
        
                        if response.Body and response.Body ~= "" then
                            local data = HttpService:JSONDecode(response.Body)
                            if data == nil or data.type == nil then return end
                            
                            local result = self:HandleMessage(data)
                            if result == nil then return end
                            
                            request({
                                Url = "http://" .. BridgeURL .. "/respond",
                                Method = "POST",
                                Headers = {
                                    ["Content-Type"] = "application/json"
                                },
                                Body = HttpService:JSONEncode(result)
                            })
                        end
                    end)
        
                    if not success then
                        consecutiveFailures += 1
                        if consecutiveFailures >= 3 then
                            self.Connected = false
                        end
                    end
        
                    task.wait(self.PollInterval)
                end
            end)
        
            return self
        end
        
        function HTTPBridge:WaitForDisconnect()
            while self.Connected do
                task.wait(0.1)
            end
        end
    end

    local function CreateBridge()
        if WebSocketAvailable then
            local success, bridge = pcall(function()
                return WebSocketBridge.new()
            end)
            
            if success then
                return bridge
            end
        end

        return HTTPBridge.new()
    end

    while true do
        local Bridge = CreateBridge()

        Bridge:BindToType("execute", function(data)
            local fn, err = loadstring(data.source)
            if not fn then
                return { error = "Failed to compile code: " .. tostring(err) }
            end
            if setfenv then setfenv(fn, execEnv) end
            fn()
        end)

        Bridge:BindToType("get-console-output", function(data)
            local limit = data.limit or 50
            local logsOrder = data.logsOrder or "NewestFirst"
            local messageTypeFilter = data.messageType or "All"
            local sinceTimestamp = type(data.sinceTimestamp) == "number" and data.sinceTimestamp or nil

            local logs = LogService:GetLogHistory()
            local MessageType = Enum.MessageType
            local typeMap = {
                All = nil,
                Error = MessageType.MessageError,
                MessageError = MessageType.MessageError,
                Warning = MessageType.MessageWarning,
                MessageWarning = MessageType.MessageWarning,
                MessageOutput = MessageType.MessageOutput,
                Info = MessageType.MessageInfo,
                MessageInfo = MessageType.MessageInfo,
            }
            local wantType = typeMap[messageTypeFilter] or typeMap[messageTypeFilter and tostring(messageTypeFilter)] or nil

            local filtered = {}
            for _, log in ipairs(logs) do
                if sinceTimestamp and log.timestamp and log.timestamp < sinceTimestamp then
                elseif wantType and log.messageType ~= wantType then
                else
                    table.insert(filtered, log)
                end
            end

            local results = {}
            if logsOrder == "NewestFirst" then
                for i = #filtered, 1, -1 do
                    if #results >= limit then break end
                    table.insert(results, filtered[i])
                end
            else
                for _, log in ipairs(filtered) do
                    if #results >= limit then break end
                    table.insert(results, log)
                end
            end

            return { count = #filtered, limited = #filtered > limit, results = results }
        end)

        Bridge:BindToType("get-data-by-code", function(data)
            local fn, err = loadstring(data.source)
            if not fn then
                return { error = "Failed to compile code: " .. tostring(err) }
            end
            if setfenv then setfenv(fn, execEnv) end
            local dataOutput = table.pack(pcall(fn))
            local success = dataOutput[1]
            table.remove(dataOutput, 1) -- pcall prepends success; strip for serialization

            if not success then
                return { error = dataOutput[1] or "Unknown error during execution" }
            end

            return dataOutput
        end)

        Bridge:BindToType("search-instances", function(data)
            local rootInstance = loadstring("return " .. data.root)()
            local instances = rootInstance:QueryDescendants(data.selector)
            
            local limit = data.limit or 50
            local results = {}
            
            for i, instance in ipairs(instances) do
                if i > limit then break end

                table.insert(results, {
                    Name = instance.Name,
                    ClassName = instance.ClassName,
                    InstancePath = instance,
                    DebugId = instance:GetDebugId()
                })
            end
            
            return {
                count = #instances,
                limited = #instances > limit,
                results = results
            }
        end)

        Bridge:BindToType("get-game-info", function(data)
            local productInfo = MarketplaceService:GetProductInfo(game.PlaceId)
            
            return {
                PlaceId = game.PlaceId,
                GameId = game.GameId,
                PlaceVersion = game.PlaceVersion,
                PlaceName = productInfo.Name,
                PlaceDescription = productInfo.Description,
                Creator = productInfo.Creator.Name,
                CreatorType = tostring(productInfo.Creator.CreatorType),
                CreatorId = productInfo.Creator.CreatorTargetId,
                JobId = game.JobId,
                ServerType = RobloxReplicatedStorage.GetServerType:InvokeServer()
            }
        end)

        Bridge:WaitForDisconnect()
    end
