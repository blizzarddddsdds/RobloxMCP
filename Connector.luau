    if not game:IsLoaded() then game.Loaded:Wait() end

    -- Expose executor in _G.ExecutorAPI for MCP-invoked code.
    _G.ExecutorAPI = _G.ExecutorAPI or {}
    if not _G.ExecutorAPI._initialized then
        local genv = getgenv and getgenv() or {}
        for name, value in pairs(genv) do _G.ExecutorAPI[name] = value end
        for name, value in pairs(_G) do
            if not _G.ExecutorAPI[name] then _G.ExecutorAPI[name] = value end
        end
        -- Decompile: use getgenv().decompile when present (e.g. custom HTTP decompiler).
        -- This wrapper lets you set getgenv().decompile before OR after the connector runs.
        _G.ExecutorAPI.decompile = function(scr)
            local g = getgenv and getgenv()
            local d = g and g.decompile
            if type(d) ~= "function" then
                error("decompile not available (set getgenv().decompile or use an executor that provides it)")
            end
            return d(scr)
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
    local BridgeSecret = (function()
        local g = getgenv and getgenv()
        if g and g.BridgeSecret and type(g.BridgeSecret) == "string" then return g.BridgeSecret end
        return nil
    end)()
    local LuaEncode
    do
        local ok, fn = pcall(function()
            return loadstring(game:HttpGet("https://raw.githubusercontent.com/chadhyatt/LuaEncode/refs/heads/master/src/LuaEncode.lua"))()
        end)
        if ok and type(fn) == "function" then
            LuaEncode = fn
        else
            LuaEncode = nil
            warn("[Roblox MCP connector] LuaEncode load failed; responses will use JSONEncode fallback.")
        end
    end
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

            local encoded = HttpService:JSONEncode(output)
        
            return {
                output = encoded,
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
            if BridgeSecret and BridgeSecret ~= "" then
                pcall(function()
                    self.WebSocket:Send(HttpService:JSONEncode({ type = "auth", secret = BridgeSecret }))
                end)
            end
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
                        local headers = {}
                        if BridgeSecret and BridgeSecret ~= "" then
                            headers["X-Bridge-Secret"] = BridgeSecret
                        end
                        local response = request({
                            Url = "http://" .. BridgeURL .. "/poll",
                            Method = "GET",
                            Headers = next(headers) and headers or nil
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
                            
                            local respondHeaders = { ["Content-Type"] = "application/json" }
                            if BridgeSecret and BridgeSecret ~= "" then
                                respondHeaders["X-Bridge-Secret"] = BridgeSecret
                            end
                            request({
                                Url = "http://" .. BridgeURL .. "/respond",
                                Method = "POST",
                                Headers = respondHeaders,
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
            -- Guard: loadstring expects a string; avoid "string expected, got Instance" from bad payloads.
            if data.source == nil then
                return { error = "execute: source is required" }
            end
            if type(data.source) ~= "string" then
                return { error = "execute: source must be a string, got " .. type(data.source) }
            end
            local src = data.source
            local fn, err = loadstring(src)
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
            if data.source == nil then
                return { error = "get-data-by-code: source is required" }
            end
            if type(data.source) ~= "string" then
                return { error = "get-data-by-code: source must be a string, got " .. type(data.source) }
            end
            local src = data.source
            local fn, err = loadstring(src)
            if not fn then
                return { error = "Failed to compile code: " .. tostring(err) }
            end
            if setfenv then setfenv(fn, execEnv) end
            local ok, result = pcall(fn)
            if not ok then
                return { error = result or "Unknown error during execution" }
            end
            return result
        end)

        Bridge:BindToType("search-instances", function(data)
            local rootInstance
            if type(data.rootParts) == "table" and #data.rootParts > 0 then
                -- Path-walk from game (avoids loadstring; supports Players.LocalPlayer)
                local inst = game
                for _, p in ipairs(data.rootParts) do
                    if inst.ClassName == "Players" and p == "LocalPlayer" then
                        inst = inst.LocalPlayer
                    elseif inst.ClassName == "Player" and p == "Character" then
                        inst = inst.Character
                    else
                        inst = inst:FindFirstChild(p)
                    end
                    if not inst then
                        return { error = "Root not found (path walk)", success = false }
                    end
                end
                rootInstance = inst
            else
                local ok, res = pcall(function()
                    return loadstring("return " .. (data.root or "game"))()
                end)
                if not ok or not res then
                    return { error = "Root not found or invalid root expression: " .. tostring(data.root or "game"), success = false }
                end
                rootInstance = res
            end
            local ok, instances = pcall(function()
                return rootInstance:QueryDescendants(data.selector or "")
            end)
            if not ok or not instances then
                return { error = "QueryDescendants failed: " .. tostring(instances), success = false }
            end
            local limit = data.limit or 50
            local results = {}

            for i, instance in ipairs(instances) do
                if i > limit then break end

                table.insert(results, {
                    Name = instance.Name,
                    ClassName = instance.ClassName,
                    InstancePath = instance,
                    path = instance:GetFullName(),
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
            local productInfo
            local okProduct, errProduct = pcall(function()
                productInfo = MarketplaceService:GetProductInfo(game.PlaceId)
            end)
            if not okProduct or not productInfo then
                return { error = "GetProductInfo failed: " .. tostring(errProduct or "unknown") }
            end
            local serverType = "Unknown"
            local okServer, st = pcall(function()
                return RobloxReplicatedStorage.GetServerType:InvokeServer()
            end)
            if okServer and st ~= nil then serverType = tostring(st) end
            local creator = productInfo.Creator or {}
            local creatorId = creator.Id or creator.CreatorTargetId
            return {
                PlaceId = game.PlaceId,
                GameId = game.GameId,
                PlaceVersion = game.PlaceVersion,
                PlaceName = productInfo.Name,
                PlaceDescription = productInfo.Description,
                Creator = creator.Name or "",
                CreatorType = tostring(creator.CreatorType or ""),
                CreatorId = creatorId,
                JobId = game.JobId,
                ServerType = serverType
            }
        end)

        Bridge:WaitForDisconnect()
    end
