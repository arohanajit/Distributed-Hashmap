-- wrk.lua - Load testing script for Distributed Hashmap
-- Usage: wrk -t8 -c100 -d30s -s wrk.lua http://localhost:8081

-- Initialize random seed
math.randomseed(os.time())

-- Configuration
local key_prefix = "loadtest-key-"
local value_prefix = "loadtest-value-"
local key_count = 10000  -- Number of unique keys to use
local value_size = 100   -- Size of each value in bytes
local read_write_ratio = 0.8  -- 80% reads, 20% writes

-- Generate a random string of specified length
function random_string(length)
    local chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    local str = ""
    for i = 1, length do
        local rand = math.random(#chars)
        str = str .. string.sub(chars, rand, rand)
    end
    return str
end

-- Generate a random key
function random_key()
    return key_prefix .. math.random(key_count)
end

-- Generate a random value
function random_value()
    return value_prefix .. random_string(value_size - #value_prefix)
end

-- Setup function - called once when the thread starts
function setup(thread)
    thread:set("id", thread.id)
    print("Thread " .. thread.id .. " started")
end

-- Initialize function - called once per connection
function init(args)
    -- Warm up the system by pre-populating some keys
    if wrk.thread:get("id") == 1 then
        print("Pre-populating keys...")
        for i = 1, 100 do
            local key = key_prefix .. i
            local value = value_prefix .. random_string(value_size - #value_prefix)
            local path = "/kv/" .. key
            local req = wrk.format("PUT", path, nil, value)
            wrk.request(req)
            if i % 10 == 0 then
                print("Pre-populated " .. i .. " keys")
            end
        end
        print("Pre-population complete")
    end
end

-- Request function - called for each request
function request()
    -- Determine if this is a read or write based on the ratio
    local is_read = math.random() < read_write_ratio
    
    if is_read then
        -- GET request
        local key = random_key()
        local path = "/kv/" .. key
        return wrk.format("GET", path)
    else
        -- PUT request
        local key = random_key()
        local value = random_value()
        local path = "/kv/" .. key
        return wrk.format("PUT", path, nil, value)
    end
end

-- Response function - called with the HTTP response
function response(status, headers, body)
    -- Track status codes
    if status ~= 200 then
        wrk.thread:get("errors")[status] = (wrk.thread:get("errors")[status] or 0) + 1
    end
end

-- Thread initialization
function setup(thread)
    thread:set("id", thread.id)
    thread:set("errors", {})
    print("Thread " .. thread.id .. " initialized")
end

-- Thread done function - called when the thread is done
function done(summary, latency, requests)
    local errors = wrk.thread:get("errors")
    local total_errors = 0
    
    for status, count in pairs(errors) do
        total_errors = total_errors + count
        print("Status " .. status .. ": " .. count .. " errors")
    end
    
    print("Thread " .. wrk.thread:get("id") .. " completed")
    print("  Errors: " .. total_errors .. " / " .. summary.requests)
    print("  Latency (ms):")
    print("    Min: " .. latency.min / 1000)
    print("    Max: " .. latency.max / 1000)
    print("    Mean: " .. latency.mean / 1000)
    print("    Stdev: " .. latency.stdev / 1000)
    print("    P50: " .. latency:percentile(50) / 1000)
    print("    P90: " .. latency:percentile(90) / 1000)
    print("    P95: " .. latency:percentile(95) / 1000)
    print("    P99: " .. latency:percentile(99) / 1000)
end 