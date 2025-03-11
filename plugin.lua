local core = require("apisix.core")
local producer = require("resty.kafka.producer")

local plugin_name = "kafka-request-logger"

local _M = {
    version = 0.1,
    priority = 2500,
    name = plugin_name,
    schema = {
        type = "object",
        properties = {
            brokers = {
                type = "array",
                items = { type = "string" },
                minItems = 1
            },
            topic = { type = "string" },
            host = { type = "string" },
            uri = { type = "string" }
        },
        required = { "brokers", "topic", "uri", "host" },
    },
}

function _M.access(conf, ctx)
    -- Extract request details
    local request_body = core.request.get_body() or ""
    local headers = core.request.headers(ctx) or {}
    local req_http_version = ngx.req.http_version()
    local method = core.request.get_method()

    -- Build Kafka message
    local message = {
        method = method,
        headers = headers,
        body = request_body,
    }

    -- Build raw HTTP request format
    local raw_request = method .. " " .. conf.host .. conf.uri .. " HTTP/" .. req_http_version .. "\r\n"

    -- Append headers
    -- headers["host"] = conf.host
    for k, v in pairs(headers) do
        raw_request = raw_request .. k .. ": " .. v .. "\r\n"
    end

    -- Add blank line and body
    raw_request = raw_request .. "\r\n" .. request_body

    -- Convert broker list
    local broker_list = {}
    for _, broker in ipairs(conf.brokers) do
        local host, port = broker:match("([^:]+):(%d+)")
        if host and port then
            table.insert(broker_list, { host = host, port = tonumber(port) })
        end
    end

    -- Create async Kafka producer
    local kafka_producer = producer:new(broker_list, {
        producer_type = "async",
        max_retry = 5,      -- Retry up to 5 times if Kafka fails
        keepalive = true,   -- Reuse Kafka connections
        batch_num = 100,    -- Number of messages per batch
        flush_time = 10     -- Flush batch every 10ms
    })

    -- Send message asynchronously
    local ok, k_err = kafka_producer:send(conf.topic, headers["x-request-id"], raw_request)
    if not ok then
        core.log.error("Failed to send Kafka message: ", k_err)
    else
        core.log.info("Kafka message sent successfully")
    end
end

return _M
