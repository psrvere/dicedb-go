package proto

import (
	"fmt"
	"strconv"
	"strings"
)

var respNil = "(nil)"
var emptyList = "(empty list or set)"
var invalidString = "(error) invalid type"

var simpleStringCommands = map[string]struct{}{
	"AUTH":    {},
	"SELECT":  {},
	"RENAME":  {},
	"RESTORE": {},
	"MSET":    {},
	"SET":     {},
	"PFMERGE": {},
	"FLUSHDB": {},
	"LSET":    {},
	"LTRIM":   {},
	"MIGRATE": {},
}

var intCommands = map[string]struct{}{
	"APPEND":           {},
	"BITCOUNT":         {},
	"BITOP":            {},
	"BITPOS":           {},
	"COPY":             {},
	"DECR":             {},
	"DECRBY":           {},
	"DEL":              {},
	"EXISTS":           {},
	"EXPIRE":           {},
	"EXPIREAT":         {},
	"EXPIRETIME":       {},
	"GEOADD":           {},
	"GETBIT":           {},
	"HDEL":             {},
	"HEXISTS":          {},
	"HINCRBY":          {},
	"HLEN":             {},
	"HSET":             {},
	"HSETNX":           {},
	"HSTRLEN":          {},
	"INCR":             {},
	"INCRBY":           {},
	"LINSERT":          {},
	"LLEN":             {},
	"LPUSH":            {},
	"LPUSHX":           {},
	"LREM":             {},
	"MOVE":             {},
	"MSETNX":           {},
	"PERSIST":          {},
	"PEXPIRE":          {},
	"PEXPIREAT":        {},
	"PFADD":            {},
	"PFCOUNT":          {},
	"PTTL":             {},
	"RENAMENX":         {},
	"RPUSH":            {},
	"RPUSHX":           {},
	"SADD":             {},
	"SCARD":            {},
	"SDIFFSTORE":       {},
	"SETBIT":           {},
	"SETNX":            {},
	"SETRANGE":         {},
	"SINTERSTORE":      {},
	"SISMEMBER":        {},
	"SMOVE":            {},
	"SREM":             {},
	"STRLEN":           {},
	"SUNIONSTORE":      {},
	"TOUCH":            {},
	"TTL":              {},
	"UNLINK":           {},
	"WAIT":             {},
	"ZCARD":            {},
	"ZCOUNT":           {},
	"ZINTERSTORE":      {},
	"ZLEXCOUNT":        {},
	"ZRANK":            {},
	"ZREM":             {},
	"ZREMRANGEBYLEX":   {},
	"ZREMRANGEBYRANK":  {},
	"ZREMRANGEBYSCORE": {},
	"ZREVRANK":         {},
	"ZUNIONSTORE":      {},
}

var bulkStringCommands = map[string]struct{}{
	"ECHO":         {},
	"PING":         {},
	"DUMP":         {},
	"TYPE":         {},
	"GEODIST":      {},
	"HGET":         {},
	"HINCRBYFLOAT": {},
	"GET":          {},
	"GETEX":        {},
	"GETDEL":       {},
	"GETRANGE":     {},
	"GETSET":       {},
	"INCRBYFLOAT":  {},
	"ZSCORE":       {},
	"BLMOVE":       {},
	"BRPOPLPUSH":   {},
	"HMSET":        {},
	"LINDEX":       {},
	"PSETEX":       {},
	"RANDOMKEY":    {},
	"RPOPLPUSH":    {},
	"SETEX":        {},
	"ZINCRBY":      {},
}

var listCommands = map[string]struct{}{
	"HELLO":             {},
	"KEYS":              {},
	"HKEYS":             {},
	"HMGET":             {},
	"HVALS":             {},
	"HRANDFIELD":        {},
	"SMEMBERS":          {},
	"SDIFF":             {},
	"SINTER":            {},
	"MGET":              {},
	"BLPOP":             {},
	"BRPOP":             {},
	"BZPOPMAX":          {},
	"BZPOPMIN":          {},
	"GEOHASH":           {},
	"GEOPOS":            {},
	"GEORADIUS":         {},
	"GEORADIUSBYMEMBER": {},
	"GEOSEARCH":         {},
	"GEOSEARCHSTORE":    {},
	"LPOS":              {},
	"LRANGE":            {},
	"RPOP":              {},
	"SORT":              {},
	"SPOP":              {},
	"SRANDMEMBER":       {},
	"STRALGO":           {},
	"SUNION":            {},
	"ZRANGEBYLEX":       {},
	"ZREVRANGE":         {},
	"ZREVRANGEBYLEX":    {},
	"ZREVRANGEBYSCORE":  {},
}

var membersCommands = map[string]struct{}{
	"ZPOPMAX":       {},
	"ZPOPMIN":       {},
	"ZRANGE":        {},
	"ZRANGEBYSCORE": {},
}

var stringOrIntCommands = map[string]struct{}{
	"OBJECT": {},
	"ZADD":   {},
}

var hashPairCommands = map[string]struct{}{
	"HGETALL": {},
}

var listOrStringCommands = map[string]struct{}{
	"BLPOP":             {},
	"BRPOP":             {},
	"BZPOPMAX":          {},
	"BZPOPMIN":          {},
	"GEORADIUS":         {},
	"GEORADIUSBYMEMBER": {},
	"HRANDFIELD":        {},
	"LPOP":              {},
	"LPOS":              {},
	"RPOP":              {},
	"SORT":              {},
	"SPOP":              {},
	"SRANDMEMBER":       {},
	"STRALGO":           {},
}

var commandRenders = map[string]struct{}{
	"COMMAND":         {},
	"COMMAND GETKEYS": {},
	"COMMAND INFO":    {},
	"COMMAND COUNT":   {},
}

func RenderOutput(cmdName string, cmdArgs interface{}, cmdVal interface{}, cmdErr error) (interface{}, error) {
	fn := getRender(cmdName, cmdArgs)
	if cmdErr != nil {
		return nil, renderError(cmdErr)
	}

	// If we don't have a renderer defined leave it as it is
	if fn == nil {
		return cmdVal, nil
	}

	return fn(cmdVal), nil
}

// getRender retrieves the appropriate callback for the command
func getRender(commandName string, commandArgs interface{}) func(value interface{}) interface{} {
	commandUpper := strings.ToUpper(strings.TrimSpace(commandName))

	// Determine the render method based on command group
	if _, exists := simpleStringCommands[commandUpper]; exists {
		return renderSimpleString
	}
	if _, exists := intCommands[commandUpper]; exists {
		return renderInt
	}
	if _, exists := bulkStringCommands[commandUpper]; exists {
		return renderBulkString
	}
	if _, exists := listCommands[commandUpper]; exists {
		return renderList
	}
	if _, exists := membersCommands[commandUpper]; exists {
		return renderMembers
	}
	if _, exists := stringOrIntCommands[commandUpper]; exists {
		return renderStringOrInt
	}
	if _, exists := hashPairCommands[commandUpper]; exists {
		return renderHashPairs
	}
	if _, exists := listOrStringCommands[commandUpper]; exists {
		return renderListOrString
	}
	if _, exists := commandRenders[commandUpper]; exists {
		if args, ok := commandArgs.([]interface{}); ok && len(args) > 1 {
			if args[1] == "COUNT" {
				return renderInt
			}
		}

		// Default to renderList for all other "COMMAND" options
		return renderList
	}

	return nil
}

func ensureStr(input interface{}) string {
	switch v := input.(type) {
	case string:
		return v
	case error:
		return v.Error()
	default:
		return fmt.Sprintf("%v", v)
	}
}

func renderError(errorMsg error) error {
	err := ensureStr(errorMsg)

	return fmt.Errorf("(error) %s", err)
}

// Render functions
func renderBulkString(value interface{}) interface{} {
	if value == nil {
		return respNil
	}

	result, ok := value.(int64)
	if ok {
		return renderInt(result)
	}

	return fmt.Sprintf("\"%v\"", value)
}

func renderInt(value interface{}) interface{} {
	if value == nil {
		return respNil
	}

	if intValue, ok := value.(int64); ok {
		return fmt.Sprintf("(integer) %d", intValue)
	}

	return invalidString
}

func renderList(value interface{}) interface{} {
	items, ok := value.([]interface{})
	if !ok {
		return invalidString
	}

	var builder strings.Builder
	for i, item := range items {
		if item == nil {
			builder.WriteString(fmt.Sprintf("%d) %v\n", i+1, respNil))
			continue
		}

		strItem := fmt.Sprintf("%v", item)

		if !(strings.HasPrefix(strItem, "\"") && strings.HasSuffix(strItem, "\"")) {
			strItem = fmt.Sprintf("\"%s\"", strItem)
		}

		builder.WriteString(fmt.Sprintf("%d) %s\n", i+1, strItem))
	}
	return builder.String()
}

func renderListOrString(value interface{}) interface{} {
	if items, ok := value.([]interface{}); ok {
		return renderList(items)
	}

	return renderBulkString(value)
}

func renderStringOrInt(value interface{}) interface{} {
	if intValue, ok := value.(int); ok {
		return renderInt(intValue)
	}

	return renderBulkString(value)
}

func renderSimpleString(value interface{}) interface{} {
	if value == nil {
		return respNil
	}

	text := fmt.Sprintf("%v", value)
	return text
}

func renderHashPairs(value interface{}) interface{} {
	items, ok := value.([]interface{})
	if len(items) == 0 {
		return emptyList
	}
	if !ok || len(items)%2 != 0 {
		return "(error) invalid hash pair format"
	}

	var builder strings.Builder
	indexWidth := len(strconv.Itoa(len(items) / 2))
	for i := 0; i < len(items); i += 2 {
		key := fmt.Sprintf("%v", items[i])
		value := fmt.Sprintf("%v", items[i+1])

		// Format the index and key
		indexStr := fmt.Sprintf("%*d) ", indexWidth, i/2+1)
		builder.WriteString(indexStr)
		builder.WriteString(key + "\n")

		if strings.Contains(value, "\"") {
			value = fmt.Sprintf("%q", value)
		}
		valueStr := strings.Repeat(" ", len(indexStr)) + value
		builder.WriteString(valueStr + "\n")
	}
	return builder.String()
}

func renderMembers(value interface{}) interface{} {
	items, ok := value.([]interface{})
	if !ok {
		return invalidString
	}

	var builder strings.Builder
	indexWidth := len(strconv.Itoa(len(items)))
	for i, item := range items {
		member := fmt.Sprintf("%v", item)
		indexStr := fmt.Sprintf("%*d) ", indexWidth, i+1)
		builder.WriteString(indexStr)
		builder.WriteString(member + "\n")
	}

	return builder.String()
}
