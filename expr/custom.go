package expr

import (
	"strings"
	"github.com/garyburd/redigo/redis"
)

func redisGetHash(name string, key string, db int, c redis.Conn) (string, error) {
	c.Do("SELECT", db)
	v, err := c.Do("HGET", name, key)
	return redis.String(v, err)
}

func prepareMetric(metric string) string {
	parts := strings.Split(metric, ".")
	lastPart := parts[len(parts)-1]
	prefix := strings.Split(lastPart, ",")[0]
	return strings.Trim(prefix, ")")
}

func prepareKubeMetric(metric string) (string, string, string) {
	parts := strings.Split(metric, ".")
	suffix, name, item, key := "", "", "", ""
	if len(parts) > 8 {
		suffix = "." + strings.Join(parts[8:], ".")
	}
	if len(parts) == 8 {
		suffix = "." + parts[7]
	}
	if len(parts) > 7 {
		item = parts[7]
	}
	if len(parts) > 6 {
		name = parts[6]
	}
	if len(parts) > 4 {
		//parts[4] = node
		key = parts[4] + "_" + item
	}
	return name, key, suffix

}

func aliasByHash(metric string, redisHashName string, conn redis.Conn) string {
	if redisHashName == "kube" {
		name, key, suffix := prepareKubeMetric(metric)
		redisName, err := redisGetHash(name, key, 2, conn)
		if err != nil {
			return metric
		}
		return redisName + suffix
	} else {
		key := prepareMetric(metric)
		redisName, err := redisGetHash(redisHashName, key, 0, conn)
		if err != nil {
			return metric
		}
		return redisName
	}
}
