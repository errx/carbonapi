package aliasByHash

import (
	"github.com/go-graphite/carbonapi/expr/helper"
	"github.com/go-graphite/carbonapi/expr/interfaces"
	"github.com/go-graphite/carbonapi/expr/types"
	"github.com/go-graphite/carbonapi/pkg/parser"

	"strings"
	"github.com/garyburd/redigo/redis"
)

const DSN = "monitoring01:6379"

type aliasByHash struct {
	interfaces.FunctionBase
}

func GetOrder() interfaces.Order {
	return interfaces.Any
}

// TODO move dsn to config?
func New(configFile string) []interfaces.FunctionMetadata {
	res := make([]interfaces.FunctionMetadata, 0)
	f := &aliasByHash{}
	for _, n := range []string{"aliasByHash"} {
		res = append(res, interfaces.FunctionMetadata{Name: n, F: f})
	}
	return res
}

func (f *aliasByHash) Do(e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData) ([]*types.MetricData, error) {
	args, err := helper.GetSeriesArg(e.Args()[0], from, until, values)
	if err != nil {
		return nil, err
	}

	redisHashName, err := e.GetStringArg(1)
	if err != nil {
		return nil, err
	}

	redisConnection, err := redis.Dial("tcp", DSN)
	if err != nil {
		return args, nil
	}
	defer redisConnection.Close()

	var results []*types.MetricData

	for _, a := range args {
		r := *a
		r.Name = prepareMetric(r.Name)
		redisName, err := redisGetHash(r.Name, redisHashName, redisConnection)
		if err == nil {
			r.Name = redisName
		}
		results = append(results, &r)
	}

	return results, nil
}

func prepareMetric(metric string) string {
	parts := strings.Split(metric, ".")
	lastPart := parts[len(parts)-1]
	prefix := strings.Split(lastPart, ",")[0]
	return strings.Trim(prefix, ")")
}

func redisGetHash(name string, key string, c redis.Conn) (string, error) {
	c.Do("SELECT", 0)
	v, err := c.Do("HGET", key, name)
	return redis.String(v, err)
}

func (f *aliasByHash) Description() map[string]types.FunctionDescription {
	return map[string]types.FunctionDescription{
		"aliasByHash": {
			Description: "Takes a seriesList and applies an alias derived from the remote hash.\n\n.. code-block:: none\n\n  &target=aliasByMetric(carbon.agents.graphite.creates)",
			Function:    "aliasByHash(seriesList)",
			Group:       "Alias",
			Module:      "graphite.render.functions",
			Name:        "aliasByHash",
			Params: []types.FunctionParam{
				{
					Name:     "seriesList",
					Required: true,
					Type:     types.SeriesList,
				},
			},
		},
	}
}
