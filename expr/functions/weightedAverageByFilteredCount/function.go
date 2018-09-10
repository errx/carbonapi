package weightedAverageByFilteredCount

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/go-graphite/carbonapi/expr/helper"
	"github.com/go-graphite/carbonapi/expr/interfaces"
	"github.com/go-graphite/carbonapi/expr/types"
	"github.com/go-graphite/carbonapi/pkg/parser"
	"github.com/go-graphite/carbonapi/util/statsd"
)

type weightedAverageByFilteredCount struct {
	interfaces.FunctionBase
}

func GetOrder() interfaces.Order {
	return interfaces.Any
}

func New(configFile string) []interfaces.FunctionMetadata {
	res := make([]interfaces.FunctionMetadata, 0)
	f := &weightedAverageByFilteredCount{}
	functions := []string{"weightedAverageByFilteredCount"}
	for _, n := range functions {
		res = append(res, interfaces.FunctionMetadata{Name: n, F: f})
	}
	return res
}

func trimmedSuffixSeriesMap(series []*types.MetricData, suffix string) map[string]*types.MetricData {
	seriesMap := make(map[string]*types.MetricData, len(series))
	for _, s := range series {
		metric := helper.ExtractMetric(s.Name)
		name := strings.TrimSuffix(metric, suffix)
		seriesMap[name] = s
	}
	return seriesMap
}

func filterSeries(series []*types.MetricData, threshold float64) []*types.MetricData {
	filtered := make([]*types.MetricData, 0, len(series))
	for _, s := range series {
		skip := true
		for i, v := range s.Values {
			if !s.IsAbsent[i] {
				if v > threshold {
					skip = false
					break
				}
			}
		}
		if !skip {
			filtered = append(filtered, s)
		}
	}
	return filtered
}

func sumSeries(series []*types.MetricData) []float64 {
	sumCount := make([]float64, len(series[0].Values))
	for _, s := range series {
		for i, v := range s.Values {
			if !s.IsAbsent[i] {
				sumCount[i] += v
			}
		}
	}
	return sumCount
}

func aggKey(name string, fields []int) string {
	metric := helper.ExtractMetric(name)
	nodes := strings.Split(metric, ".")
	nodeKey := make([]string, 0, len(fields))
	for _, f := range fields {
		nodeKey = append(nodeKey, nodes[f])
	}
	return strings.Join(nodeKey, ".")
}

func groupSeries(series []*types.MetricData, fields []int) map[string][]*types.MetricData {
	groupMap := make(map[string][]*types.MetricData)
	for _, s := range series {
		node := aggKey(s.Name, fields)
		groupMap[node] = append(groupMap[node], s)
	}
	return groupMap

}

// weightedAverageByFilteredCount(seriesLis, threshold, *nodes)
func (f *weightedAverageByFilteredCount) Do(e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData) ([]*types.MetricData, error) {
	seriesList, err := helper.GetSeriesArg(e.Args()[0], from, until, values)
	if err != nil {
		return nil, err
	}
	if len(seriesList) == 0 {
		return nil, nil
	}

	threshold, err := e.GetFloatArg(1)
	if err != nil {
		return nil, err
	}

	fields, err := e.GetIntArgs(2)
	if err != nil {
		return nil, err
	}
	e0 := e.Args()[0]
	for e0.IsFunc() {
		e0 = e0.Args()[0]
	}
	target := e0.Target()

	suffix := statsd.GetSuffix(target)
	cntTarget := strings.TrimSuffix(target, suffix) + statsd.CountSuffix

	cntKey := parser.MetricRequest{Metric: cntTarget, From: from, Until: until}
	seriesListCount, ok := values[cntKey]
	if !ok {
		return nil, errors.New(".count metric not found")
	}

	if threshold > 0 {
		seriesListCount = filterSeries(seriesListCount, threshold)
	}

	if len(seriesListCount) == 0 {
		return nil, errors.New(".count metric not found/or filtered")
	}

	seriesGroupMap := groupSeries(seriesList, fields)
	seriesCountGroupMap := groupSeries(seriesListCount, fields)

	result := make([]*types.MetricData, 0)

	for group, series := range seriesGroupMap {
		if len(series) == 0 {
			continue
		}
		seriesCount, ok := seriesCountGroupMap[group]
		if !ok {
			continue
		}

		r, err := do1(series, seriesCount, suffix, group, threshold)
		if err != nil {
			return nil, err
		}
		result = append(result, r)
	}

	return result, nil
}

func do1(seriesList, seriesListCount []*types.MetricData, suffix, group string, threshold float64) (*types.MetricData, error) {
	seriesMap := trimmedSuffixSeriesMap(seriesList, suffix)
	seriesCountMap := trimmedSuffixSeriesMap(seriesListCount, statsd.CountSuffix)
	sumCount := sumSeries(seriesListCount)

	r := *seriesList[0]
	r.Name = fmt.Sprintf("weightedAverageByFilteredCount(%s, %s)", group, strconv.FormatFloat(threshold, 'f', -1, 64))
	r.Values = make([]float64, len(r.Values))
	r.IsAbsent = make([]bool, len(r.IsAbsent))

	for i := 0; i < len(seriesList[0].Values); i++ {
		r.IsAbsent[i] = true
		for name, s := range seriesMap {
			cnt, ok := seriesCountMap[name]
			if !ok {
				continue
			}
			if cnt.StepTime != s.StepTime {
				return nil, fmt.Errorf("different stepTimes: %d and %d", cnt.StepTime, s.StepTime)
			}

			if len(cnt.Values) != len(s.Values) {
				return nil, fmt.Errorf("different series lengths")
			}

			if !(cnt.IsAbsent[i] || s.IsAbsent[i]) {
				r.Values[i] += cnt.Values[i] * s.Values[i]
				r.IsAbsent[i] = false
			}
		}
		r.Values[i] /= sumCount[i]
	}
	return &r, nil
}

func (f *weightedAverageByFilteredCount) Description() map[string]types.FunctionDescription {
	return map[string]types.FunctionDescription{
		"weightedAverageByFilteredCount": {
			Function: "weightedAverageByFilteredCount(seriesList, threshold, *nodes)",
			Group:    "Combine",
			Name:     "weightedAverageByFilteredCount",
			Module:   "graphite.render.functions",
			Params: []types.FunctionParam{
				{
					Required: true,
					Type:     types.SeriesList,
					Name:     "seriesList",
				},
				{
					Name:     "threshold",
					Required: true,
					Type:     types.Float,
					Default:  types.NewSuggestion(0.0),
				},
				{
					Multiple: true,
					Name:     "position",
					Type:     types.Node,
				},
			},
			Description: "Works for statsd 'ms' aggregated metrics. Takes a serieslist, threshold and calculate weighted average by pair .count metric to subgroups within as defined by multiple nodes\n\n.. code-block:: none\n\n  &target=weightedAverageByFilteredCount(ganglia.server*.*.cpu.load*.median,10,1,4)",
		},
	}
}
