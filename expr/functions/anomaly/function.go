package anomaly

import (
	"github.com/go-graphite/carbonapi/expr/interfaces"
	"github.com/go-graphite/carbonapi/expr/types"
	"github.com/go-graphite/carbonapi/pkg/parser"
	"strings"
	"fmt"
	"math"
	"github.com/go-graphite/carbonapi/expr/helper"
)

const anomalyPrefix = "resources.monitoring.anomaly_detector."

type anomaly struct {
	interfaces.FunctionBase
}

func GetOrder() interfaces.Order {
	return interfaces.Any
}

func New(configFile string) []interfaces.FunctionMetadata {
	res := make([]interfaces.FunctionMetadata, 0)
	f := &anomaly{}
	functions := []string{"anomaly"}
	for _, n := range functions {
		res = append(res, interfaces.FunctionMetadata{Name: n, F: f})
	}
	return res
}

func (f *anomaly) Do(e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData) ([]*types.MetricData, error) {
		arg, err := helper.GetSeriesArg(e.Args()[0], from, until, values)
		if err != nil {
			return nil, err
		}
		joinType, err := e.GetStringNamedOrPosArgDefault("type", 1, "all")
		if err != nil {
			return nil, err
		}
		threshold, err := e.GetFloatNamedOrPosArgDefault("threshold", 2, math.NaN())
		if err != nil {
			return nil, err
		}

		offs, err := e.GetIntervalArgDefault(3, 1, -1)

		if err != nil {
			return nil, err
		}
		// extract anomaly metrics
		nname := anomalyPrefix + e.Args()[0].Target()
		anomReq := parser.MetricRequest{Metric: nname, From: from, Until: until}
		anomalyData, ok := values[anomReq]

		anomalyMap := make(map[string]*types.MetricData)
		if ok {
			for _, d := range anomalyData {
				if offs > 0 {
					offPoints := (d.StopTime - offs - d.StartTime) / d.StepTime
					if offPoints < 0 {
						offPoints = 0
					}
					exclude := true
					for _, v := range d.IsAbsent[offPoints:] {
						if !v {
							exclude = false
							break
						}
					}
					if exclude {
						continue
					}
				}
				name := strings.TrimPrefix(d.Name, anomalyPrefix)
				d.Name = fmt.Sprintf("[anomaly] %s", name)
				anomalyMap[name] = d
			}
		}

		var results []*types.MetricData
		for _, a := range arg {
			exclude := false
			if !math.IsNaN(threshold) {
				exclude = true
				for i, v := range a.Values {
					if !a.IsAbsent[i] && v > threshold {
						exclude = false
						break
					}
				}
			}
			if exclude {
				continue
			}
			anomaly, hasAnomaly := anomalyMap[a.Name]
			// include all metrics & anomalies
			if joinType == "all" {
				results = append(results, a)
				if hasAnomaly {
					results = append(results, anomaly)
				}
			} else if joinType == "with_anomalies_only" && hasAnomaly {
				results = append(results, a)
				results = append(results, anomaly)
			} else if joinType == "only_anomalies" && hasAnomaly {
				results = append(results, anomaly)
			}
		}
		return results, nil
}

const descr = `Принимает на вход метрику (или массив метрик), выводит помимо метрик их аномальные точки.
Параметр type, принимает значения:
all - выводить все метрики и аномалии (дефолт)
with_anomalies_only - выводить только метрики с аномалиями + аномалии
only_anomalies - выводить только аномалии
`

func (f *anomaly) Description() map[string]types.FunctionDescription {
	return map[string]types.FunctionDescription{
		"anomaly": {
			Description: descr,
			Function:    "anomaly(seriesList, type='all')",
			Group:       "Special",
			Module:      "graphite.render.functions",
			Name:        "anomaly",
			Params: []types.FunctionParam{
				{
					Name:     "seriesList",
					Required: true,
					Type:     types.SeriesList,
				},
				{
					Name: "type",
					Options: []string{
						"all",
						"with_anomalies_only",
						"only_anomalies",
					},
					Required: false,
					Type:     types.String,
					Default: types.NewSuggestion("all"),
				},
				// TODO add offset & threshold
			},
		},
	}
}
