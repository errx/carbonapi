package baselines

import (
	"fmt"
	"math"

	"github.com/go-graphite/carbonapi/expr/helper"
	"github.com/go-graphite/carbonapi/expr/interfaces"
	"github.com/go-graphite/carbonapi/expr/types"
	"github.com/go-graphite/carbonapi/pkg/parser"
)

type baselines struct {
	interfaces.FunctionBase
}

func GetOrder() interfaces.Order {
	return interfaces.Any
}

func New(configFile string) []interfaces.FunctionMetadata {
	res := make([]interfaces.FunctionMetadata, 0)
	f := &baselines{}
	functions := []string{"baseline", "baselineAberration"}
	for _, n := range functions {
		res = append(res, interfaces.FunctionMetadata{Name: n, F: f})
	}
	return res
}

func (f *baselines) Do(e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData) ([]*types.MetricData, error) {
	isAberration := false
	if e.Target() == "baselineAberration" {
		isAberration = true
	}

	unit, err := e.GetIntervalArg(1, -1)
	if err != nil {
		return nil, err
	}

	start, err := e.GetIntArg(2)
	if err != nil {
		return nil, err
	}

	end, err := e.GetIntArg(3)
	if err != nil {
		return nil, err
	}

	maxAbsentPercent, err := e.GetFloatArgDefault(4, math.NaN())
	if err != nil {
		return nil, err
	}
	minAvgLimit, err := e.GetFloatArgDefault(5, math.NaN())
	if err != nil {
		return nil, err
	}

	current := make(map[string]*types.MetricData)
	arg, _ := helper.GetSeriesArg(e.Args()[0], from, until, values)
	for _, a := range arg {
		current[a.Name] = a
	}

	var results []*types.MetricData
	groups := make(map[string][]*types.MetricData)
	for i := int32(start); i < int32(end); i++ {
		if i == 0 {
			continue
		}
		offs := i * unit
		arg, _ := helper.GetSeriesArg(e.Args()[0], from+offs, until+offs, values)
		for _, a := range arg {
			r := *a
			if _, ok := current[r.Name]; ok || !isAberration {
				r.StartTime = a.StartTime - offs
				r.StopTime = a.StopTime - offs
				groups[r.Name] = append(groups[r.Name], &r)
			}
		}
	}

	for name, args := range groups {
		r := *args[0]
		if isAberration {
			r.Name = fmt.Sprintf("baselineAberration(%s)", name)
		} else {
			r.Name = fmt.Sprintf("baseline(%s)", name)
		}
		r.Values = make([]float64, len(args[0].Values))
		r.IsAbsent = make([]bool, len(args[0].Values))

		tmp := make([][]float64, len(args[0].Values)) // number of points
		lengths := make([]int, len(args[0].Values))   // number of points with data
		atLeastOne := make([]bool, len(args[0].Values))
		for _, arg := range args {
			for i, v := range arg.Values {
				if arg.IsAbsent[i] {
					continue
				}
				atLeastOne[i] = true
				tmp[i] = append(tmp[i], v)
				lengths[i]++
			}
		}

		totalSum := 0.0
		totalNotAbsent := 0
		totalCnt := len(r.Values)

		for i, v := range atLeastOne {
			if v {
				r.Values[i] = helper.Percentile(tmp[i][0:lengths[i]], 50, true)
				totalSum += r.Values[i]
				totalNotAbsent++
				if isAberration {
					if current[name].IsAbsent[i] {
						r.IsAbsent[i] = true
					}
					if !r.IsAbsent[i] && r.Values[i] != 0 {
						r.Values[i] = current[name].Values[i] / r.Values[i]
					}
				}
			} else {
				r.IsAbsent[i] = true
			}
		}

		if !math.IsNaN(maxAbsentPercent) {
			absentPercent := float64(100*(totalCnt-totalNotAbsent)) / float64(totalCnt)
			if absentPercent > maxAbsentPercent {
				continue
			}
		}

		if !math.IsNaN(minAvgLimit) && (totalNotAbsent != 0) {
			avg := totalSum / float64(totalNotAbsent)
			if avg < minAvgLimit {
				continue
			}
		}

		results = append(results, &r)
	}

	return results, nil
}

const baselineDescr = `Строит baseline, аргументы как и у timestack. берет для каждой метрики массив сдвинутых метрик и для каждой точки считает медиану.
например: baseline(metric, "1w", 1, 4) - возьмет 4 значения метрики с интервалом 1 неделя и для каждой точки возьмет медиану
необязательные параметры:
   maxAbsentPercent - если процент пустых точек больше этого значения, то baseline не строится
   minAvg - если среднее метрики ниже этого значения, то baseline не строится
`

const baselineAberrationDescr = `Отклонение от бейзлайна в долях. например, если текущее значение превысило бейзлайн на 10%, то значение метрики будет 1.1`

func (f *baselines) Description() map[string]types.FunctionDescription {
	return map[string]types.FunctionDescription{
		"baseline": {
			Description: baselineDescr,
			Function:    "baseline(seriesList, timeShiftUnit, timeShiftStart, timeShiftEnd, [maxAbsentPercent, minAvg])",
			Group:       "Calculate",
			Module:      "graphite.render.functions",
			Name:        "baseline",
			Params: []types.FunctionParam{
				{
					Name:     "seriesList",
					Required: true,
					Type:     types.SeriesList,
				},
				{
					Default: types.NewSuggestion("1d"),
					Name:    "timeShiftUnit",
					Suggestions: types.NewSuggestions(
						"1h",
						"6h",
						"12h",
						"1d",
						"2d",
						"7d",
						"14d",
						"30d",
					),
					Type: types.Interval,
				},
				{
					Default: types.NewSuggestion(0),
					Name:    "timeShiftStart",
					Type:    types.Integer,
				},
				{
					Default: types.NewSuggestion(7),
					Name:    "timeShiftEnd",
					Type:    types.Integer,
				},
			},
		},
		"baselineAberration": {
			Description: baselineAberrationDescr,
			Function:    "baselineAberration(seriesList, timeShiftUnit, timeShiftStart, timeShiftEnd, [maxAbsentPercent, minAvg])",
			Group:       "Calculate",
			Module:      "graphite.render.functions",
			Name:        "baselineAberration",
			Params: []types.FunctionParam{
				{
					Default: types.NewSuggestion("1d"),
					Name:    "timeShiftUnit",
					Suggestions: types.NewSuggestions(
						"1h",
						"6h",
						"12h",
						"1d",
						"2d",
						"7d",
						"14d",
						"30d",
					),
					Type: types.Interval,
				},
				{
					Default: types.NewSuggestion(0),
					Name:    "timeShiftStart",
					Type:    types.Integer,
				},
				{
					Default: types.NewSuggestion(7),
					Name:    "timeShiftEnd",
					Type:    types.Integer,
				},
			},
		},
	}
}
