package seriesList

import (
	"fmt"
	"math"
	"strconv"

	"github.com/go-graphite/carbonapi/expr/helper"
	"github.com/go-graphite/carbonapi/expr/interfaces"
	"github.com/go-graphite/carbonapi/expr/types"
	"github.com/go-graphite/carbonapi/pkg/parser"
)

type seriesList struct {
	interfaces.FunctionBase
}

func GetOrder() interfaces.Order {
	return interfaces.Any
}

func New(configFile string) []interfaces.FunctionMetadata {
	res := make([]interfaces.FunctionMetadata, 0)
	f := &seriesList{}
	functions := []string{"divideSeriesLists", "diffSeriesLists", "multiplySeriesLists", "powSeriesLists"}
	for _, n := range functions {
		res = append(res, interfaces.FunctionMetadata{Name: n, F: f})
	}
	return res
}

func (f *seriesList) Do(e parser.Expr, from, until int32, values map[parser.MetricRequest][]*types.MetricData) ([]*types.MetricData, error) {
	defaultValue, err := e.GetFloatNamedOrPosArgDefault("default", 3, math.NaN())
	if err != nil {
		return nil, err
	}

	useConstant := false
	useDenom := false

	numerators, err := helper.GetSeriesArg(e.Args()[0], from, until, values)
	if err != nil {
		if err == parser.ErrSeriesDoesNotExist && !math.IsNaN(defaultValue) {
			useConstant = true
			useDenom = true
		} else {
			return nil, err
		}
	}
	if len(numerators) == 0 {
		if !math.IsNaN(defaultValue) {
			useConstant = true
			useDenom = true
		} else {
			return nil, nil
		}
	}

	denominators, err := helper.GetSeriesArg(e.Args()[1], from, until, values)
	if err != nil {
		if err == parser.ErrSeriesDoesNotExist && !math.IsNaN(defaultValue) && !useConstant {
			useConstant = true
		} else {
			return nil, err
		}
	}
	if len(denominators) == 0 {
		if !math.IsNaN(defaultValue) && !useConstant {
			useConstant = true
		} else {
			return nil, nil
		}
	}

	sizeMatch := len(denominators) == len(numerators) || len(denominators) == 1
	useMatching, err := e.GetBoolNamedOrPosArgDefault("matching", 2, !useConstant && !sizeMatch)
	if err != nil {
		return nil, err
	}

	var results []*types.MetricData
	functionName := e.Target()[:len(e.Target())-len("Lists")]

	var compute func(l, r float64) float64

	switch e.Target() {
	case "divideSeriesLists":
		compute = func(l, r float64) float64 { return l / r }
	case "multiplySeriesLists":
		compute = func(l, r float64) float64 { return l * r }
	case "diffSeriesLists":
		compute = func(l, r float64) float64 { return l - r }
	case "powSeriesLists":
		compute = func(l, r float64) float64 { return math.Pow(l, r) }
	}

	if useConstant {
		var single []*types.MetricData
		if useDenom {
			single = denominators
		} else {
			single = numerators
		}
		for _, s := range single {
			r := *s
			r.Name = fmt.Sprintf("%s(%s,%s)", functionName, s.Name, s.Name)
			r.Values = make([]float64, len(s.Values))
			r.IsAbsent = make([]bool, len(s.Values))
			for i, v := range s.Values {
				if s.IsAbsent[i] {
					r.IsAbsent[i] = true
					continue
				}

				if e.Target() == "divideSeriesLists" {
					if (useDenom && v == 0) || (!useDenom && defaultValue == 0) {
						r.IsAbsent[i] = true
						continue
					}
				}
				if useDenom {
					r.Values[i] = compute(defaultValue, v)
				} else {
					r.Values[i] = compute(v, defaultValue)
				}

			}
			results = append(results, &r)
		}
		return results, nil
	}

	var denomMap map[string]*types.MetricData
	if useMatching {
		denomMap = make(map[string]*types.MetricData, len(denominators))
		for _, s := range denominators {
			denomMap[s.Name] = s
		}
	}

	var denominator *types.MetricData

	for i, numerator := range numerators {
		pairFound := false
		if useMatching {
			denominator, pairFound = denomMap[numerator.Name]
			if !pairFound && math.IsNaN(defaultValue) {
				continue
			}
		} else {
			if len(denominators) == 1 {
				denominator = denominators[0]
			} else {
				denominator = denominators[i]
			}
		}
		if pairFound {
			if numerator.StepTime != denominator.StepTime || len(numerator.Values) != len(denominator.Values) {
				return nil, fmt.Errorf("series %s must have the same length as %s", numerator.Name, denominator.Name)
			}
		}
		r := *numerator
		var denomName string
		if pairFound {
			denomName = denominator.Name
		} else {
			denomName = strconv.FormatFloat(defaultValue, 'f', -1, 64)
		}
		r.Name = fmt.Sprintf("%s(%s,%s)", functionName, numerator.Name, denomName)
		r.Values = make([]float64, len(numerator.Values))
		r.IsAbsent = make([]bool, len(numerator.Values))

		for i, v := range numerator.Values {
			denomIsAbsent := pairFound && denominator.IsAbsent[i]
			if numerator.IsAbsent[i] || denomIsAbsent {
				r.IsAbsent[i] = true
				continue
			}

			denomValue := defaultValue
			if pairFound {
				denomValue = denominator.Values[i]
			}

			switch e.Target() {
			case "divideSeriesLists":
				if denominator.Values[i] == 0 {
					r.IsAbsent[i] = true
					continue
				}
				r.Values[i] = compute(v, denomValue)
			default:
				r.Values[i] = compute(v, denomValue)
			}
		}
		results = append(results, &r)
	}
	return results, nil
}

// Description is auto-generated description, based on output of https://github.com/graphite-project/graphite-web
// TODO add custom params
func (f *seriesList) Description() map[string]types.FunctionDescription {
	return map[string]types.FunctionDescription{
		"divideSeriesLists": {
			Description: "Iterates over a two lists and divides list1[0} by list2[0}, list1[1} by list2[1} and so on.\nThe lists need to be the same length",
			Function:    "divideSeriesLists(dividendSeriesList, divisorSeriesList)",
			Group:       "Combine",
			Module:      "graphite.render.functions",
			Name:        "divideSeriesLists",
			Params: []types.FunctionParam{
				{
					Name:     "dividendSeriesList",
					Required: true,
					Type:     types.SeriesList,
				},
				{
					Name:     "divisorSeriesList",
					Required: true,
					Type:     types.SeriesList,
				},
			},
		},
		"diffSeriesLists": {
			Description: "Iterates over a two lists and substracts list1[0} by list2[0}, list1[1} by list2[1} and so on.\nThe lists need to be the same length",
			Function:    "diffSeriesLists(firstSeriesList, secondSeriesList)",
			Group:       "Combine",
			Module:      "graphite.render.functions.custom",
			Name:        "diffSeriesLists",
			Params: []types.FunctionParam{
				{
					Name:     "firstSeriesList",
					Required: true,
					Type:     types.SeriesList,
				},
				{
					Name:     "secondSeriesList",
					Required: true,
					Type:     types.SeriesList,
				},
			},
		},
		"multiplySeriesLists": {
			Description: "Iterates over a two lists and multiplies list1[0} by list2[0}, list1[1} by list2[1} and so on.\nThe lists need to be the same length",
			Function:    "multiplySeriesLists(sourceSeriesList, factorSeriesList)",
			Group:       "Combine",
			Module:      "graphite.render.functions.custom",
			Name:        "multiplySeriesLists",
			Params: []types.FunctionParam{
				{
					Name:     "sourceSeriesList",
					Required: true,
					Type:     types.SeriesList,
				},
				{
					Name:     "factorSeriesList",
					Required: true,
					Type:     types.SeriesList,
				},
			},
		},
		"powSeriesLists": {
			Description: "Iterates over a two lists and do list1[0} in power of list2[0}, list1[1} in power of  list2[1} and so on.\nThe lists need to be the same length",
			Function:    "powSeriesLists(sourceSeriesList, factorSeriesList)",
			Group:       "Combine",
			Module:      "graphite.render.functions.custom",
			Name:        "powSeriesLists",
			Params: []types.FunctionParam{
				{
					Name:     "sourceSeriesList",
					Required: true,
					Type:     types.SeriesList,
				},
				{
					Name:     "factorSeriesList",
					Required: true,
					Type:     types.SeriesList,
				},
			},
		},
	}
}
