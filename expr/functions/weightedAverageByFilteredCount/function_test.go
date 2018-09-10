package weightedAverageByFilteredCount

import (
	"math"
	"testing"
	"time"

	"github.com/go-graphite/carbonapi/expr/helper"
	"github.com/go-graphite/carbonapi/expr/metadata"
	"github.com/go-graphite/carbonapi/expr/types"
	"github.com/go-graphite/carbonapi/pkg/parser"
	th "github.com/go-graphite/carbonapi/tests"
)

func init() {
	md := New("")
	evaluator := th.EvaluatorFromFunc(md[0].F)
	metadata.SetEvaluator(evaluator)
	helper.SetEvaluator(evaluator)
	for _, m := range md {
		metadata.RegisterFunction(m.Name, m.F)
	}
}

func TestWeightedAverageByFilteredCount(t *testing.T) {
	now32 := int32(time.Now().Unix())

	tests := []th.EvalTestItem{
		{
			parser.NewExpr("weightedAverageByFilteredCount",
				"app.metric*.mean", 0.0, 0,
			),
			map[parser.MetricRequest][]*types.MetricData{
				{"app.metric*.mean", 0, 1}: {
					types.MakeMetricData("app.metric1.mean", []float64{30, 10}, 1, now32),
					types.MakeMetricData("app.metric2.mean", []float64{20, 220}, 1, now32),
				},
				{"app.metric*.count", 0, 1}: {
					types.MakeMetricData("app.metric1.count", []float64{3, 1}, 1, now32),
					types.MakeMetricData("app.metric2.count", []float64{2, 2}, 1, now32),
				},
			},
			[]*types.MetricData{types.MakeMetricData("weightedAverageByFilteredCount(app, 0)",
				[]float64{26, 150}, 1, now32)},
		},

		{
			parser.NewExpr("weightedAverageByFilteredCount",
				"app.metric*.mean", 0.0, 0,
			),
			map[parser.MetricRequest][]*types.MetricData{
				{"app.metric*.mean", 0, 1}: {
					types.MakeMetricData("app.metric1.mean", []float64{30, math.NaN()}, 1, now32),
					types.MakeMetricData("app.metric2.mean", []float64{20, 220}, 1, now32),
				},
				{"app.metric*.count", 0, 1}: {
					types.MakeMetricData("app.metric1.count", []float64{3, math.NaN()}, 1, now32),
					types.MakeMetricData("app.metric2.count", []float64{2, 2}, 1, now32),
				},
			},
			[]*types.MetricData{types.MakeMetricData("weightedAverageByFilteredCount(app, 0)",
				[]float64{26, 220}, 1, now32)},
		},

		{
			parser.NewExpr("weightedAverageByFilteredCount",
				"app.metric*.mean", 10.0, 0,
			),
			map[parser.MetricRequest][]*types.MetricData{
				{"app.metric*.mean", 0, 1}: {
					types.MakeMetricData("app.metric1.mean", []float64{30, 400}, 1, now32),
					types.MakeMetricData("app.metric2.mean", []float64{20, 220}, 1, now32),
				},
				{"app.metric*.count", 0, 1}: {
					types.MakeMetricData("app.metric1.count", []float64{3, 4}, 1, now32),
					types.MakeMetricData("app.metric2.count", []float64{11, 3}, 1, now32),
				},
			},
			[]*types.MetricData{types.MakeMetricData("weightedAverageByFilteredCount(app, 10)",
				[]float64{20, 0}, 1, now32)},
		},

		{
			parser.NewExpr("weightedAverageByFilteredCount",
				"app.metric*.mean", 0.0, 0, 1,
			),
			map[parser.MetricRequest][]*types.MetricData{
				{"app.metric*.mean", 0, 1}: {
					types.MakeMetricData("app.metric1.mean", []float64{30, 20}, 1, now32),
					types.MakeMetricData("app.metric2.mean", []float64{20, 220}, 1, now32),
				},
				{"app.metric*.count", 0, 1}: {
					types.MakeMetricData("app.metric1.count", []float64{3, 4}, 1, now32),
					types.MakeMetricData("app.metric2.count", []float64{11, 3}, 1, now32),
				},
			},
			[]*types.MetricData{
				types.MakeMetricData("weightedAverageByFilteredCount(app.metric1, 0)", []float64{30, 20}, 1, now32),
				types.MakeMetricData("weightedAverageByFilteredCount(app.metric2, 0)", []float64{20, 220}, 1, now32),
			},
		},
	}

	for _, tt := range tests {
		testName := tt.E.Target() + "(" + tt.E.RawArgs() + ")"
		t.Run(testName, func(t *testing.T) {
			th.TestEvalExpr(t, &tt)
		})
	}

}
