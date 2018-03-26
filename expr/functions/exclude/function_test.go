package exclude

import (
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

func TestExclude(t *testing.T) {
	now32 := uint32(time.Now().Unix())

	tests := []th.EvalTestItem{
		{
			parser.NewExpr("exclude",

				"metric1",
				parser.ArgValue("(Foo|Baz)"),
			),
			map[parser.MetricRequest][]*types.MetricData{
				{"metric1", 0, 1}: {
					types.MakeMetricData("metricFoo", []float64{1, 1, 1, 1, 1}, 1, now32),
					types.MakeMetricData("metricBar", []float64{2, 2, 2, 2, 2}, 1, now32),
					types.MakeMetricData("metricBaz", []float64{3, 3, 3, 3, 3}, 1, now32),
				},
			},
			[]*types.MetricData{types.MakeMetricData("metricBar", // NOTE(dgryski): not sure if this matches graphite
				[]float64{2, 2, 2, 2, 2}, 1, now32)},
		},
	}

	for _, tt := range tests {
		testName := tt.E.Target() + "(" + tt.E.RawArgs() + ")"
		t.Run(testName, func(t *testing.T) {
			th.TestEvalExpr(t, &tt)
		})
	}

}
