package expr

import (
	"testing"
)

type metricCase struct {
  input    string
  expected string
}

var metricTests = []metricCase{
	{"", ""},
	{"some.string", "string"},
	{"some.string,1,2,3", "string"},
	{"some.string,1,2,3))))", "string"},
}

func TestPrepareMetric(t *testing.T) {
	for _, pair := range metricTests {
		m := prepareMetric(pair.input)
		if m != pair.expected {
			t.Error(
				"input", pair.input,
				"expected", pair.expected,
				"got", m,
      		)
		}
	}
}

type kubeMetricCase struct {
	input    string
	expected []string
}

var kubeMetricTests = []kubeMetricCase{
	{"", []string{"", "", ""}},
	{"1.2.3.4", []string{"", "", ""}},
	{"1.2.3.4.5", []string{"", "5_", ""}},
	{"1.2.3.4.5.6", []string{"", "5_", ""}},
	{"1.2.3.4.5.6.7", []string{"7", "5_", ""}},
	{"1.2.3.4.5.6.7.8", []string{"7", "5_8", ".8"}},
	{"1.2.3.4.5.6.7.8.9", []string{"7", "5_8", ".9"}},
	{"1.2.3.4.5.6.7.8.9.10", []string{"7", "5_8", ".9.10"}},
}

func TestPrepareKubeMetric(t *testing.T) {
	for _, pair := range kubeMetricTests {
		name, key, suffix := prepareKubeMetric(pair.input)
		if name != pair.expected[0] || key != pair.expected[1] || suffix != pair.expected[2] {
			t.Error(
				"input", pair.input,
				"expected", pair.expected,
				"got", []string{name, key, suffix},
			)
		}
	}
}
