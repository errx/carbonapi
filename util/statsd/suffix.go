package statsd

import "strings"

const CountSuffix = ".count"

var StatsdSuffixes = [...]string{
	".last",
	".min",
	".max",
	".sum",
	".median",
	".mean",
	".percentile.75",
	".percentile.95",
	".percentile.98",
	".percentile.99",
	".percentile.999",
}

func CountSuffixMetric(name string) string {
	suffix := GetSuffix(name)
	return strings.TrimSuffix(name, suffix) + CountSuffix
}

func GetSuffix(name string) (suffix string) {
	for _, suffix := range StatsdSuffixes {
		if strings.HasSuffix(name, suffix) {
			return suffix
		}
	}
	return
}
