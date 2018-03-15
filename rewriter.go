package main

type rewriter map[string]string

func newRewriter(cfg []rewriteConfig) rewriter {
	r := make(rewriter)
	for _, v := range cfg {
		r[v.From] = v.To
	}
	return r
}
