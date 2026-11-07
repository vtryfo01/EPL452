// Command verify checks a Spark WordCount or Inverted Index result against
// the single-machine reference implementation run on a local copy of the
// same corpus.
//
// Small inputs are compared in full. Large inputs are compared on totals
// (number of terms and total count or postings) plus spot checks of the most
// frequent terms, a seeded random sample of terms, and terms that must be
// absent.
//
// With -copies k the input is k complete copies of the -local corpus, made by
// scripts/data/scale_dataset.py --copies k: every count and document frequency
// is k times the reference, and every document appears in each copy. A large
// weak-scaling input is then checked without a local copy of it.
//
//	verify -job wordcount -input hdfs://node0:9000/datasets/gutenberg-100 \
//	    -local data/processed/gutenberg-100 [-result hdfs://node0:9000/results/wordcount]
//	verify -job invertedindex -input hdfs://node0:9000/datasets/weak-3 \
//	    -local data/processed/sample-2000 -copies 15
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"net/url"
	"os"
	"sort"

	"github.com/apache/spark-connect-go/spark/sql"

	"github.com/vtryfo01/EPL452/internal/jobs"
	"github.com/vtryfo01/EPL452/internal/reference"
)

// fullLimit is the largest number of terms compared in full in -mode auto.
const fullLimit = 100_000

func main() {
	log.SetFlags(0)
	log.SetPrefix("verify: ")

	remote := flag.String("remote", envOr("SPARK_REMOTE", "sc://localhost:15002"), "Spark Connect `URL` (default $SPARK_REMOTE)")
	job := flag.String("job", "", "wordcount or invertedindex")
	input := flag.String("input", "", "corpus path as seen by Spark")
	local := flag.String("local", "", "local copy of the corpus for the reference (default: -input if it is local)")
	result := flag.String("result", "", "check this stored result instead of running the job")
	format := flag.String("format", "parquet", "format of -result")
	mode := flag.String("mode", "auto", "full, sample, or auto (full when the result has at most 100000 terms)")
	sampleSize := flag.Int("sample", 50, "number of random terms checked in sample mode")
	seed := flag.Uint64("seed", 452, "seed for the random sample")
	copies := flag.Int("copies", 1, "the input is this many complete copies of -local (scale_dataset.py --copies)")
	flag.Parse()

	if *job != "wordcount" && *job != "invertedindex" {
		log.Fatal("-job must be wordcount or invertedindex")
	}
	if *copies < 1 {
		log.Fatal("-copies must be at least 1")
	}
	if *input == "" {
		log.Fatal("-input is required")
	}
	if *local == "" {
		p, ok := localPath(*input)
		if !ok {
			log.Fatalf("-input %s is not local; pass -local with a local copy of the corpus", *input)
		}
		*local = p
	}

	ctx := context.Background()
	v, err := newVerifier(ctx, *remote, *job, *input, *local, *result, *format, *copies)
	if err != nil {
		log.Fatal(err)
	}
	full := *mode == "full" || (*mode == "auto" && v.refTerms() <= fullLimit)
	var problems []string
	if full {
		problems, err = v.full(ctx)
	} else {
		problems, err = v.sample(ctx, *sampleSize, *seed)
	}
	if err != nil {
		log.Fatal(err)
	}
	if len(problems) > 0 {
		for _, p := range problems {
			fmt.Println("MISMATCH", p)
		}
		os.Exit(1)
	}
	fmt.Println("OK")
}

type verifier struct {
	job    string
	df     sql.DataFrame
	counts reference.WordCounts // already multiplied by copies
	index  *reference.Index     // of one copy; see wantDocs
	copies int
}

func newVerifier(ctx context.Context, remote, job, input, local, result, format string, copies int) (*verifier, error) {
	files, err := reference.InputFiles(local)
	if err != nil {
		return nil, err
	}
	v := &verifier{job: job, copies: copies}
	log.Printf("computing the reference %s of %s", job, local)
	if job == "wordcount" {
		v.counts, err = reference.WordCount(files)
	} else {
		v.index, err = reference.InvertedIndex(files)
	}
	if err != nil {
		return nil, err
	}
	if copies > 1 {
		log.Printf("expecting %d complete copies of %s", copies, local)
		for t := range v.counts {
			v.counts[t] *= int64(copies)
		}
	}

	spark, err := sql.NewSessionBuilder().Remote(remote).Build(ctx)
	if err != nil {
		return nil, fmt.Errorf("connecting to %s: %w", remote, err)
	}
	switch {
	case result != "":
		log.Printf("reading the stored result %s", result)
		v.df, err = spark.Read().Format(format).Load(result)
	case job == "wordcount":
		v.df, err = jobs.WordCount(ctx, spark, input)
	default:
		v.df, err = jobs.InvertedIndex(ctx, spark, input)
	}
	return v, err
}

func (v *verifier) refTerms() int {
	if v.counts != nil {
		return len(v.counts)
	}
	return len(v.index.Postings)
}

// full compares every row of the result.
func (v *verifier) full(ctx context.Context) ([]string, error) {
	log.Printf("comparing all %d terms", v.refTerms())
	if v.job == "wordcount" {
		got, err := jobs.CollectCounts(ctx, v.df)
		if err != nil {
			return nil, err
		}
		return reference.DiffCounts(v.counts, got, 20), nil
	}
	got, err := jobs.CollectIndex(ctx, v.df)
	if err != nil {
		return nil, err
	}
	want := make(map[string][]string, len(v.index.Postings))
	for t := range v.index.Postings {
		want[t] = v.wantDocs(t)
	}
	return reference.DiffIndex(want, got, 20), nil
}

// wantDocs returns the expected sorted documents of term in the input, or
// nil if the term does not occur.
func (v *verifier) wantDocs(term string) []string {
	docs := v.index.DocsOf(term)
	if docs == nil || v.copies == 1 {
		return docs
	}
	return reference.Replicated(docs, v.copies)
}

// sample compares the totals and a subset of the terms.
func (v *verifier) sample(ctx context.Context, n int, seed uint64) ([]string, error) {
	var problems []string
	var want jobs.Stats
	var stats func(context.Context, sql.DataFrame) (jobs.Stats, error)
	if v.job == "wordcount" {
		want = jobs.Stats{Terms: int64(len(v.counts)), Total: v.counts.Tokens()}
		stats = jobs.CountStats
	} else {
		want = jobs.Stats{Terms: int64(len(v.index.Postings)), Total: v.index.TotalPostings() * int64(v.copies)}
		stats = jobs.IndexStats
	}
	got, err := stats(ctx, v.df)
	if err != nil {
		return nil, err
	}
	log.Printf("totals: spark %+v, reference %+v", got, want)
	if got != want {
		problems = append(problems, fmt.Sprintf("totals: got %+v, want %+v", got, want))
	}

	terms := v.pickTerms(n, seed)
	log.Printf("spot-checking %d terms", len(terms))
	df, err := jobs.SelectTerms(ctx, v.df, terms)
	if err != nil {
		return nil, err
	}
	if v.job == "wordcount" {
		gotCounts, err := jobs.CollectCounts(ctx, df)
		if err != nil {
			return nil, err
		}
		wantCounts := make(map[string]int64)
		for _, t := range terms {
			if c, ok := v.counts[t]; ok {
				wantCounts[t] = c
			}
		}
		return append(problems, reference.DiffCounts(wantCounts, gotCounts, 20)...), nil
	}
	gotIndex, err := jobs.CollectIndex(ctx, df)
	if err != nil {
		return nil, err
	}
	wantIndex := make(map[string][]string)
	for _, t := range terms {
		if docs := v.wantDocs(t); docs != nil {
			wantIndex[t] = docs
		}
	}
	return append(problems, reference.DiffIndex(wantIndex, gotIndex, 20)...), nil
}

// pickTerms returns the 10 most frequent terms, n terms drawn at random with
// the given seed, and a few terms that do not occur in the corpus.
func (v *verifier) pickTerms(n int, seed uint64) []string {
	weight := make(map[string]int64)
	if v.counts != nil {
		for t, c := range v.counts {
			weight[t] = c
		}
	} else {
		for t, list := range v.index.Postings {
			weight[t] = int64(len(list))
		}
	}
	all := make([]string, 0, len(weight))
	for t := range weight {
		all = append(all, t)
	}
	sort.Slice(all, func(i, j int) bool {
		if weight[all[i]] != weight[all[j]] {
			return weight[all[i]] > weight[all[j]]
		}
		return all[i] < all[j]
	})

	picked := make(map[string]bool)
	for _, t := range all[:min(10, len(all))] {
		picked[t] = true
	}
	rng := rand.New(rand.NewPCG(seed, 0))
	for _, i := range rng.Perm(len(all))[:min(n, len(all))] {
		picked[all[i]] = true
	}
	for _, t := range []string{"zzzqqq452", "notaword0x", "xqzv9999"} {
		if _, ok := weight[t]; !ok {
			picked[t] = true
		}
	}
	terms := make([]string, 0, len(picked))
	for t := range picked {
		terms = append(terms, t)
	}
	sort.Strings(terms)
	return terms
}

// localPath reports whether input is a local path and returns it.
func localPath(input string) (string, bool) {
	u, err := url.Parse(input)
	if err != nil || len(u.Scheme) <= 1 { // no scheme, or a Windows drive letter
		return input, true
	}
	if u.Scheme == "file" {
		// file:///C:/data -> C:/data on Windows, file:///data -> /data elsewhere.
		p := u.Path
		if len(p) > 2 && p[0] == '/' && p[2] == ':' {
			p = p[1:]
		}
		return p, true
	}
	return "", false
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
