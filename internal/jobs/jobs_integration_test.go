package jobs

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/spark-connect-go/spark/sql"

	"github.com/vtryfo01/EPL452/internal/reference"
)

// These tests run the jobs on a real Spark Connect server and compare the
// results with the hand-checked expected outputs of the small corpus. They
// are skipped unless SPARK_REMOTE is set, e.g.
//
//	SPARK_REMOTE=sc://localhost:15002 go test ./internal/jobs
//
// By default the server reads testdata/small/corpus from the local disk, which
// only works when it runs on this machine. For a remote cluster, copy the
// corpus to HDFS (scripts/hdfs/load_hdfs.sh testdata/small/corpus) and set
// SPARK_TEST_INPUT=hdfs://<namenode>:9000/datasets/corpus.

func session(t *testing.T) (context.Context, sql.SparkSession, string) {
	t.Helper()
	remote := os.Getenv("SPARK_REMOTE")
	if remote == "" {
		t.Skip("SPARK_REMOTE not set")
	}
	input := os.Getenv("SPARK_TEST_INPUT")
	if input == "" {
		abs, err := filepath.Abs("../../testdata/small/corpus")
		if err != nil {
			t.Fatal(err)
		}
		input = "file://" + ensureLeadingSlash(filepath.ToSlash(abs))
	}
	ctx := context.Background()
	spark, err := sql.NewSessionBuilder().Remote(remote).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { spark.Stop() })
	return ctx, spark, input
}

func ensureLeadingSlash(p string) string {
	if strings.HasPrefix(p, "/") {
		return p
	}
	return "/" + p // Windows: C:/x -> /C:/x
}

func golden(t *testing.T, name string) *os.File {
	t.Helper()
	f, err := os.Open(filepath.Join("../../testdata/small/expected", name))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	return f
}

func TestWordCountOnSpark(t *testing.T) {
	ctx, spark, input := session(t)
	df, err := WordCount(ctx, spark, input)
	if err != nil {
		t.Fatal(err)
	}
	got, err := CollectCounts(ctx, df)
	if err != nil {
		t.Fatal(err)
	}
	want, err := reference.ReadCounts(golden(t, "wordcount.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	if diffs := reference.DiffCounts(want, got, 20); len(diffs) > 0 {
		t.Fatalf("Spark word count differs from the expected output:\n%s", strings.Join(diffs, "\n"))
	}

	stats, err := CountStats(ctx, df)
	if err != nil {
		t.Fatal(err)
	}
	if stats != (Stats{Terms: 29, Total: 42}) {
		t.Errorf("CountStats = %+v, want {Terms:29 Total:42}", stats)
	}
}

func TestInvertedIndexOnSpark(t *testing.T) {
	ctx, spark, input := session(t)
	df, err := InvertedIndex(ctx, spark, input)
	if err != nil {
		t.Fatal(err)
	}
	got, err := CollectIndex(ctx, df)
	if err != nil {
		t.Fatal(err)
	}
	want, err := reference.ReadIndex(golden(t, "invertedindex.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	if diffs := reference.DiffIndex(want, got, 20); len(diffs) > 0 {
		t.Fatalf("Spark inverted index differs from the expected output:\n%s", strings.Join(diffs, "\n"))
	}

	stats, err := IndexStats(ctx, df)
	if err != nil {
		t.Fatal(err)
	}
	if stats != (Stats{Terms: 29, Total: 33}) {
		t.Errorf("IndexStats = %+v, want {Terms:29 Total:33}", stats)
	}
}

func TestSelectTermsOnSpark(t *testing.T) {
	ctx, spark, input := session(t)
	df, err := WordCount(ctx, spark, input)
	if err != nil {
		t.Fatal(err)
	}
	selected, err := SelectTerms(ctx, df, []string{"fox", "hello", "absent"})
	if err != nil {
		t.Fatal(err)
	}
	got, err := CollectCounts(ctx, selected)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int64{"fox": 3, "hello": 4}
	if diffs := reference.DiffCounts(want, got, 0); len(diffs) > 0 {
		t.Fatal(diffs)
	}
	if _, err := SelectTerms(ctx, df, []string{"it's"}); err == nil {
		t.Error("SelectTerms accepted a term with a quote")
	}
}
