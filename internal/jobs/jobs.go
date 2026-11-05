// Package jobs builds the WordCount and Inverted Index queries with the
// Spark Connect Go client. The queries only describe the computation; Spark
// runs them on the cluster when an action (write, collect, ...) is called.
//
// The record and tokenization rules are documented in package textproc.
package jobs

import (
	"context"

	"github.com/apache/spark-connect-go/spark/sql"
	"github.com/apache/spark-connect-go/spark/sql/column"
	"github.com/apache/spark-connect-go/spark/sql/functions"

	"github.com/vtryfo01/EPL452/internal/textproc"
)

// call invokes a Spark SQL function by name. It is used where the generated
// Go wrapper is missing (array_sort) or sends an argument with a type that
// Spark's ANSI mode rejects (the generated split() always sends a BIGINT
// limit, while Spark expects an INT).
func call(name string, args ...column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns(name, args...))
}

// Records reads a corpus (a file or a directory of files, local or on HDFS)
// and returns one row per line with columns doc_id and text.
func Records(ctx context.Context, spark sql.SparkSession, input string) (sql.DataFrame, error) {
	lines, err := spark.Read().Format("text").Load(input)
	if err != nil {
		return nil, err
	}
	value := functions.Col("value")
	tab := functions.StringLit("\t")
	return lines.Select(ctx,
		functions.SplitPart(value, tab, functions.Int32Lit(1)).Alias("doc_id"),
		functions.SplitPart(value, tab, functions.Int32Lit(2)).Alias("text"),
	)
}

// Terms returns one (doc_id, term) row per term occurrence.
func Terms(ctx context.Context, spark sql.SparkSession, input string) (sql.DataFrame, error) {
	records, err := Records(ctx, spark, input)
	if err != nil {
		return nil, err
	}
	words := call("split", functions.Lower(functions.Col("text")), functions.StringLit(textproc.TokenPattern))
	terms, err := records.Select(ctx,
		functions.Col("doc_id"),
		functions.Explode(words).Alias("term"),
	)
	if err != nil {
		return nil, err
	}
	return terms.Filter(ctx, functions.Col("term").Neq(functions.StringLit("")))
}

// WordCount returns (term, count): the number of occurrences of every term.
func WordCount(ctx context.Context, spark sql.SparkSession, input string) (sql.DataFrame, error) {
	terms, err := Terms(ctx, spark, input)
	if err != nil {
		return nil, err
	}
	return terms.GroupBy(functions.Col("term")).
		Agg(ctx, functions.Count(functions.Col("term")).Alias("count"))
}

// InvertedIndex returns (term, docs, doc_freq): the sorted ids of the
// documents that contain every term and how many there are.
func InvertedIndex(ctx context.Context, spark sql.SparkSession, input string) (sql.DataFrame, error) {
	terms, err := Terms(ctx, spark, input)
	if err != nil {
		return nil, err
	}
	docs := call("array_sort", functions.CollectSet(functions.Col("doc_id")))
	index, err := terms.GroupBy(functions.Col("term")).Agg(ctx, docs.Alias("docs"))
	if err != nil {
		return nil, err
	}
	return index.WithColumn(ctx, "doc_freq", functions.Size(functions.Col("docs")))
}
