// Command wordcount counts the terms of a corpus on Spark through Spark
// Connect.
//
//	wordcount -remote sc://node0:15002 -input hdfs://node0:9000/datasets/gutenberg-100 \
//	    -output hdfs://node0:9000/results/wordcount -metrics runs.jsonl
package main

import (
	"context"
	"io"

	"github.com/apache/spark-connect-go/spark/sql"
	"github.com/apache/spark-connect-go/spark/sql/column"
	"github.com/apache/spark-connect-go/spark/sql/functions"

	"github.com/vtryfo01/EPL452/internal/cli"
	"github.com/vtryfo01/EPL452/internal/jobs"
	"github.com/vtryfo01/EPL452/internal/reference"
)

func main() {
	cli.Main(cli.Job{
		Name:  "wordcount",
		Build: jobs.WordCount,
		Order: func() []column.Convertible {
			return []column.Convertible{functions.Col("count").Desc(), functions.Col("term").Asc()}
		},
		Dump: func(ctx context.Context, df sql.DataFrame, w io.Writer) error {
			counts, err := jobs.CollectCounts(ctx, df)
			if err != nil {
				return err
			}
			return reference.WriteCounts(w, counts)
		},
		Stats: jobs.CountStats,
	})
}
