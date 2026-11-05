// Command invertedindex builds an inverted index (term -> documents that
// contain it) of a corpus on Spark through Spark Connect.
//
//	invertedindex -remote sc://node0:15002 -input hdfs://node0:9000/datasets/gutenberg-100 \
//	    -output hdfs://node0:9000/results/invertedindex -metrics runs.jsonl
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
		Name:  "invertedindex",
		Build: jobs.InvertedIndex,
		Order: func() []column.Convertible {
			return []column.Convertible{functions.Col("doc_freq").Desc(), functions.Col("term").Asc()}
		},
		Dump: func(ctx context.Context, df sql.DataFrame, w io.Writer) error {
			index, err := jobs.CollectIndex(ctx, df)
			if err != nil {
				return err
			}
			return reference.WriteIndex(w, index)
		},
		Stats: jobs.IndexStats,
	})
}
