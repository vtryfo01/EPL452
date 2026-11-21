// Command clientbench measures the work of the Spark Connect client itself,
// which the job benchmarks do not show: connecting, the round trip of a
// trivial query, and bringing a result back to the client, as rows and as an
// Arrow table. baseline/clientbench.py does the same with PySpark.
//
//	clientbench -remote sc://localhost:15002 -metrics client.jsonl -label rep=1
//
// Every query is built with SQL, so both clients send the same plans:
//
//   - connect: create the session and run SELECT 1, the first query of the process;
//   - latency: SELECT 1 (submit the SQL, then collect its one row), -queries times;
//   - rows: collect "SELECT concat('term', id) AS term, id AS count FROM range(n)",
//     a result shaped like WordCount's, as rows, for every n of -sizes, -samples times;
//   - arrow: the same result as an Arrow table.
//
// Each measurement is one JSON line {"bench", "client", "case", "rows",
// "label", "ms"} on stdout or appended to -metrics. Before latency and every
// size, untimed queries warm the client and the server up.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/apache/spark-connect-go/spark/sql"

	"github.com/vtryfo01/EPL452/internal/cli"
)

// Sample is one measurement.
type Sample struct {
	Bench  string  `json:"bench"` // always "client"
	Client string  `json:"client"`
	Case   string  `json:"case"`
	Rows   int64   `json:"rows"`
	Label  string  `json:"label,omitempty"`
	MS     float64 `json:"ms"`
}

const warmups = 3

func main() {
	log.SetFlags(0)
	log.SetPrefix("clientbench: ")
	if err := run(context.Background(), os.Args[1:]); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("clientbench", flag.ContinueOnError)
	remote := fs.String("remote", envOr("SPARK_REMOTE", cli.DefaultRemote), "Spark Connect `URL` (default $SPARK_REMOTE)")
	cases := fs.String("cases", "connect,latency,rows,arrow", "comma-separated `cases` to measure")
	queries := fs.Int("queries", 200, "timed SELECT 1 queries of latency")
	sizes := fs.String("sizes", "1000,10000,100000,1000000", "comma-separated result `sizes` (rows) of rows and arrow")
	samples := fs.Int("samples", 5, "timed collects of every size")
	metrics := fs.String("metrics", "", "append the JSON lines to this `file` instead of printing them")
	label := fs.String("label", "", "free-form label stored with every measurement, e.g. \"rep=1\"")
	if err := fs.Parse(args); err != nil {
		return err
	}
	todo := map[string]bool{}
	for _, c := range strings.Split(*cases, ",") {
		switch c {
		case "connect", "latency", "rows", "arrow":
			todo[c] = true
		default:
			return fmt.Errorf("unknown case %q (connect, latency, rows, arrow)", c)
		}
	}
	var ns []int64
	for _, s := range strings.Split(*sizes, ",") {
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil || n < 1 {
			return fmt.Errorf("invalid size %q", s)
		}
		ns = append(ns, n)
	}

	var out io.Writer = os.Stdout
	if *metrics != "" {
		f, err := os.OpenFile(*metrics, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			return err
		}
		defer f.Close()
		out = f
	}
	enc := json.NewEncoder(out)
	record := func(name string, rows int64, d time.Duration) error {
		return enc.Encode(Sample{"client", "go", name, rows, *label, float64(d.Microseconds()) / 1000})
	}

	start := time.Now()
	spark, err := sql.NewSessionBuilder().Remote(*remote).Build(ctx)
	if err != nil {
		return fmt.Errorf("connecting to %s: %w", *remote, err)
	}
	defer spark.Stop()
	if err := selectOne(ctx, spark); err != nil {
		return err
	}
	if todo["connect"] {
		if err := record("connect", 1, time.Since(start)); err != nil {
			return err
		}
	}

	if todo["latency"] {
		for i := 0; i < warmups+*queries; i++ {
			start := time.Now()
			if err := selectOne(ctx, spark); err != nil {
				return err
			}
			if i >= warmups {
				if err := record("latency", 1, time.Since(start)); err != nil {
					return err
				}
			}
		}
	}

	for _, name := range []string{"rows", "arrow"} {
		if !todo[name] {
			continue
		}
		for _, n := range ns {
			for i := 0; i < warmups+*samples; i++ {
				start := time.Now()
				if err := fetch(ctx, spark, name, n); err != nil {
					return err
				}
				if i >= warmups {
					if err := record(name, n, time.Since(start)); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func selectOne(ctx context.Context, spark sql.SparkSession) error {
	df, err := spark.Sql(ctx, "SELECT 1")
	if err != nil {
		return err
	}
	rows, err := df.Collect(ctx)
	if err != nil {
		return err
	}
	if len(rows) != 1 {
		return fmt.Errorf("SELECT 1 returned %d rows", len(rows))
	}
	return nil
}

// fetch brings the n-row result back as rows or as an Arrow table.
func fetch(ctx context.Context, spark sql.SparkSession, how string, n int64) error {
	df, err := spark.Sql(ctx, fmt.Sprintf("SELECT concat('term', id) AS term, id AS count FROM range(%d)", n))
	if err != nil {
		return err
	}
	var got int64
	if how == "rows" {
		rows, err := df.Collect(ctx)
		if err != nil {
			return err
		}
		got = int64(len(rows))
	} else {
		table, err := df.ToArrow(ctx)
		if err != nil {
			return err
		}
		got = (*table).NumRows()
		(*table).Release()
	}
	if got != n {
		return fmt.Errorf("%s: expected %d rows, got %d", how, n, got)
	}
	return nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
