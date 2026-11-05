// Package cli is the command-line driver shared by cmd/wordcount and
// cmd/invertedindex: it connects to Spark Connect, runs one job, and reports
// its timing in a form the benchmark scripts can collect.
package cli

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/apache/spark-connect-go/spark/sql"
	"github.com/apache/spark-connect-go/spark/sql/column"

	"github.com/vtryfo01/EPL452/internal/jobs"
)

// Job describes one Spark job and how to present its result.
type Job struct {
	Name  string
	Build func(ctx context.Context, spark sql.SparkSession, input string) (sql.DataFrame, error)
	// Order is used to print the most interesting rows first.
	Order func() []column.Convertible
	// Dump writes the whole result in the canonical TSV format.
	Dump  func(ctx context.Context, df sql.DataFrame, w io.Writer) error
	Stats func(ctx context.Context, df sql.DataFrame) (jobs.Stats, error)
}

// DefaultRemote is used when neither -remote nor SPARK_REMOTE is set.
const DefaultRemote = "sc://localhost:15002"

// Metrics is appended as one JSON line to the -metrics file.
type Metrics struct {
	Job       string      `json:"job"`
	Label     string      `json:"label,omitempty"`
	Remote    string      `json:"remote"`
	Input     string      `json:"input"`
	Output    string      `json:"output,omitempty"`
	Format    string      `json:"format,omitempty"`
	Action    string      `json:"action"`
	Start     time.Time   `json:"start"`
	ElapsedMS int64       `json:"elapsed_ms"`
	Stats     *jobs.Stats `json:"stats,omitempty"`
}

// Main parses the command line, runs the job and exits.
func Main(job Job) {
	log.SetFlags(0)
	log.SetPrefix(job.Name + ": ")
	if err := run(context.Background(), job, os.Args[1:], os.Stdout); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, job Job, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet(job.Name, flag.ContinueOnError)
	remote := fs.String("remote", envOr("SPARK_REMOTE", DefaultRemote), "Spark Connect `URL` (default $SPARK_REMOTE)")
	input := fs.String("input", "", "input corpus: a file or directory, e.g. hdfs://node0:9000/datasets/gutenberg-100")
	output := fs.String("output", "", "write the result to this `path` (overwritten)")
	format := fs.String("format", "parquet", "output format: parquet, json, or noop (run the job without storing the result)")
	show := fs.Int("show", -1, "print the first `n` rows of the result (default 20 unless -output or -dump is set)")
	dump := fs.Bool("dump", false, "print the whole result as sorted TSV on stdout (small inputs only)")
	stats := fs.Bool("stats", false, "also compute the number of terms and the total count")
	metricsPath := fs.String("metrics", "", "append a JSON line with the timing to this `file`")
	label := fs.String("label", "", "free-form label stored in the metrics, e.g. \"strong workers=4 run=1\"")
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "usage: %s -input PATH [flags]\n\n", job.Name)
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *input == "" {
		fs.Usage()
		return fmt.Errorf("-input is required")
	}
	switch *format {
	case "parquet", "json":
	case "noop":
		if *output != "" {
			return fmt.Errorf("-format noop does not store anything; drop -output")
		}
	default:
		return fmt.Errorf("unsupported -format %q", *format)
	}
	write := *output != "" || *format == "noop"
	if *dump && write {
		return fmt.Errorf("-dump cannot be combined with -output or -format noop")
	}
	if *stats && *format == "noop" {
		return fmt.Errorf("-stats needs a stored result; use -output instead of -format noop")
	}
	if *show < 0 {
		*show = 0
		if !write && !*dump {
			*show = 20
		}
	}

	spark, err := sql.NewSessionBuilder().Remote(*remote).Build(ctx)
	if err != nil {
		return fmt.Errorf("connecting to %s: %w", *remote, err)
	}
	defer spark.Stop()

	result, err := job.Build(ctx, spark, *input)
	if err != nil {
		return err
	}

	m := Metrics{Job: job.Name, Label: *label, Remote: *remote, Input: *input, Start: time.Now().UTC()}
	switch {
	case write:
		m.Action, m.Output, m.Format = "write", *output, *format
		err = result.Writer().Mode("overwrite").Format(*format).Save(ctx, *output)
	case *dump:
		m.Action = "collect"
		err = job.Dump(ctx, result, stdout)
	default:
		// Sorting needs the complete result, so this still runs the whole job.
		m.Action = "show"
		var rows [][]string
		rows, err = jobs.FirstRows(ctx, result, *show, job.Order()...)
		if err == nil {
			printRows(os.Stderr, rows)
		}
	}
	if err != nil {
		return fmt.Errorf("running %s on %s: %w", job.Name, *input, err)
	}
	m.ElapsedMS = time.Since(m.Start).Milliseconds()
	log.Printf("%s of %s took %.2fs", m.Action, *input, float64(m.ElapsedMS)/1000)

	if m.Action == "write" && *format != "noop" {
		// Inspect what was stored instead of recomputing the job.
		if result, err = spark.Read().Format(*format).Load(*output); err != nil {
			return err
		}
		if *show > 0 {
			rows, err := jobs.FirstRows(ctx, result, *show, job.Order()...)
			if err != nil {
				return err
			}
			printRows(os.Stderr, rows)
		}
	}
	if *stats {
		s, err := job.Stats(ctx, result)
		if err != nil {
			return err
		}
		m.Stats = &s
		log.Printf("terms=%d total=%d", s.Terms, s.Total)
	}

	if *metricsPath != "" {
		return appendJSONLine(*metricsPath, m)
	}
	return nil
}

func printRows(w io.Writer, rows [][]string) {
	const maxCell = 80
	for _, row := range rows {
		for i, cell := range row {
			if len(cell) > maxCell {
				row[i] = cell[:maxCell-3] + "..."
			}
		}
		fmt.Fprintln(w, strings.Join(row, "\t"))
	}
}

func appendJSONLine(path string, v any) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if err := json.NewEncoder(f).Encode(v); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
