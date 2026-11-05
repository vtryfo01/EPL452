package jobs

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/spark-connect-go/spark/sql"
	"github.com/apache/spark-connect-go/spark/sql/column"
	"github.com/apache/spark-connect-go/spark/sql/functions"

	"github.com/vtryfo01/EPL452/internal/textproc"
)

// The helpers below bring (parts of) a job's result back to the client. Use
// them for validation and small inputs only: a full Collect of a large result
// funnels everything through the driver and the client.

// CollectCounts collects a WordCount result into a map.
func CollectCounts(ctx context.Context, df sql.DataFrame) (map[string]int64, error) {
	rows, err := df.Collect(ctx)
	if err != nil {
		return nil, err
	}
	counts := make(map[string]int64, len(rows))
	for _, row := range rows {
		term, err := asString(row.Value("term"))
		if err != nil {
			return nil, err
		}
		if counts[term], err = asInt64(row.Value("count")); err != nil {
			return nil, fmt.Errorf("term %q: %w", term, err)
		}
	}
	return counts, nil
}

// CollectIndex collects an InvertedIndex result into a map. It also checks
// that every doc_freq equals the length of its document list.
func CollectIndex(ctx context.Context, df sql.DataFrame) (map[string][]string, error) {
	rows, err := df.Collect(ctx)
	if err != nil {
		return nil, err
	}
	index := make(map[string][]string, len(rows))
	for _, row := range rows {
		term, err := asString(row.Value("term"))
		if err != nil {
			return nil, err
		}
		docs, err := asStrings(row.Value("docs"))
		if err != nil {
			return nil, fmt.Errorf("term %q: %w", term, err)
		}
		freq, err := asInt64(row.Value("doc_freq"))
		if err != nil {
			return nil, fmt.Errorf("term %q: %w", term, err)
		}
		if freq != int64(len(docs)) {
			return nil, fmt.Errorf("term %q: doc_freq %d but %d documents", term, freq, len(docs))
		}
		index[term] = docs
	}
	return index, nil
}

// Stats summarizes a result. For WordCount, Total is the number of term
// occurrences; for InvertedIndex it is the number of (term, document) pairs.
type Stats struct {
	Terms int64 `json:"terms"`
	Total int64 `json:"total"`
}

// CountStats summarizes a WordCount result.
func CountStats(ctx context.Context, df sql.DataFrame) (Stats, error) {
	return stats(ctx, df, "count")
}

// IndexStats summarizes an InvertedIndex result.
func IndexStats(ctx context.Context, df sql.DataFrame) (Stats, error) {
	return stats(ctx, df, "doc_freq")
}

func stats(ctx context.Context, df sql.DataFrame, sumColumn string) (Stats, error) {
	agg, err := df.Agg(ctx,
		functions.Count(functions.Col("term")).Alias("terms"),
		functions.Sum(functions.Col(sumColumn)).Alias("total"),
	)
	if err != nil {
		return Stats{}, err
	}
	row, err := agg.First(ctx)
	if err != nil {
		return Stats{}, err
	}
	var s Stats
	if s.Terms, err = asInt64(row.Value("terms")); err != nil {
		return Stats{}, err
	}
	if row.Value("total") == nil { // sum over an empty result
		return s, nil
	}
	s.Total, err = asInt64(row.Value("total"))
	return s, err
}

// SelectTerms keeps the rows of a result whose term is in terms.
func SelectTerms(ctx context.Context, df sql.DataFrame, terms []string) (sql.DataFrame, error) {
	quoted := make([]string, len(terms))
	for i, t := range terms {
		// Terms only contain [a-z0-9], so they can be inlined safely.
		if !textproc.IsTerm(t) {
			return nil, fmt.Errorf("invalid term %q", t)
		}
		quoted[i] = "'" + t + "'"
	}
	return df.Where(ctx, "term IN ("+strings.Join(quoted, ", ")+")")
}

// FirstRows returns the first n rows of df in the given order, formatted as
// strings for printing.
func FirstRows(ctx context.Context, df sql.DataFrame, n int, order ...column.Convertible) ([][]string, error) {
	sorted, err := df.Sort(ctx, order...)
	if err != nil {
		return nil, err
	}
	rows, err := sorted.Head(ctx, int32(n))
	if err != nil {
		return nil, err
	}
	out := make([][]string, len(rows))
	for i, row := range rows {
		for _, v := range row.Values() {
			out[i] = append(out[i], fmt.Sprint(v))
		}
	}
	return out, nil
}

func asString(v any) (string, error) {
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("expected a string, got %T", v)
	}
	return s, nil
}

func asInt64(v any) (int64, error) {
	switch n := v.(type) {
	case int64:
		return n, nil
	case int32:
		return int64(n), nil
	case int:
		return int64(n), nil
	}
	return 0, fmt.Errorf("expected an integer, got %T", v)
}

func asStrings(v any) ([]string, error) {
	switch list := v.(type) {
	case []string:
		return list, nil
	case []any:
		out := make([]string, len(list))
		for i, e := range list {
			s, err := asString(e)
			if err != nil {
				return nil, err
			}
			out[i] = s
		}
		return out, nil
	}
	return nil, fmt.Errorf("expected a list of strings, got %T", v)
}
