// Package reference is a single-machine implementation of WordCount and
// Inverted Index over a local copy of a corpus. It follows the same record and
// tokenization rules as the Spark jobs (see package textproc) and is the
// oracle for correctness checks.
package reference

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/vtryfo01/EPL452/internal/textproc"
)

// maxLine bounds the length of a single corpus line.
const maxLine = 64 << 20

// InputFiles resolves path the way Spark's file sources do: a file is read
// as is; for a directory every regular file directly inside it is read,
// except names starting with '_' or '.' (e.g. _SUCCESS, _MANIFEST.json).
func InputFiles(path string) ([]string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return []string{path}, nil
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	var files []string
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || strings.HasPrefix(name, "_") || strings.HasPrefix(name, ".") {
			continue
		}
		files = append(files, filepath.Join(path, name))
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no input files in %s", path)
	}
	sort.Strings(files)
	return files, nil
}

// ForEachRecord calls fn for every record of the given files.
func ForEachRecord(files []string, fn func(docID, text string)) error {
	for _, name := range files {
		if err := forEachRecordInFile(name, fn); err != nil {
			return err
		}
	}
	return nil
}

func forEachRecordInFile(name string, fn func(docID, text string)) error {
	f, err := os.Open(name)
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), maxLine)
	for sc.Scan() {
		fn(textproc.ParseRecord(sc.Text()))
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	return nil
}

// WordCounts maps each term to its number of occurrences.
type WordCounts map[string]int64

// WordCount counts the terms of the given files.
func WordCount(files []string) (WordCounts, error) {
	counts := make(WordCounts)
	err := ForEachRecord(files, func(_, text string) {
		textproc.ForEachTerm(text, func(term string) { counts[term]++ })
	})
	return counts, err
}

// Tokens returns the total number of term occurrences.
func (wc WordCounts) Tokens() int64 {
	var n int64
	for _, c := range wc {
		n += c
	}
	return n
}

// Index is an inverted index. Docs holds the distinct document ids in sorted
// order; each posting list holds ascending indexes into Docs.
type Index struct {
	Docs     []string
	Postings map[string][]int32
}

// InvertedIndex builds the inverted index of the given files. Document ids
// are interned so that memory grows with the number of distinct
// (term, document) pairs rather than with the number of tokens.
func InvertedIndex(files []string) (*Index, error) {
	docIndex := make(map[string]int32)
	var docs []string
	postings := make(map[string][]int32)
	err := ForEachRecord(files, func(docID, text string) {
		d, ok := docIndex[docID]
		if !ok {
			d = int32(len(docs))
			docIndex[docID] = d
			docs = append(docs, docID)
		}
		textproc.ForEachTerm(text, func(term string) {
			list := postings[term]
			// Documents are usually contiguous, so this check removes almost
			// all duplicates; the rest are removed below.
			if n := len(list); n == 0 || list[n-1] != d {
				postings[term] = append(list, d)
			}
		})
	})
	if err != nil {
		return nil, err
	}

	// Renumber documents in sorted id order, then sort and deduplicate.
	order := make([]int32, len(docs))
	for i := range order {
		order[i] = int32(i)
	}
	sort.Slice(order, func(i, j int) bool { return docs[order[i]] < docs[order[j]] })
	rank := make([]int32, len(docs))
	sorted := make([]string, len(docs))
	for r, d := range order {
		rank[d] = int32(r)
		sorted[r] = docs[d]
	}
	for term, list := range postings {
		for i, d := range list {
			list[i] = rank[d]
		}
		sort.Slice(list, func(i, j int) bool { return list[i] < list[j] })
		postings[term] = dedupSorted(list)
	}
	return &Index{Docs: sorted, Postings: postings}, nil
}

func dedupSorted(list []int32) []int32 {
	if len(list) < 2 {
		return list
	}
	out := list[:1]
	for _, d := range list[1:] {
		if d != out[len(out)-1] {
			out = append(out, d)
		}
	}
	return out
}

// DocsOf returns the sorted document ids that contain term.
func (ix *Index) DocsOf(term string) []string {
	list, ok := ix.Postings[term]
	if !ok {
		return nil
	}
	docs := make([]string, len(list))
	for i, d := range list {
		docs[i] = ix.Docs[d]
	}
	return docs
}

// TotalPostings returns the number of distinct (term, document) pairs.
func (ix *Index) TotalPostings() int64 {
	var n int64
	for _, list := range ix.Postings {
		n += int64(len(list))
	}
	return n
}

// Map returns the index as term -> sorted document ids.
func (ix *Index) Map() map[string][]string {
	m := make(map[string][]string, len(ix.Postings))
	for term := range ix.Postings {
		m[term] = ix.DocsOf(term)
	}
	return m
}

// WriteCounts writes "term\tcount" lines sorted by term.
func WriteCounts(w io.Writer, counts map[string]int64) error {
	bw := bufio.NewWriter(w)
	for _, term := range sortedKeys(counts) {
		fmt.Fprintf(bw, "%s\t%d\n", term, counts[term])
	}
	return bw.Flush()
}

// WriteIndex writes "term\tdoc_freq\tdoc1,doc2,..." lines sorted by term.
func WriteIndex(w io.Writer, index map[string][]string) error {
	bw := bufio.NewWriter(w)
	for _, term := range sortedKeys(index) {
		docs := index[term]
		fmt.Fprintf(bw, "%s\t%d\t%s\n", term, len(docs), strings.Join(docs, ","))
	}
	return bw.Flush()
}

// ReadCounts parses the output of WriteCounts.
func ReadCounts(r io.Reader) (map[string]int64, error) {
	counts := make(map[string]int64)
	err := readTSV(r, 2, func(fields []string) error {
		n, err := strconv.ParseInt(fields[1], 10, 64)
		counts[fields[0]] = n
		return err
	})
	return counts, err
}

// ReadIndex parses the output of WriteIndex.
func ReadIndex(r io.Reader) (map[string][]string, error) {
	index := make(map[string][]string)
	err := readTSV(r, 3, func(fields []string) error {
		docs := strings.Split(fields[2], ",")
		if n, err := strconv.Atoi(fields[1]); err != nil || n != len(docs) {
			return fmt.Errorf("term %q: doc_freq %s does not match %d documents", fields[0], fields[1], len(docs))
		}
		index[fields[0]] = docs
		return nil
	})
	return index, err
}

func readTSV(r io.Reader, nfields int, fn func([]string) error) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 1<<20), maxLine)
	for line := 1; sc.Scan(); line++ {
		if sc.Text() == "" {
			continue
		}
		fields := strings.Split(sc.Text(), "\t")
		if len(fields) != nfields {
			return fmt.Errorf("line %d: want %d tab-separated fields, got %d", line, nfields, len(fields))
		}
		if err := fn(fields); err != nil {
			return fmt.Errorf("line %d: %w", line, err)
		}
	}
	return sc.Err()
}

// DiffCounts describes up to max differences between want and got.
func DiffCounts(want, got map[string]int64, max int) []string {
	var diffs []string
	for _, term := range unionKeys(want, got) {
		w, inWant := want[term]
		g, inGot := got[term]
		switch {
		case !inGot:
			diffs = append(diffs, fmt.Sprintf("%q: missing (want %d)", term, w))
		case !inWant:
			diffs = append(diffs, fmt.Sprintf("%q: unexpected (got %d)", term, g))
		case w != g:
			diffs = append(diffs, fmt.Sprintf("%q: got %d, want %d", term, g, w))
		}
		if len(diffs) == max {
			break
		}
	}
	return diffs
}

// DiffIndex describes up to max differences between want and got.
func DiffIndex(want, got map[string][]string, max int) []string {
	var diffs []string
	for _, term := range unionKeys(want, got) {
		w, inWant := want[term]
		g, inGot := got[term]
		switch {
		case !inGot:
			diffs = append(diffs, fmt.Sprintf("%q: missing (want %v)", term, w))
		case !inWant:
			diffs = append(diffs, fmt.Sprintf("%q: unexpected (got %v)", term, g))
		case strings.Join(w, ",") != strings.Join(g, ","):
			diffs = append(diffs, fmt.Sprintf("%q: got %v, want %v", term, g, w))
		}
		if len(diffs) == max {
			break
		}
	}
	return diffs
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func unionKeys[V any](a, b map[string]V) []string {
	keys := sortedKeys(a)
	for k := range b {
		if _, ok := a[k]; !ok {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys
}
