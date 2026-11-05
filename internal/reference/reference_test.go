package reference

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

const (
	smallCorpus        = "../../testdata/small/corpus"
	smallWordCount     = "../../testdata/small/expected/wordcount.tsv"
	smallInvertedIndex = "../../testdata/small/expected/invertedindex.tsv"
)

func readGolden[T any](t *testing.T, path string, parse func(*os.File) (T, error)) T {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	v, err := parse(f)
	if err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	return v
}

func smallFiles(t *testing.T) []string {
	t.Helper()
	files, err := InputFiles(smallCorpus)
	if err != nil {
		t.Fatal(err)
	}
	return files
}

func TestInputFilesSkipsHiddenAndMetadata(t *testing.T) {
	var names []string
	for _, f := range smallFiles(t) {
		names = append(names, filepath.Base(f))
	}
	want := []string{"part-00000.tsv", "part-00001.tsv"}
	if !reflect.DeepEqual(names, want) {
		t.Fatalf("InputFiles = %v, want %v", names, want)
	}
}

func TestWordCountSmallCorpus(t *testing.T) {
	got, err := WordCount(smallFiles(t))
	if err != nil {
		t.Fatal(err)
	}
	want := readGolden(t, smallWordCount, func(f *os.File) (map[string]int64, error) { return ReadCounts(f) })
	if diffs := DiffCounts(want, got, 10); len(diffs) > 0 {
		t.Fatalf("word count differs from %s:\n%v", smallWordCount, diffs)
	}
	if got.Tokens() != 42 {
		t.Errorf("Tokens() = %d, want 42", got.Tokens())
	}
}

func TestInvertedIndexSmallCorpus(t *testing.T) {
	ix, err := InvertedIndex(smallFiles(t))
	if err != nil {
		t.Fatal(err)
	}
	want := readGolden(t, smallInvertedIndex, func(f *os.File) (map[string][]string, error) { return ReadIndex(f) })
	if diffs := DiffIndex(want, ix.Map(), 10); len(diffs) > 0 {
		t.Fatalf("inverted index differs from %s:\n%v", smallInvertedIndex, diffs)
	}
	if ix.TotalPostings() != 33 {
		t.Errorf("TotalPostings() = %d, want 33", ix.TotalPostings())
	}
	// The line without a tab is a document with no terms, so it is counted
	// as a document id but never appears in a posting list.
	if !reflect.DeepEqual(ix.Docs, []string{"doc1", "doc2", "doc3", "doc4", "this line has no tab separator"}) {
		t.Errorf("Docs = %q", ix.Docs)
	}
}

func TestInvertedIndexNonContiguousDocuments(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "part-00000.tsv")
	data := "b\tfox\na\tfox dog\nb\tfox\na\tdog\nc\tfox fox\n"
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
	ix, err := InvertedIndex([]string{path})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string][]string{"fox": {"a", "b", "c"}, "dog": {"a"}}
	if diffs := DiffIndex(want, ix.Map(), 10); len(diffs) > 0 {
		t.Fatal(diffs)
	}
}

func TestWriteReadRoundTrip(t *testing.T) {
	counts := map[string]int64{"the": 6, "2026": 1}
	var buf bytes.Buffer
	if err := WriteCounts(&buf, counts); err != nil {
		t.Fatal(err)
	}
	if buf.String() != "2026\t1\nthe\t6\n" {
		t.Fatalf("WriteCounts wrote %q", buf.String())
	}
	back, err := ReadCounts(&buf)
	if err != nil || !reflect.DeepEqual(back, counts) {
		t.Fatalf("ReadCounts = %v, %v", back, err)
	}

	index := map[string][]string{"fox": {"doc1", "doc2"}}
	buf.Reset()
	if err := WriteIndex(&buf, index); err != nil {
		t.Fatal(err)
	}
	if buf.String() != "fox\t2\tdoc1,doc2\n" {
		t.Fatalf("WriteIndex wrote %q", buf.String())
	}
	backIndex, err := ReadIndex(&buf)
	if err != nil || !reflect.DeepEqual(backIndex, index) {
		t.Fatalf("ReadIndex = %v, %v", backIndex, err)
	}
}

func TestDiff(t *testing.T) {
	want := map[string]int64{"a": 1, "b": 2}
	got := map[string]int64{"b": 3, "c": 1}
	diffs := DiffCounts(want, got, 0)
	if len(diffs) != 3 {
		t.Fatalf("DiffCounts = %v, want 3 differences", diffs)
	}
	if diffs := DiffCounts(want, got, 1); len(diffs) != 1 {
		t.Fatalf("DiffCounts with max 1 = %v", diffs)
	}
}
