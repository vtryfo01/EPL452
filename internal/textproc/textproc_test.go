package textproc

import (
	"reflect"
	"regexp"
	"strings"
	"testing"
)

func TestParseRecord(t *testing.T) {
	tests := []struct {
		line, doc, text string
	}{
		{"doc1\tHello world", "doc1", "Hello world"},
		{"doc1\t", "doc1", ""},
		{"no tab here", "no tab here", ""},
		{"doc2\ttext\tignored third field", "doc2", "text"},
		{"\torphan text", "", "orphan text"},
	}
	for _, tt := range tests {
		doc, text := ParseRecord(tt.line)
		if doc != tt.doc || text != tt.text {
			t.Errorf("ParseRecord(%q) = (%q, %q), want (%q, %q)", tt.line, doc, text, tt.doc, tt.text)
		}
	}
}

func TestTokenize(t *testing.T) {
	tests := []struct {
		text string
		want []string
	}{
		{"The quick brown fox.", []string{"the", "quick", "brown", "fox"}},
		{"  Hello, hello... HELLO?  ", []string{"hello", "hello", "hello"}},
		{"It's 2026 -- the fox's year.", []string{"it", "s", "2026", "the", "fox", "s", "year"}},
		{"Well-known e-mail: hello@example.com", []string{"well", "known", "e", "mail", "hello", "example", "com"}},
		{"A naïve dog", []string{"a", "na", "ve", "dog"}},
		{"", nil},
		{"...!!!", nil},
	}
	for _, tt := range tests {
		if got := Tokenize(tt.text); !reflect.DeepEqual(got, tt.want) {
			t.Errorf("Tokenize(%q) = %q, want %q", tt.text, got, tt.want)
		}
	}
}

// TestTokenizeMatchesRegexpSplit checks the hand-written scanner against the
// literal definition used by Spark: split(lower(text), TokenPattern) minus
// empty strings.
func TestTokenizeMatchesRegexpSplit(t *testing.T) {
	sep := regexp.MustCompile(TokenPattern)
	inputs := []string{
		"The DOG barks; the fox runs!",
		"\t\tTabs\tand  spaces ",
		"MiXeD123case 456",
		"__under_scores__",
		"ÀÉÎõü ascii-and-not",
		"trailing separator!",
	}
	for _, in := range inputs {
		var want []string
		for _, term := range sep.Split(strings.ToLower(in), -1) {
			if term != "" {
				want = append(want, term)
			}
		}
		if got := Tokenize(in); !reflect.DeepEqual(got, want) {
			t.Errorf("Tokenize(%q) = %q, regexp split gives %q", in, got, want)
		}
	}
}

func TestIsTerm(t *testing.T) {
	for _, s := range []string{"fox", "2026", "a1"} {
		if !IsTerm(s) {
			t.Errorf("IsTerm(%q) = false", s)
		}
	}
	for _, s := range []string{"", "Fox", "it's", "a b", "naïve"} {
		if IsTerm(s) {
			t.Errorf("IsTerm(%q) = true", s)
		}
	}
}
