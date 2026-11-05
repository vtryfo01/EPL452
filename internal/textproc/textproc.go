// Package textproc defines the corpus record format and the tokenization
// rules shared by the Spark Connect jobs (internal/jobs), the local reference
// implementation (internal/reference) and the PySpark baseline.
//
// Record format: one record per line, "<doc_id>\t<text>". Only the first two
// tab-separated fields are used, exactly like Spark's split_part(value, '\t', n):
// a line without a tab is a document id with empty text.
//
// Tokenization: lower-case the text, split it on every run of characters
// outside [a-z0-9] and drop empty strings. In Spark this is
//
//	explode(split(lower(text), '[^a-z0-9]+'))  filtered by term != ''
//
// The preprocessing scripts (scripts/data) fold all text to ASCII, so the Go,
// Spark and Python implementations agree byte for byte on our datasets.
package textproc

import "strings"

// TokenPattern is the regular expression (valid in Java, Go and Python) that
// separates terms after lower-casing.
const TokenPattern = "[^a-z0-9]+"

// ParseRecord splits a corpus line into its document id and text.
func ParseRecord(line string) (docID, text string) {
	docID, rest, found := strings.Cut(line, "\t")
	if !found {
		return line, ""
	}
	text, _, _ = strings.Cut(rest, "\t")
	return docID, text
}

// ForEachTerm calls fn for every term of text, in order.
func ForEachTerm(text string, fn func(term string)) {
	text = strings.ToLower(text)
	start := -1
	for i := 0; i < len(text); i++ {
		if isTermByte(text[i]) {
			if start < 0 {
				start = i
			}
			continue
		}
		if start >= 0 {
			fn(text[start:i])
			start = -1
		}
	}
	if start >= 0 {
		fn(text[start:])
	}
}

// Tokenize returns the terms of text.
func Tokenize(text string) []string {
	var terms []string
	ForEachTerm(text, func(term string) { terms = append(terms, term) })
	return terms
}

// IsTerm reports whether s could be produced by the tokenizer.
func IsTerm(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if !isTermByte(s[i]) {
			return false
		}
	}
	return true
}

func isTermByte(b byte) bool {
	return ('a' <= b && b <= 'z') || ('0' <= b && b <= '9')
}
