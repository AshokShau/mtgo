package parser

import (
	"testing"
)

// --- Markdown parsing ---

func BenchmarkParseMarkdownSimple(b *testing.B) {
	text := "**bold** and _italic_ text"
	b.ReportAllocs()
	for b.Loop() {
		_, _, _ = Parse(ParseModeMarkdown, text)
	}
}

func BenchmarkParseMarkdownComplex(b *testing.B) {
	text := "**bold** _italic_ `code` ```pre``` [link](https://example.com) __underline__ ~~strike~~"
	b.ReportAllocs()
	for b.Loop() {
		_, _, _ = Parse(ParseModeMarkdown, text)
	}
}

// --- HTML parsing ---

func BenchmarkParseHTMLSimple(b *testing.B) {
	text := "<b>bold</b> and <i>italic</i> text"
	b.ReportAllocs()
	for b.Loop() {
		_, _, _ = Parse(ParseModeHTML, text)
	}
}

func BenchmarkParseHTMLComplex(b *testing.B) {
	text := `<b>bold</b> <i>italic</i> <code>code</code> <pre>pre</pre> <a href="https://example.com">link</a> <u>underline</u> <s>strike</s>`
	b.ReportAllocs()
	for b.Loop() {
		_, _, _ = Parse(ParseModeHTML, text)
	}
}

func BenchmarkParseHTMLNested(b *testing.B) {
	text := `<b>bold <i>and italic</i> bold</b> plain <a href="https://example.com">link with <b>bold</b></a>`
	b.ReportAllocs()
	for b.Loop() {
		_, _, _ = Parse(ParseModeHTML, text)
	}
}

// --- Plain text (no entities) ---

func BenchmarkParsePlain(b *testing.B) {
	text := "This is a plain text message with no formatting entities at all."
	b.ReportAllocs()
	for b.Loop() {
		_, _, _ = Parse(ParseModeDefault, text)
	}
}
