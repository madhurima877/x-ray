// Package keywords matches a message against the keyword list (step 7) with
// an in-memory Aho-Corasick automaton, rebuilt in the background.
package keywords

import (
	"strings"
	"unicode"
)

// Matcher is an immutable Aho-Corasick automaton. It is safe for concurrent
// use; a nil *Matcher matches nothing.
type Matcher struct {
	nodes    []node
	patterns []string
}

type node struct {
	next map[rune]int32
	fail int32
	out  []int32 // indexes into patterns that end here
}

// Build compiles the keywords. Each keyword is cleaned the same way as the
// text, so "F.R.E.E" and "free" are the same keyword.
func Build(keywords []string) *Matcher {
	m := &Matcher{nodes: []node{{next: map[rune]int32{}}}}
	seen := map[string]bool{}
	for _, k := range keywords {
		k = Clean(k)
		if k == "" || seen[k] {
			continue
		}
		seen[k] = true
		m.add(k)
	}
	m.link()
	return m
}

func (m *Matcher) add(p string) {
	cur := int32(0)
	for _, r := range p {
		nxt, ok := m.nodes[cur].next[r]
		if !ok {
			m.nodes = append(m.nodes, node{next: map[rune]int32{}})
			nxt = int32(len(m.nodes) - 1)
			m.nodes[cur].next[r] = nxt
		}
		cur = nxt
	}
	m.nodes[cur].out = append(m.nodes[cur].out, int32(len(m.patterns)))
	m.patterns = append(m.patterns, p)
}

// link sets failure links breadth-first and merges outputs along them.
func (m *Matcher) link() {
	queue := make([]int32, 0, len(m.nodes))
	for _, child := range m.nodes[0].next {
		queue = append(queue, child)
	}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for r, child := range m.nodes[cur].next {
			f := m.nodes[cur].fail
			for f != 0 {
				if _, ok := m.nodes[f].next[r]; ok {
					break
				}
				f = m.nodes[f].fail
			}
			if nxt, ok := m.nodes[f].next[r]; ok && nxt != child {
				m.nodes[child].fail = nxt
			}
			m.nodes[child].out = append(m.nodes[child].out, m.nodes[m.nodes[child].fail].out...)
			queue = append(queue, child)
		}
	}
}

// Find returns the keywords found in text as whole words, each once, in
// order of first appearance.
func (m *Matcher) Find(text string) []string {
	if m == nil || len(m.patterns) == 0 {
		return nil
	}
	rs := []rune(Clean(text))
	var found []string
	seen := map[int32]bool{}
	cur := int32(0)
	for i, r := range rs {
		for {
			if nxt, ok := m.nodes[cur].next[r]; ok {
				cur = nxt
				break
			}
			if cur == 0 {
				break
			}
			cur = m.nodes[cur].fail
		}
		for _, p := range m.nodes[cur].out {
			if seen[p] {
				continue
			}
			start := i - len([]rune(m.patterns[p])) + 1
			if (start == 0 || rs[start-1] == ' ') && (i == len(rs)-1 || rs[i+1] == ' ') {
				seen[p] = true
				found = append(found, m.patterns[p])
			}
		}
	}
	return found
}

// Len is the number of distinct keywords.
func (m *Matcher) Len() int {
	if m == nil {
		return 0
	}
	return len(m.patterns)
}

var leet = map[rune]rune{'0': 'o', '1': 'i', '3': 'e', '4': 'a', '5': 's', '7': 't', '@': 'a', '$': 's'}

// Clean lower-cases text, undoes common look-alike swaps (fr33 → free),
// drops zero-width and dot/dash separators inside words ("f.r.e.e"), and
// turns everything else that is not a letter or digit into single spaces.
func Clean(text string) string {
	var b strings.Builder
	b.Grow(len(text))
	space := true
	for _, r := range text {
		if l, ok := leet[r]; ok {
			r = l
		}
		r = unicode.ToLower(r)
		switch {
		case unicode.IsLetter(r) || unicode.IsDigit(r):
			b.WriteRune(r)
			space = false
		case r == '.' || r == '-' || r == '_' || r == '*' || unicode.Is(unicode.Cf, r) || unicode.Is(unicode.Mn, r):
			// separator used to break up a word: skip it
		default:
			if !space {
				b.WriteByte(' ')
				space = true
			}
		}
	}
	return strings.TrimRight(b.String(), " ")
}
