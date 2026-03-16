package transform

import "regexp"

// refPattern matches {{ ref "model_name" }} or {{ ref 'model_name' }} with
// optional whitespace and trim markers. Group 1 captures the name for
// double-quoted refs; group 2 captures it for single-quoted refs. RE2 does
// not support backreferences, so both quote styles are handled via alternation.
var refPattern = regexp.MustCompile(`\{\{-?\s*ref\s+(?:"([^"]+)"|'([^']+)')\s*-?\}\}`)

// thisPattern matches {{ this }} with optional whitespace and trim markers.
var thisPattern = regexp.MustCompile(`\{\{-?\s*this\s*-?\}\}`)

// ExtractRefs parses a SQL template string and returns all unique model names
// referenced via {{ ref "name" }} or {{ ref 'name' }} calls, in order of
// first appearance.
func ExtractRefs(sql string) []string {
	matches := refPattern.FindAllStringSubmatch(sql, -1)
	if len(matches) == 0 {
		return nil
	}
	seen := make(map[string]bool)
	var refs []string
	for _, m := range matches {
		// Group 1 is set for double-quoted refs, group 2 for single-quoted.
		name := m[1]
		if name == "" {
			name = m[2]
		}
		if !seen[name] {
			seen[name] = true
			refs = append(refs, name)
		}
	}
	return refs
}
