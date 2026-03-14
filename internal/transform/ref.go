package transform

import "regexp"

// refPattern matches {{ ref "model_name" }} with optional whitespace and trim markers.
var refPattern = regexp.MustCompile(`\{\{-?\s*ref\s+"([^"]+)"\s*-?\}\}`)

// ExtractRefs parses a SQL template string and returns all unique model names
// referenced via {{ ref "name" }} calls, in order of first appearance.
func ExtractRefs(sql string) []string {
	matches := refPattern.FindAllStringSubmatch(sql, -1)
	if len(matches) == 0 {
		return nil
	}
	seen := make(map[string]bool)
	var refs []string
	for _, m := range matches {
		name := m[1]
		if !seen[name] {
			seen[name] = true
			refs = append(refs, name)
		}
	}
	return refs
}
