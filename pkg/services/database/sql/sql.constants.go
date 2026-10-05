package sql

import "regexp"

var (
	nonNameChars   = regexp.MustCompile(`[^a-z0-9]+`)
	addConstraint  = regexp.MustCompile(`ALTER TABLE ((?:"[^"]+"\.)?"[^"]+") ADD CONSTRAINT "([^"]+)"`)
	dropUnnamedFK  = regexp.MustCompile(`ALTER TABLE ((?:"[^"]+"\.)?"[^"]+") DROP CONSTRAINT ""`)
	unqualifiedTbl = regexp.MustCompile(`^"public"\.`)
)
