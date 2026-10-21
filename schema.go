package flexpg

import (
	"database/sql"
	"strings"
	"unicode"

	"github.com/lib/pq"
)

type schemaReader interface {
	Query(string, ...interface{}) (*sql.Rows, error)
	QueryRow(string, ...interface{}) *sql.Row
}

func (c *Connection) schemaReader() schemaReader {
	if c.IsTx() {
		return c.tx
	}
	return c.db
}

// Resolve names using PostgreSQL's search_path, including qualified names.
func (c *Connection) tableIdentity(name string) (schema, table string, err error) {
	err = c.schemaReader().QueryRow(`SELECT n.nspname, c.relname
		FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		WHERE c.oid = pg_catalog.to_regclass($1)`, name).Scan(&schema, &table)
	return
}

func quotePGIdentifier(name string) string { return pq.QuoteIdentifier(name) }

func quotePGName(schema, name string) string {
	return quotePGIdentifier(schema) + "." + quotePGIdentifier(name)
}

type pgIndexMetadata struct {
	unique, valid, ready bool
	method, definition   string
}

func (c *Connection) indexMetadata(table, name string) (metadata pgIndexMetadata, err error) {
	err = c.schemaReader().QueryRow(`SELECT i.indisunique, i.indisvalid, i.indisready,
		am.amname, pg_catalog.pg_get_indexdef(i.indexrelid)
		FROM pg_catalog.pg_index i
		JOIN pg_catalog.pg_class ic ON ic.oid = i.indexrelid
		JOIN pg_catalog.pg_am am ON am.oid = ic.relam
		WHERE i.indrelid = pg_catalog.to_regclass($1) AND ic.relname = $2`, table, name).
		Scan(&metadata.unique, &metadata.valid, &metadata.ready, &metadata.method, &metadata.definition)
	return
}

func (m pgIndexMetadata) matches(unique bool, method string, fields []string) bool {
	if m.unique != unique || m.method != method || !m.valid || !m.ready {
		return false
	}
	marker := " USING " + method + " "
	start := strings.Index(m.definition, marker)
	if start < 0 {
		return false
	}
	actual := m.definition[start+len(marker):]
	expected := "(" + strings.Join(fields, ", ") + ")"
	return normalizeIndexDefinition(actual) == normalizeIndexDefinition(expected)
}

// Ignore SQL formatting and unquoted identifier case, while preserving quoted
// identifier case, JSON keys, expression grouping, opclasses and sort options.
func normalizeIndexDefinition(definition string) string {
	type sqlToken struct {
		text   string
		quoted bool
	}
	tokens := []sqlToken{}
	for i := 0; i < len(definition); {
		ch := definition[i]
		if unicode.IsSpace(rune(ch)) {
			i++
			continue
		}
		start := i
		if ch == '\'' || ch == '"' {
			i++
			for i < len(definition) {
				if definition[i] == ch {
					i++
					if i < len(definition) && definition[i] == ch {
						i++
						continue
					}
					break
				}
				i++
			}
			token := definition[start:i]
			if ch == '"' && len(token) >= 2 && token[len(token)-1] == '"' {
				identifier := strings.ReplaceAll(token[1:len(token)-1], `""`, `"`)
				if simplePGIdentifier(identifier) {
					token = identifier
				}
			}
			tokens = append(tokens, sqlToken{text: token, quoted: true})
		} else if identifierByte(ch) {
			i++
			for i < len(definition) && identifierByte(definition[i]) {
				i++
			}
			tokens = append(tokens, sqlToken{text: strings.ToLower(definition[start:i])})
		} else {
			tokens = append(tokens, sqlToken{text: string(ch)})
			i++
		}
	}
	// PostgreSQL omits these default ordering clauses in pg_get_indexdef.
	result := []string{}
	descending := false
	for i := 0; i < len(tokens); i++ {
		token := tokens[i].text
		if token == "," {
			descending = false
		}
		if !tokens[i].quoted && token == "asc" {
			continue
		}
		if !tokens[i].quoted && token == "desc" {
			descending = true
		}
		if !tokens[i].quoted && token == "nulls" && i+1 < len(tokens) && !tokens[i+1].quoted &&
			((descending && tokens[i+1].text == "first") || (!descending && tokens[i+1].text == "last")) {
			i++
			continue
		}
		result = append(result, token)
	}
	return strings.Join(result, "\x00")
}

func identifierByte(ch byte) bool {
	return ch == '_' || ch == '$' || ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch >= 128
}

func simplePGIdentifier(name string) bool {
	if name == "" || !(name[0] == '_' || name[0] >= 'a' && name[0] <= 'z') {
		return false
	}
	for i := range name {
		if !identifierByte(name[i]) || name[i] >= 'A' && name[i] <= 'Z' {
			return false
		}
	}
	return true
}
