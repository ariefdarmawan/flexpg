package flexpg

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"git.kanosolution.net/kano/dbflex"
	"git.kanosolution.net/kano/dbflex/orm"
	"github.com/sebarcode/codekit"
)

func TestPostgresMetadataTypeAliases(t *testing.T) {
	for _, pair := range [][2]string{
		{"varchar(32)", "varchar"}, {"character varying(32)", "varchar"},
		{"timestamp with time zone", "timestamptz"}, {"timestamp(3) without time zone", "timestamp"},
		{"double precision", "float8"}, {"real", "float4"}, {"boolean", "bool"},
		{"integer[]", "_int4"}, {"text[]", "_text"}, {"time with time zone", "timetz"},
		{"integer[][]", "_int4"}, {"float", "float8"},
	} {
		if !pgTypesEquivalent(pair[0], pair[1]) {
			t.Errorf("equivalent types differ: %q and %q", pair[0], pair[1])
		}
	}
}

func TestPostgresFormattedTypeDefaults(t *testing.T) {
	for _, pair := range [][2]string{
		{"decimal(12)", "numeric(12,0)"}, {"float(24)", "real"},
		{"float(53)", "double precision"}, {"timestamp(3) with time zone", "timestamptz(3)"},
	} {
		if canonicalPGType(pair[0]) != canonicalPGType(pair[1]) {
			t.Errorf("equivalent formatted types differ: %q and %q", pair[0], pair[1])
		}
	}
}

func TestUpdatingTableComparesExplicitTypeModifiers(t *testing.T) {
	model := struct {
		Name string `json:"name" db_type:"varchar(32)"`
	}{}
	for _, length := range []int{32, 64} {
		commands, err := createCommandForUpdatingTableFields("items", model, []codekit.M{
			{"column_name": "name", "udt_name": "varchar", "formatted_type": fmt.Sprintf("character varying(%d)", length)},
		})
		if err != nil {
			t.Fatal(err)
		}
		if (len(commands) == 0) != (length == 32) {
			t.Fatalf("unexpected commands for varchar(%d): %v", length, commands)
		}
	}
}

func TestIndexMetadataComparison(t *testing.T) {
	base := pgIndexMetadata{valid: true, ready: true, method: "btree",
		definition: `CREATE INDEX items_lookup ON public.items USING btree ("name", id DESC)`}
	if !base.matches(false, "btree", []string{"NAME ASC NULLS LAST", "id DESC NULLS FIRST"}) {
		t.Fatal("formatting and default sort options should match")
	}
	for _, fields := range [][]string{{"id DESC", "name"}, {"name", "id"}, {"name NULLS FIRST", "id DESC"}} {
		if base.matches(false, "btree", fields) {
			t.Errorf("different index fields matched: %v", fields)
		}
	}
	if base.matches(true, "btree", []string{"name", "id DESC"}) || base.matches(false, "hash", []string{"name", "id DESC"}) {
		t.Fatal("different uniqueness or access method must not match")
	}
	for _, suffix := range []string{" WHERE id > 0", " INCLUDE (other)", " NULLS NOT DISTINCT"} {
		changed := base
		changed.definition += suffix
		if changed.matches(false, "btree", []string{"name", "id DESC"}) {
			t.Fatalf("different index options matched: %s", suffix)
		}
	}
	base.valid = false
	if base.matches(false, "btree", []string{"name", "id DESC"}) {
		t.Fatal("invalid indexes must not match")
	}
	quoted := pgIndexMetadata{valid: true, ready: true, method: "btree",
		definition: `CREATE INDEX items_quoted ON public.items USING btree (first, "desc")`}
	if !quoted.matches(false, "btree", []string{`"first"`, `"desc"`}) {
		t.Fatal("quoted column names must preserve their meaning")
	}
}

type ensureMetadataModel struct {
	orm.DataModelBase `json:"-"`
	ID                int64     `json:"id" key:"1" db_auto:"1"`
	Name              string    `json:"name" db_type:"varchar(32)"`
	Created           time.Time `json:"created" db_type:"timestamp with time zone"`
	Amount            float64   `json:"amount" db_type:"decimal(12,2)"`
	Whole             float64   `json:"whole" db_type:"numeric(12)"`
	Approx            float64   `json:"approx" db_type:"float(24)"`
	Payload           codekit.M `json:"payload"`
	table             string    `json:"-"`
}

func (m *ensureMetadataModel) TableName() string { return m.table }

func (*ensureMetadataModel) Indexes() []dbflex.DbIndex {
	return []dbflex.DbIndex{
		{Name: "lookup", Fields: []string{"name", "id DESC"}},
		{Name: "unique", IsUnique: true, Fields: []string{"id", "name"}},
		{Name: "json", Fields: []string{"payload.Nested.Code"}},
		{Name: "json_one", Fields: []string{"payload.Code"}},
		{Name: strings.Repeat("long", 20), Fields: []string{"created"}},
	}
}

// FLEXPG_TEST_DSN must point to a disposable PostgreSQL database with permission
// to create schemas, pg_trgm and event triggers. No production database is used.
func TestSchemaEnsureIdempotentPostgres(t *testing.T) {
	dsn := os.Getenv("FLEXPG_TEST_DSN")
	if dsn == "" {
		t.Skip("set FLEXPG_TEST_DSN to run PostgreSQL schema integration tests")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn := &Connection{db: db}
	conn.SetThis(conn)
	exec := func(command string) {
		t.Helper()
		if _, err := db.Exec(command); err != nil {
			t.Fatalf("%s: %v", command, err)
		}
	}
	schema := fmt.Sprintf("flexpg_ensure_%d", time.Now().UnixNano())
	otherSchema := schema + "_other"
	trigger := schema + "_ddl"
	exec("CREATE SCHEMA " + schema)
	defer db.Exec("DROP SCHEMA " + schema + " CASCADE")
	exec("CREATE SCHEMA " + otherSchema)
	defer db.Exec("DROP SCHEMA " + otherSchema + " CASCADE")
	exec("CREATE EXTENSION IF NOT EXISTS pg_trgm")
	exec("CREATE TABLE " + schema + ".ddl_events (command text)")
	exec("CREATE FUNCTION " + schema + `.record_ddl() RETURNS event_trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO ` + schema + `.ddl_events VALUES (tg_tag); END $$`)
	exec("CREATE EVENT TRIGGER " + trigger + " ON ddl_command_end EXECUTE FUNCTION " + schema + ".record_ddl()")
	defer db.Exec("DROP EVENT TRIGGER " + trigger)
	model := &ensureMetadataModel{table: schema + ".items"}
	if err := orm.EnsureDb(conn, model); err != nil {
		t.Fatal(err)
	}
	// A same-named table and index in another schema must not affect comparison.
	exec("CREATE TABLE " + otherSchema + ".items (id text, name int)")
	exec("CREATE INDEX items_lookup ON " + otherSchema + ".items (name)")
	exec("TRUNCATE " + schema + ".ddl_events")
	assertNoDDL := func() {
		t.Helper()
		var count int
		if err := db.QueryRow("SELECT count(*) FROM " + schema + ".ddl_events").Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("matching metadata caused %d DDL commands", count)
		}
	}
	for i := 0; i < 2; i++ {
		if err := orm.EnsureDb(conn, model); err != nil {
			t.Fatal(err)
		}
	}
	assertNoDDL()
	// Default ordering clauses and a reused slice must remain idempotent.
	fields := []string{"payload.Nested.Code"}
	if err := conn.EnsureIndex(model.table, "json", false, fields...); err != nil {
		t.Fatal(err)
	}
	if fields[0] != "payload.Nested.Code" {
		t.Fatalf("EnsureIndex changed the caller's fields: %v", fields)
	}
	if err := conn.EnsureIndex(model.table, "lookup", false, "NAME ASC NULLS LAST", "id DESC NULLS FIRST"); err != nil {
		t.Fatal(err)
	}
	assertNoDDL()
	// Metadata must also see uncommitted tables/indexes in the active transaction.
	if err := conn.BeginTx(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if conn.IsTx() {
			_ = conn.RollBack()
		}
	}()
	if _, err := conn.tx.Exec("SET LOCAL search_path TO " + schema + ", public"); err != nil {
		t.Fatal(err)
	}
	model.table = "items"
	if err := orm.EnsureDb(conn, model); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := conn.tx.QueryRow("SELECT count(*) FROM " + schema + ".ddl_events").Scan(&count); err != nil || count != 0 {
		t.Fatalf("search_path metadata caused DDL: count=%d, error=%v", count, err)
	}
	model.table = schema + ".transaction_items"
	for i := 0; i < 2; i++ {
		if err := orm.EnsureDb(conn, model); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			if _, err := conn.tx.Exec("TRUNCATE " + schema + ".ddl_events"); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := conn.tx.QueryRow("SELECT count(*) FROM " + schema + ".ddl_events").Scan(&count); err != nil || count != 0 {
		t.Fatalf("uncommitted metadata caused repeated DDL: count=%d, error=%v", count, err)
	}
	if err := conn.RollBack(); err != nil {
		t.Fatal(err)
	}
	assertNoDDL()
	// A real field-order change must rebuild once, then become a no-op.
	model.table = schema + ".items"
	if err := conn.EnsureIndex(model.table, "lookup", false, "id", "name"); err != nil {
		t.Fatal(err)
	}
	var events string
	if err := db.QueryRow("SELECT string_agg(command, ',') FROM " + schema + ".ddl_events").Scan(&events); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(events, "DROP INDEX") || !strings.Contains(events, "CREATE INDEX") {
		t.Fatalf("a changed index was not rebuilt: %s", events)
	}
	exec("TRUNCATE " + schema + ".ddl_events")
	if err := conn.EnsureIndex(model.table, "lookup", false, "id", "name"); err != nil {
		t.Fatal(err)
	}
	assertNoDDL()
	// Changes to uniqueness and JSON keys are specifications too.
	for _, change := range []struct {
		name   string
		unique bool
		fields []string
	}{
		{"lookup", true, []string{"id", "name"}},
		{"json", false, []string{"payload.Nested.code"}},
		{"json", false, []string{"name"}},
	} {
		if err := conn.EnsureIndex(model.table, change.name, change.unique, change.fields...); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRow("SELECT count(*) FROM " + schema + ".ddl_events").Scan(&count); err != nil || count != 2 {
			t.Fatalf("changed specification should rebuild once: count=%d, error=%v", count, err)
		}
		exec("TRUNCATE " + schema + ".ddl_events")
		if err := conn.EnsureIndex(model.table, change.name, change.unique, change.fields...); err != nil {
			t.Fatal(err)
		}
		assertNoDDL()
	}
	// An explicit column-length change must also settle after one ALTER.
	small := struct {
		Name string `json:"name" db_type:"varchar(32)"`
	}{}
	large := struct {
		Name string `json:"name" db_type:"varchar(64)"`
	}{}
	if err := conn.EnsureTable(schema+".resized", nil, small); err != nil {
		t.Fatal(err)
	}
	exec("TRUNCATE " + schema + ".ddl_events")
	if err := conn.EnsureTable(schema+".resized", nil, large); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT count(*) FROM " + schema + ".ddl_events").Scan(&count); err != nil || count != 1 {
		t.Fatalf("column-length change should ALTER once: count=%d, error=%v", count, err)
	}
	exec("TRUNCATE " + schema + ".ddl_events")
	if err := conn.EnsureTable(schema+".resized", nil, large); err != nil {
		t.Fatal(err)
	}
	assertNoDDL()
}
