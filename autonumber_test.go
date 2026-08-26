package flexpg

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"git.kanosolution.net/kano/dbflex"
	"github.com/sebarcode/codekit"
)

const autoValueTestDriverName = "flexpg-auto-value-test"

var (
	autoValueTestDriverOnce sync.Once
	autoValueTestQueryMu    sync.Mutex
	autoValueTestQuery      string
)

type autoValueTestDriver struct{}

func (autoValueTestDriver) Open(string) (driver.Conn, error) {
	return autoValueTestSQLConn{}, nil
}

type autoValueTestSQLConn struct{}

func (autoValueTestSQLConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare is not implemented")
}

func (autoValueTestSQLConn) Close() error {
	return nil
}

func (autoValueTestSQLConn) Begin() (driver.Tx, error) {
	return nil, errors.New("transactions are not implemented")
}

func (autoValueTestSQLConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	autoValueTestQueryMu.Lock()
	autoValueTestQuery = query
	autoValueTestQueryMu.Unlock()

	columns := []string{"id"}
	values := []driver.Value{int64(501)}
	if strings.Contains(query, `"code"`) {
		columns = append(columns, "code")
		values = append(values, "AUTO-501")
	}
	return &autoValueTestRows{columns: columns, values: values}, nil
}

type autoValueTestRows struct {
	columns []string
	values  []driver.Value
	read    bool
}

func (r *autoValueTestRows) Columns() []string {
	return r.columns
}

func (r *autoValueTestRows) Close() error {
	return nil
}

func (r *autoValueTestRows) Next(dest []driver.Value) error {
	if r.read {
		return io.EOF
	}
	copy(dest, r.values)
	r.read = true
	return nil
}

func TestGetAutoValReturnsAndConsumesGeneratedValues(t *testing.T) {
	conn := new(Connection)
	conn.setAutoValues("items", []string{"id", "code"}, []interface{}{int64(42), "AUTO-42"})

	id, err := conn.GetAutoVal("items", "id")
	if err != nil {
		t.Fatalf("get generated id failed: %v", err)
	}
	if id != int64(42) {
		t.Fatalf("unexpected generated id: %#v", id)
	}

	code, err := conn.GetAutoVal("items", "code")
	if err != nil {
		t.Fatalf("get generated code failed: %v", err)
	}
	if code != "AUTO-42" {
		t.Fatalf("unexpected generated code: %#v", code)
	}

	if _, err := conn.GetAutoVal("items", "id"); err == nil {
		t.Fatal("generated value should be consumed after it is read")
	}
}

func TestReturningClauseQuotesFieldNames(t *testing.T) {
	clause := returningClause([]string{"id", "generated-code"})
	if clause != ` RETURNING "id","generated-code"` {
		t.Fatalf("unexpected returning clause: %s", clause)
	}
	if strings.Contains(clause, `RETURNING id`) {
		t.Fatalf("returning field names should be quoted: %s", clause)
	}
}

func TestInsertResultExposesFirstGeneratedInteger(t *testing.T) {
	result := insertResult{firstAutoValue: int64(73)}
	id, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("last insert id failed: %v", err)
	}
	if id != 73 {
		t.Fatalf("unexpected last insert id: %d", id)
	}
	rows, err := result.RowsAffected()
	if err != nil || rows != 1 {
		t.Fatalf("unexpected rows affected: %d, %v", rows, err)
	}
}

func TestExecuteInsertReturningCapturesGeneratedValues(t *testing.T) {
	conn := newAutoValueSQLConnection(t)
	payload := codekit.M{}.
		Set("data", codekit.M{}.Set("name", "sample")).
		Set(dbflex.DataKeyAutoFields, []string{"id", "code"})

	result, err := conn.Execute(dbflex.From("items").Insert(), payload)
	if err != nil {
		t.Fatalf("insert returning failed: %v", err)
	}
	query := lastAutoValueTestQuery()
	if !strings.Contains(query, `INSERT INTO items (name) VALUES ('sample')`) {
		t.Fatalf("unexpected insert query: %s", query)
	}
	if !strings.Contains(query, `RETURNING "id","code"`) {
		t.Fatalf("insert query does not return auto fields: %s", query)
	}

	id, err := conn.GetAutoVal("items", "id")
	if err != nil || id != int64(501) {
		t.Fatalf("unexpected generated id: %#v, %v", id, err)
	}
	code, err := conn.GetAutoVal("items", "code")
	if err != nil || code != "AUTO-501" {
		t.Fatalf("unexpected generated code: %#v, %v", code, err)
	}
	lastID, err := result.(sql.Result).LastInsertId()
	if err != nil || lastID != 501 {
		t.Fatalf("unexpected sql result ID: %d, %v", lastID, err)
	}
}

func TestExecuteAutoOnlyInsertUsesDefaultValues(t *testing.T) {
	conn := newAutoValueSQLConnection(t)
	payload := codekit.M{}.
		Set("data", codekit.M{}).
		Set(dbflex.DataKeyAutoFields, []string{"id"})

	if _, err := conn.Execute(dbflex.From("items").Insert(), payload); err != nil {
		t.Fatalf("default-values insert failed: %v", err)
	}
	query := lastAutoValueTestQuery()
	if !strings.Contains(query, `INSERT INTO items DEFAULT VALUES RETURNING "id"`) {
		t.Fatalf("unexpected default-values query: %s", query)
	}
}

func newAutoValueSQLConnection(t *testing.T) *Connection {
	t.Helper()
	autoValueTestDriverOnce.Do(func() {
		sql.Register(autoValueTestDriverName, autoValueTestDriver{})
	})
	db, err := sql.Open(autoValueTestDriverName, "")
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	conn := &Connection{db: db}
	conn.SetThis(conn)
	return conn
}

func lastAutoValueTestQuery() string {
	autoValueTestQueryMu.Lock()
	defer autoValueTestQueryMu.Unlock()
	return autoValueTestQuery
}
