package flexpg

import (
	"git.kanosolution.net/kano/dbflex"
)

// type JoinTable struct {
// 	Table string
// 	On    *dbflex.Filter
// }

// QueryParam extend from dbflex.QueryParam, adding helper wrap and query
type QueryParam struct {
	dbflex.QueryParam
	// Joins []JoinTable
}

// NewQueryParam create new QueryParam
func NewQueryParam() *QueryParam {
	return new(QueryParam)
}

// func (qp *QueryParam) Join(j JoinTable) *QueryParam {
// 	if len(qp.Joins) == 0 {
// 		qp.Joins = []JoinTable{}
// 	}
// 	qp.Joins = append(qp.Joins, j)
// 	return qp
// }

func (qp *QueryParam) ToSelectFields() string {
	q := Query{}
	return q.BuildPartial(dbflex.QuerySelect, qp.QueryParam.Select)
}

func (qp *QueryParam) ToSQLWhere() string {
	q := Query{}
	i, _ := q.BuildFilter(qp.QueryParam.Where)
	if i.(string) != "" {
		return "WHERE " + i.(string)
	}
	return ""
}

func (qp *QueryParam) ToSQLSort() string {
	q := Query{}
	return q.BuildPartial(dbflex.QueryOrder, qp.QueryParam.Sort)
}

func (qp *QueryParam) ToSQLTake() string {
	if qp.QueryParam.Take == 0 {
		return ""
	}
	q := Query{}
	return q.BuildPartial(dbflex.QueryTake, qp.QueryParam.Take)
}

func (qp *QueryParam) ToSQLSkip() string {
	q := Query{}
	return q.BuildPartial(dbflex.QuerySkip, qp.QueryParam.Skip)
}

func (qp *QueryParam) ToSQLGroup() string {
	q := Query{}
	return q.BuildPartial(dbflex.QueryGroup, qp.QueryParam.GroupBy)
}

// func (qp *QueryParam) BuildTemplate(, k string, v string) string {
// 	q := c.NewQuery()
// 	commands := q.This().(rdbms.RdbmsQuery).Templates()
// 	return executeTemplate(commands[k],
// 		toolkit.M{}.Set(dbflex.QueryOrder, v))
// }
