package parser

import (
	"fmt"
	"github.com/tkuchiki/mysql-parser/query"
	"reflect"

	"github.com/knocknote/vitess-sqlparser/sqlparser"
)

type Parser struct {
	query *query.Query
}

func New() *Parser {
	query := query.New()
	return &Parser{
		query: query,
	}
}

func (p *Parser) parseSelectStmt(stmt *sqlparser.Select, q *query.Query) error {
	for _, tableExpr := range stmt.From {
		if err := p.parseTableExpr(stmt, tableExpr, q); err != nil {
			return err
		}
	}

	for _, selectExpr := range stmt.SelectExprs {
		switch expr := selectExpr.(type) {
		case *sqlparser.StarExpr:
			if err := p.parseStarExpr(expr, q); err != nil {
				return err
			}
		case *sqlparser.AliasedExpr:
			if err := p.parseAliasedExpr(expr, q); err != nil {
				return err
			}
		}
	}

	if stmt.Where == nil {
		return nil
	}

	if err := p.parseWhere(stmt.Where, q); err != nil {
		return err
	}

	return nil
}

func (p *Parser) parseStarExpr(starExpr *sqlparser.StarExpr, q *query.Query) error {
	tableName := starExpr.TableName.Name.String()
	qualifier := starExpr.TableName.Qualifier.String()
	if tableName == "" && qualifier == "" {
		q.Table.Columns[q.Table.Name] = append(q.Table.Columns[q.Table.Name], "*")
	} else {
		table, ok := q.Table.Aliases[tableName]
		if ok {
			q.Table.Columns[table] = append(q.Table.Columns[table], "*")
			return nil
		}

		q.Table.Columns[tableName] = append(q.Table.Columns[tableName], "*")
	}

	return nil
}

func (p *Parser) parseAliasedExpr(aliasedExpr *sqlparser.AliasedExpr, q *query.Query) error {
	as := aliasedExpr.As.String()
	switch expr := aliasedExpr.Expr.(type) {
	case *sqlparser.ColName:
		var column string
		if as == "" {
			column = expr.Name.String()
		} else {
			column = as
		}

		tableName := expr.Qualifier.Name.String()
		if tableName == "" {
			tableName = q.Table.Name
		} else {
			val, ok := q.Table.Aliases[tableName]
			if ok {
				tableName = val
			}
		}

		q.Table.Columns[tableName] = append(q.Table.Columns[tableName], column)
	case *sqlparser.FuncExpr:
		tableName := "*aliases_functions*"
		if as == "" {
			q.Table.Columns[tableName] = append(q.Table.Columns[tableName], expr.Name.String())
		} else {
			q.Table.Columns[tableName] = append(q.Table.Columns[tableName], as)
		}
	}

	return nil
}

func (p *Parser) parseWhere(where *sqlparser.Where, q *query.Query) error {
	var err error
	switch whereStmt := where.Expr.(type) {
	case *sqlparser.ComparisonExpr:
		err = p.parseExpr(whereStmt, q)
		if err != nil {
			return err
		}
	case *sqlparser.AndExpr:
		err = p.parseExpr(whereStmt, q)
		if err != nil {
			return err
		}
	case *sqlparser.OrExpr:
		err = p.parseExpr(whereStmt, q)
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *Parser) parseExpr(expr sqlparser.Expr, q *query.Query) error {
	switch valExpr := expr.(type) {
	case *sqlparser.SQLVal:
		return nil
	case *sqlparser.AndExpr:
		if err := p.parseExpr(valExpr.Left, q); err != nil {
			return err
		}
		if err := p.parseExpr(valExpr.Right, q); err != nil {
			return err
		}
	case *sqlparser.OrExpr:
		if err := p.parseExpr(valExpr.Left, q); err != nil {
			return err
		}
		if err := p.parseExpr(valExpr.Right, q); err != nil {
			return err
		}
	case *sqlparser.RangeCond:
		q.Wheres = append(q.Wheres, query.NewWhere(
			valExpr.Left.(*sqlparser.ColName).Qualifier.Name.String(),
			valExpr.Left.(*sqlparser.ColName).Name.String(),
			valExpr.Operator,
		))
	case *sqlparser.ComparisonExpr:
		q.Wheres = append(q.Wheres, query.NewWhere(
			valExpr.Left.(*sqlparser.ColName).Qualifier.Name.String(),
			valExpr.Left.(*sqlparser.ColName).Name.String(),
			valExpr.Operator,
		))
	case *sqlparser.ParenExpr:
		if err := p.parseExpr(valExpr.Expr, q); err != nil {
			return err
		}
	default:
		return fmt.Errorf("'%s' does not supported", reflect.TypeOf(valExpr))
	}
	return nil
}

func (p *Parser) parseTableExpr(stmt *sqlparser.Select, tableExpr sqlparser.TableExpr, q *query.Query) error {
	switch expr := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		p.parseAliasedTableExpr(expr, q)
	case *sqlparser.ParenTableExpr:
	case *sqlparser.JoinTableExpr:
		p.parseJoinTableExpr(stmt, expr, q)
	}
	return nil
}

func (p *Parser) parseAliasedTableExpr(tableExpr *sqlparser.AliasedTableExpr, q *query.Query) error {
	switch expr := tableExpr.Expr.(type) {
	case sqlparser.TableName:
		tableName := expr.Name.String()
		if q.Table.Name == "" {
			q.Table.Name = tableName
		} else {
			q.Table.Names = append(q.Table.Names, tableName)
		}

		as := tableExpr.As.String()
		if as != "" {
			q.Table.Aliases[as] = tableName
			q.Table.AsNames = append(q.Table.AsNames, as)
			q.Table.As = as

			if len(q.Table.AsNames) > 1 {
				q.Table.As = ""
			}
		}
	case *sqlparser.Subquery:
		q := query.New()
		if err := p.parseSelectStmt(expr.Select.(*sqlparser.Select), q); err != nil {
			return err
		}

		as := tableExpr.As.String()
		if as != "" {
			q.Table.As = as
		}
		p.query.Subqueries = append(p.query.Subqueries, q)
	default:
	}

	return nil
}

func (p *Parser) parseJoinTableExpr(stmt *sqlparser.Select, joinExpr *sqlparser.JoinTableExpr, q *query.Query) error {
	switch expr := joinExpr.LeftExpr.(type) {
	case *sqlparser.JoinTableExpr:
		if err := p.parseJoinTableExpr(stmt, expr, q); err != nil {
			return err
		}
	case *sqlparser.AliasedTableExpr:
		if err := p.parseAliasedTableExpr(expr, q); err != nil {
			return err
		}
	default:

	}

	switch expr := joinExpr.RightExpr.(type) {
	case *sqlparser.JoinTableExpr:
		if err := p.parseJoinTableExpr(stmt, expr, q); err != nil {
			return err
		}
	case *sqlparser.AliasedTableExpr:
		if err := p.parseAliasedTableExpr(expr, q); err != nil {
			return err
		}
	}

	switch expr := joinExpr.On.(type) {
	case *sqlparser.ComparisonExpr:
		if err := p.parseCompExpr(expr, q); err != nil {
			return err
		}
	case *sqlparser.ParenExpr:
		if err := p.parseParenExpr(expr, q); err != nil {
			return err
		}

	}

	return nil
}

func (p *Parser) parseCompExpr(compExpr *sqlparser.ComparisonExpr, q *query.Query) error {
	leftTable := compExpr.Left.(*sqlparser.ColName).Qualifier.Name.String()
	leftCol := compExpr.Left.(*sqlparser.ColName).Name.String()

	rightTable := compExpr.Right.(*sqlparser.ColName).Qualifier.Name.String()
	rightCol := compExpr.Right.(*sqlparser.ColName).Name.String()

	q.Table.Join.Comparisons = append(q.Table.Join.Comparisons, query.NewComparison(
		fmt.Sprintf("%s.%s", leftTable, leftCol),
		fmt.Sprintf("%s.%s", rightTable, rightCol),
		compExpr.Operator,
	))

	return nil
}

func (p *Parser) parseParenExpr(parenExpr *sqlparser.ParenExpr, q *query.Query) error {
	switch expr := parenExpr.Expr.(type) {
	case *sqlparser.AndExpr:
		if err := p.parseAndExpr(expr, q); err != nil {
			return err
		}
	}

	return nil
}

func (p *Parser) parseAndExpr(andExpr *sqlparser.AndExpr, q *query.Query) error {
	switch expr := andExpr.Left.(type) {
	case *sqlparser.AndExpr:
		if err := p.parseAndExpr(expr, q); err != nil {
			return err
		}
	case *sqlparser.ComparisonExpr:
		if err := p.parseCompExpr(expr, q); err != nil {
			return err
		}
	}

	switch expr := andExpr.Right.(type) {
	case *sqlparser.AndExpr:
		if err := p.parseAndExpr(expr, q); err != nil {
			return err
		}
	case *sqlparser.ComparisonExpr:
		if err := p.parseCompExpr(expr, q); err != nil {
			return err
		}
	}

	return nil
}

func (p *Parser) Parse(sql string) error {
	ast, err := sqlparser.Parse(sql)
	if err != nil {
		return err
	}

	switch stmt := ast.(type) {
	case *sqlparser.Select:
		err = p.parseSelectStmt(stmt, p.query)

		if err != nil {
			return err
		}
	case *sqlparser.Insert:
	case *sqlparser.Update:
	case *sqlparser.Delete:
	}

	return nil
}

func (p *Parser) Query() *query.Query {
	return p.query
}
