package query

type Where struct {
	Table    string
	Column   string
	Operator string
}

type Table struct {
	Columns Columns
	Name    string
	Names   []string
	Aliases map[string]string
	As      string
	AsNames []string
	Join    Join
}

func (t *Table) GetNames() []string {
	return append([]string{t.Name}, t.Names...)
}

type Join struct {
	Comparisons []*Comparison
}

type Comparison struct {
	Left     string
	Right    string
	Operator string
}

type Columns map[string][]string

type Query struct {
	Table      *Table
	Wheres     []*Where
	Subqueries []*Query
}

func New() *Query {
	table := &Table{}
	table.Aliases = make(map[string]string)
	table.Columns = make(map[string][]string)

	subqueries := make([]*Query, 0)
	return &Query{
		Table:      table,
		Wheres:     make([]*Where, 0),
		Subqueries: subqueries,
	}
}

func NewWhere(table, column, operator string) *Where {
	return &Where{
		Table:    table,
		Column:   column,
		Operator: operator,
	}
}

func NewComparison(left, right, operator string) *Comparison {
	return &Comparison{
		Left:     left,
		Right:    right,
		Operator: operator,
	}
}
