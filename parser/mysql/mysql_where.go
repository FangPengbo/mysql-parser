package mysql

import (
	"fmt"
	"github.com/blastrain/vitess-sqlparser/sqlparser"
	"log"
)

func ExtractWhereColumns() {
	query := `SELECT u.id, u.name, o.order_id, o.amount, (SELECT MAX(p.price) FROM products p WHERE p.order_id = o.order_id) AS max_price
	FROM users u
	JOIN orders o ON u.id = o.user_id
	WHERE u.age > 30 AND o.amount > 100 AND (u.name = 'John' OR o.status = 'shipped')
	AND EXISTS (SELECT 1 FROM reviews r WHERE r.user_id = u.id AND r.rating > 4)`

	// 解析 SQL 查询
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		log.Fatalf("Failed to parse query: %v", err)
	}

	// 类型断言为 *sqlparser.Select
	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok {
		log.Fatalf("Not a SELECT statement")
	}

	// 提取表名
	tableMap := extractTableNames(selectStmt)
	fmt.Println("Tables:", tableMap)

	// 提取 WHERE 条件中的列名和表名
	columnTableMap := extractColumnsFromWhere(selectStmt.Where, tableMap)
	for column, table := range columnTableMap {
		fmt.Printf("Column: %s, Table: %s\n", column, table)
	}
}

// 提取表名
func extractTableNames(stmt *sqlparser.Select) map[string]string {
	tableMap := make(map[string]string)
	for _, tableExpr := range stmt.From {
		switch table := tableExpr.(type) {
		case *sqlparser.AliasedTableExpr:
			if tblName, ok := table.Expr.(sqlparser.TableName); ok {
				alias := table.As.String()
				if alias == "" {
					alias = tblName.Name.String()
				}
				tableMap[alias] = tblName.Name.String()
				fmt.Printf("Extracted table: alias=%s, name=%s\n", alias, tblName.Name.String())
			}
		case *sqlparser.JoinTableExpr:
			// 递归处理 JOIN 表达式
			handleJoinTableExpr(table, tableMap)
		default:
			fmt.Printf("Unknown table expression type: %T\n", tableExpr)
		}
	}
	return tableMap
}

// 处理 JOIN 表达式
func handleJoinTableExpr(joinExpr *sqlparser.JoinTableExpr, tableMap map[string]string) {
	switch left := joinExpr.LeftExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		if tblName, ok := left.Expr.(sqlparser.TableName); ok {
			alias := left.As.String()
			if alias == "" {
				alias = tblName.Name.String()
			}
			tableMap[alias] = tblName.Name.String()
			fmt.Printf("Extracted table from join: alias=%s, name=%s\n", alias, tblName.Name.String())
		}
	case *sqlparser.JoinTableExpr:
		handleJoinTableExpr(left, tableMap)
	default:
		fmt.Printf("Unknown join left expression type: %T\n", left)
	}

	switch right := joinExpr.RightExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		if tblName, ok := right.Expr.(sqlparser.TableName); ok {
			alias := right.As.String()
			if alias == "" {
				alias = tblName.Name.String()
			}
			tableMap[alias] = tblName.Name.String()
			fmt.Printf("Extracted table from join: alias=%s, name=%s\n", alias, tblName.Name.String())
		}
	case *sqlparser.JoinTableExpr:
		handleJoinTableExpr(right, tableMap)
	default:
		fmt.Printf("Unknown join right expression type: %T\n", right)
	}
}

// 提取 WHERE 条件中的列名和表名
func extractColumnsFromWhere(where *sqlparser.Where, tableMap map[string]string) map[string]string {
	columnTableMap := make(map[string]string)
	if where != nil {
		extractColumnsFromExpr(where.Expr, columnTableMap, tableMap)
	}
	return columnTableMap
}

// 递归解析 WHERE 条件表达式
func extractColumnsFromExpr(expr sqlparser.Expr, columnTableMap map[string]string, tableMap map[string]string) {
	switch v := expr.(type) {
	case *sqlparser.AndExpr:
		extractColumnsFromExpr(v.Left, columnTableMap, tableMap)
		extractColumnsFromExpr(v.Right, columnTableMap, tableMap)
	case *sqlparser.OrExpr:
		extractColumnsFromExpr(v.Left, columnTableMap, tableMap)
		extractColumnsFromExpr(v.Right, columnTableMap, tableMap)
	case *sqlparser.ComparisonExpr:
		extractColumnsFromExpr(v.Left, columnTableMap, tableMap)
		extractColumnsFromExpr(v.Right, columnTableMap, tableMap)
	case *sqlparser.RangeCond:
		extractColumnsFromExpr(v.Left, columnTableMap, tableMap)
		extractColumnsFromExpr(v.From, columnTableMap, tableMap)
		extractColumnsFromExpr(v.To, columnTableMap, tableMap)
	case *sqlparser.IsExpr:
		extractColumnsFromExpr(v.Expr, columnTableMap, tableMap)
	case *sqlparser.NotExpr:
		extractColumnsFromExpr(v.Expr, columnTableMap, tableMap)
	case *sqlparser.ParenExpr:
		extractColumnsFromExpr(v.Expr, columnTableMap, tableMap)
	case *sqlparser.ColName:
		tableAlias := v.Qualifier.Name.String()
		column := v.Name.String()
		// 使用别名映射回真实表名
		if tableAlias != "" {
			if table, ok := tableMap[tableAlias]; ok {
				columnTableMap[column] = table
				fmt.Printf("Extracted column: name=%s, table=%s\n", column, table)
			} else {
				fmt.Printf("Alias %s not found in table map\n", tableAlias)
			}
		} else {
			fmt.Printf("Column %s has no table alias\n", column)
		}
	case *sqlparser.SQLVal:
		// 处理 SQL 值（如字符串、数字等）
		fmt.Printf("SQL value: %s\n", string(v.Val))
	case *sqlparser.Subquery:
		// 处理子查询
		subSelect, ok := v.Select.(*sqlparser.Select)
		if ok {
			subTableMap := extractTableNames(subSelect)
			for k, v := range subTableMap {
				tableMap[k] = v
			}
			subColumnTableMap := extractColumnsFromWhere(subSelect.Where, tableMap)
			for k, v := range subColumnTableMap {
				columnTableMap[k] = v
			}
		} else {
			fmt.Printf("Unknown subquery select type: %T\n", v.Select)
		}
	case *sqlparser.FuncExpr:
		// 处理函数调用
		for _, arg := range v.Exprs {
			if aliasedExpr, ok := arg.(*sqlparser.AliasedExpr); ok {
				extractColumnsFromExpr(aliasedExpr.Expr, columnTableMap, tableMap)
			} else {
				fmt.Printf("Unknown function argument type: %T\n", arg)
			}
		}
	case *sqlparser.ExistsExpr:
		// 处理 EXISTS 子查询
		subSelect, ok := v.Subquery.Select.(*sqlparser.Select)
		if ok {
			subTableMap := extractTableNames(subSelect)
			for k, v := range subTableMap {
				tableMap[k] = v
			}
			subColumnTableMap := extractColumnsFromWhere(subSelect.Where, tableMap)
			for k, v := range subColumnTableMap {
				columnTableMap[k] = v
			}
		} else {
			fmt.Printf("Unknown EXISTS subquery select type: %T\n", v.Subquery.Select)
		}
	default:
		fmt.Printf("Unknown expression type: %T\n", expr)
	}
}
