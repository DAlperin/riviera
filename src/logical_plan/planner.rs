use sqlparser::ast::{
    GroupByExpr, Join as AstJoin, JoinOperator, Query, Select, SelectItem, SetExpr, Statement,
    TableFactor, TableWithJoins,
};

use crate::{
    expr::Expr,
    logical_plan::common::find_exprs_in_exprs,
    schema::{Schema, TableProvider},
};

use super::{
    common::{rebase_expr, sql_expr_to_logical_expr, sql_item_to_expr, to_field},
    Aggregate, Join, JoinType, LogicalPlan, Projection, Scan, SubqueryAlias,
};

#[derive(Debug, Clone, PartialEq)]
pub struct RivieraPlanner {
    table_provider: TableProvider,
}

impl RivieraPlanner {
    pub fn new(table_provider: TableProvider) -> Self {
        Self { table_provider }
    }

    pub fn plan_statement(&self, statement: Statement) -> Result<LogicalPlan, String> {
        match statement {
            Statement::Query(query) => {
                let plan = self.plan_query(*query)?;
                Ok(plan)
            }
            _ => Err("not a query".to_string()),
        }
    }

    fn plan_query(&self, query: Query) -> Result<LogicalPlan, String> {
        match *query.body {
            SetExpr::Select(select) => self.plan_select(*select),
            _ => Err("only SELECT is implemented".to_string()),
        }
    }

    fn plan_select(&self, select: Select) -> Result<LogicalPlan, String> {
        let from = self.plan_from(&select.from).unwrap();
        let select_exprs = self.build_select_exprs(&select.projection, from.schema())?;
        let agg_exprs = find_exprs_in_exprs(&select_exprs, |expr| {
            matches!(expr, Expr::AggregateFunction(_))
        });
        let group_by_exprs = if let GroupByExpr::Expressions(group_by_exprs) = select.group_by {
            group_by_exprs
                .iter()
                .map(|e| sql_expr_to_logical_expr(e, from.schema()))
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
        } else {
            vec![]
        };

        // create mutable plan variable
        let mut plan = from.clone();
        let mut select_exprs = select_exprs.clone();

        if !agg_exprs.is_empty() || !group_by_exprs.is_empty() {
            (plan, select_exprs) = self
                .aggregate(&select_exprs, &agg_exprs, &group_by_exprs, &from)
                .unwrap();
        }

        plan = self.project(&select_exprs, &plan, plan.schema()).unwrap();

        Ok(plan)
    }

    fn aggregate(
        &self,
        select_exprs: &[Expr],
        aggr_exprs: &[Expr],
        group_exprs: &[Expr],
        input: &LogicalPlan,
    ) -> Result<(LogicalPlan, Vec<Expr>), String> {
        let mut all_exprs = Vec::from(aggr_exprs);
        all_exprs.extend_from_slice(group_exprs);
        let fields = all_exprs
            .iter()
            .map(|expr| to_field(expr, input.schema()))
            .collect::<Result<Vec<_>, _>>()?;
        let schema = Schema::new(fields);

        let rebased_exprs = select_exprs
            .iter()
            .map(|expr| rebase_expr(expr, &all_exprs))
            .collect::<Result<Vec<Expr>, String>>();

        let plan = LogicalPlan::Aggregate(Aggregate {
            input: Box::new(input.clone()),
            aggr_expr: Vec::from(aggr_exprs),
            group_expr: Vec::from(group_exprs),
            schema: Box::new(schema),
        });

        Ok((plan, rebased_exprs?))
    }

    fn plan_join_table(&self, table: &TableWithJoins) -> Result<LogicalPlan, String> {
        let left = self.plan_relation(&table.relation)?;
        match table.joins.len() {
            0 => Ok(left),
            1 => self.plan_join(&left, &table.joins[0]),
            _ => Err("only one join is supported".to_string()),
        }
    }

    fn plan_join(&self, left: &LogicalPlan, join: &AstJoin) -> Result<LogicalPlan, String> {
        let right = self.plan_relation(&join.relation)?;
        match &join.join_operator {
            JoinOperator::LeftOuter(contraint) => match contraint {
                sqlparser::ast::JoinConstraint::On(expr) => {
                    let new_schema = left.schema().clone().merge(right.schema());
                    let on = sql_expr_to_logical_expr(expr, &new_schema)?;
                    Ok(LogicalPlan::Join(Join {
                        left: Box::new(left.clone()),
                        right: Box::new(right.clone()),
                        on: on.clone(),
                        join_type: JoinType::Left,
                        schema: Box::new(new_schema),
                    }))
                }
                _ => Err("only ON is supported".to_string()),
            },
            _ => Err("only LEFT OUTER JOIN is supported".to_string()),
        }
    }

    fn plan_relation(&self, relation: &TableFactor) -> Result<LogicalPlan, String> {
        match relation {
            TableFactor::Table { name, .. } => {
                let table_name = name.to_string();
                let schema = match self.table_provider.tables.get(&table_name) {
                    Some(schema) => schema,
                    None => return Err(format!("no table named {}", table_name)),
                };
                Ok(LogicalPlan::Scan(Scan {
                    table_name,
                    projection: None,
                    filters: vec![],
                    schema: Box::new(schema.clone()),
                }))
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                let plan = self.plan_query(*subquery.clone())?;
                if alias.is_some() {
                    let alias = alias.as_ref().unwrap();
                    Ok(LogicalPlan::SubqueryAlias(SubqueryAlias {
                        alias: alias.name.to_string(),
                        plan: Box::new(plan.clone()),
                        schema: Box::new(
                            plan.schema()
                                .requalify(alias.name.to_string().as_str())
                                .clone(),
                        ),
                    }))
                } else {
                    Ok(plan)
                }
            }
            _ => Err("only tables are supported".to_string()),
        }
    }

    fn plan_from(&self, from: &[TableWithJoins]) -> Result<LogicalPlan, String> {
        match from.len() {
            0 => Err("no tables".to_string()),
            1 => self.plan_join_table(&from[0]),
            //TODO: Do an implicit cross join here
            _ => Err("only one table is supported".to_string()),
        }
    }

    fn build_select_exprs(
        &self,
        items: &[SelectItem],
        schema: &Schema,
    ) -> Result<Vec<Expr>, String> {
        items
            .iter()
            .map(|item| sql_item_to_expr(item, schema))
            .collect::<Result<Vec<_>, _>>()
    }

    fn project(
        &self,
        exprs: &[Expr],
        input: &LogicalPlan,
        schema: &Schema,
    ) -> Result<LogicalPlan, String> {
        let fields = exprs
            .iter()
            .map(|expr| to_field(expr, schema))
            .collect::<Result<Vec<_>, _>>()?;
        let schema = Schema::new(fields);

        Ok(LogicalPlan::Projection(Projection {
            expr: Vec::from(exprs),
            input: Box::new(input.clone()),
            schema: Box::new(schema),
        }))
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use sqlparser::{dialect::PostgreSqlDialect, parser::Parser};

    use crate::{expr::DataType, schema::Field};

    use super::*;

    fn logical_plan(sql: &str) -> LogicalPlan {
        let ast = Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap();
        let statement = ast[0].clone();
        let provider = TableProvider::new(HashMap::from([
            (
                "posts".to_string(),
                Schema::new(vec![
                    Field::new("id", DataType::Int32, false),
                    Field::new("name", DataType::Utf8, false),
                ]),
            ),
            (
                "likes".to_string(),
                Schema::new(vec![
                    Field::new("id", DataType::Int32, false),
                    Field::new("post_id", DataType::Int32, false),
                ]),
            ),
        ]));

        let planner = RivieraPlanner::new(provider);

        planner.plan_statement(statement).unwrap()
    }

    #[test]
    fn test_simple_select() {
        let plan = logical_plan("SELECT name FROM posts");

        assert_eq!(
            format!("{plan}"),
            "Projection(posts.name) Schema(name)\
            \n  Scan: posts Schema(id, name)"
        );
    }

    #[test]
    fn test_basic_alias() {
        let plan = logical_plan("SELECT name AS n FROM posts");

        assert_eq!(
            format!("{plan}"),
            "Projection(posts.name AS n) Schema(n)\
            \n  Scan: posts Schema(id, name)"
        );
    }

    #[test]
    fn test_basic_join() {
        let plan =
            logical_plan("SELECT name FROM posts LEFT JOIN likes ON posts.id = likes.post_id");

        assert_eq!(
            format!("{plan}"),
            "Projection(posts.name) Schema(name)\
            \n  LEFT Join ON posts.id = likes.post_id Schema(id, name, id, post_id)\
            \n    Scan: posts Schema(id, name)\
            \n    Scan: likes Schema(id, post_id)"
        );
    }

    #[test]
    fn test_simple_subquery() {
        let plan = logical_plan("SELECT name FROM (SELECT name FROM posts) AS posts");

        assert_eq!(
            format!("{plan}"),
            "Projection(posts.name) Schema(name)\
            \n  SubqueryAlias: posts Schema(name)\
            \n    Projection(posts.name) Schema(name)\
            \n      Scan: posts Schema(id, name)"
        );
    }

    #[test]
    fn test_simple_aggregate() {
        let plan =
            logical_plan("SELECT post_id, COUNT(*) AS postcount FROM likes GROUP BY post_id");

        assert_eq!(
            format!("{plan}"),
            "Projection(likes.post_id, count(*) AS postcount) Schema(post_id, postcount)\
            \n  Aggregate aggr_expr=[count(*)] group_expr=[likes.post_id] Schema(count(*), post_id)\
            \n    Scan: likes Schema(id, post_id)"
        );
    }

    #[test]
    fn test_complex_query() {
        let plan = logical_plan(
            "SELECT name, postcount FROM posts LEFT JOIN (SELECT post_id, COUNT(id) AS postcount FROM likes GROUP BY post_id) AS likes ON posts.id = likes.post_id",
        );

        assert_eq!(
            format!("{plan}"),
            "Projection(posts.name, likes.postcount) Schema(name, postcount)\
            \n  LEFT Join ON posts.id = likes.post_id Schema(id, name, post_id, postcount)\
            \n    Scan: posts Schema(id, name)\
            \n    SubqueryAlias: likes Schema(post_id, postcount)\
            \n      Projection(likes.post_id, count(likes.id) AS postcount) Schema(post_id, postcount)\
            \n        Aggregate aggr_expr=[count(likes.id)] group_expr=[likes.post_id] Schema(count(likes.id), post_id)\
            \n          Scan: likes Schema(id, post_id)"
        );
    }
}
