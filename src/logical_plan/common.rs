use std::str::FromStr;

use sqlparser::ast::{
    Expr as SQLExpr, Function as SQLFunction, FunctionArg, FunctionArgExpr, SelectItem,
};

use crate::{
    expr::{
        AggregateFunction, AggregateFunctionKind, Alias, BinaryExpr, BinaryOperator, Column, Expr,
        Function,
    },
    schema::{Field, Schema},
    tree::{TreeNode, VisitRecursion},
};

pub fn sql_expr_to_logical_expr(expr: &SQLExpr, schema: &Schema) -> Result<Expr, String> {
    let expr = match expr {
        SQLExpr::CompoundIdentifier(idents) => {
            if idents.len() > 2 {
                return Err("only one dot is supported".to_string());
            }
            if idents.len() == 1 {
                Expr::Column(Column::new(None, idents[0].value.as_str()))
            } else {
                Expr::Column(Column::new(
                    Some(idents[0].value.as_str()),
                    idents[1].value.as_str(),
                ))
            }
        }
        SQLExpr::Identifier(ident) => Expr::Column(Column::new(None, ident.value.as_str())),
        SQLExpr::Function(func) => sql_function_to_expr(func, schema).unwrap(),
        SQLExpr::BinaryOp { left, op, right } => {
            let left = sql_expr_to_logical_expr(left, schema)?;
            let right = sql_expr_to_logical_expr(right, schema)?;
            let op = match op {
                sqlparser::ast::BinaryOperator::Eq => BinaryOperator::Eq,
                _ => return Err("only = is implemented".to_string()),
            };
            Expr::BinaryExpr(BinaryExpr::new(left, op, right))
        }
        _ => return Err("only column names are implemented".to_string()),
    };
    resolve_column(&expr, schema)
}

pub fn expr_as_column(expr: &Expr) -> Result<Expr, String> {
    match expr {
        Expr::Column(column) => Ok(Expr::Column(column.clone())),
        _ => Ok(Expr::Column(Column::new(None, expr.to_string().as_str()))),
    }
}

pub fn sql_item_to_expr(item: &SelectItem, schema: &Schema) -> Result<Expr, String> {
    match item {
        SelectItem::UnnamedExpr(expr) => sql_expr_to_logical_expr(expr, schema),
        SelectItem::ExprWithAlias { expr, alias } => {
            let expr = sql_expr_to_logical_expr(expr, schema)?;
            Ok(Expr::Alias(Alias::new(expr, alias.value.as_str())))
        }
        _ => return Err("only column names are implemented".to_string()),
    }
}

pub fn rebase_expr(expr: &Expr, base_exprs: &Vec<Expr>) -> Result<Expr, String> {
    expr.clone().transform_down(&mut |nested_expr| {
        if base_exprs.contains(nested_expr) {
            Ok(expr_as_column(nested_expr)?)
        } else {
            Ok(nested_expr.clone())
        }
    })
}

//Take column expressions and resolve them against the schema
pub fn resolve_column(expr: &Expr, schema: &Schema) -> Result<Expr, String> {
    match expr {
        Expr::Column(column) => {
            // println!("resolve_column: {:?}, schema {:?}", column.name, schema);
            if column.relation.is_some() {
                let options = schema.fields_by_name_and_qualifier(
                    column.relation.as_deref().unwrap(),
                    &column.name,
                );
                match options.len() {
                    0 => {
                        return Err(format!("no column named {}", column.name));
                    }
                    1 => {
                        return Ok(Expr::Column(Column::new(
                            options[0].qualifier.as_deref(),
                            options[0].name.as_str(),
                        )))
                    }
                    _ => return Err(format!("multiple columns named {}", column.name)),
                }
            }
            let options = schema.fields_by_name(&column.name);
            match options.len() {
                0 => Err(format!("no column named {}", column.name)),
                1 => Ok(Expr::Column(Column::new(
                    options[0].qualifier.as_deref(),
                    options[0].name.as_str(),
                ))),
                _ => Err(format!("multiple columns named {}", column.name)),
            }
        }
        _ => Ok(expr.clone()),
    }
}

pub fn to_field(expr: &Expr, schema: &Schema) -> Result<Field, String> {
    match expr {
        Expr::Column(column) => Ok(Field::new_with_qualifier(
            column.relation.as_deref(),
            column.name.as_str(),
            expr.get_type(schema),
            expr.nullable(schema),
        )),
        Expr::Alias(alias) => {
            let field = to_field(&alias.expr, schema)?;
            Ok(Field::new_with_qualifier(
                None,
                alias.alias.as_str(),
                expr.get_type(schema),
                field.nullable,
            ))
        }
        _ => Ok(Field::new_with_qualifier(
            None,
            expr.to_string().as_str(),
            expr.get_type(schema),
            expr.nullable(schema),
        )),
    }
}

pub fn sql_function_to_expr(func: &SQLFunction, schema: &Schema) -> Result<Expr, String> {
    let args = func
        .args
        .iter()
        .map(|arg| sql_function_arg_to_expr(arg, schema))
        .collect::<Result<Vec<Expr>, String>>()?;
    if let Ok(func) = AggregateFunctionKind::from_str(func.name.to_string().to_lowercase().as_str())
    {
        let function = Expr::AggregateFunction(AggregateFunction::new(func, args));
        return Ok(function);
    }
    let function = Expr::Function(Function::new(func.name.to_string().as_str(), args));
    Ok(function)
}

pub fn sql_function_arg_to_expr(arg: &FunctionArg, schema: &Schema) -> Result<Expr, String> {
    match arg {
        FunctionArg::Named {
            arg: FunctionArgExpr::Expr(expr),
            ..
        } => sql_expr_to_logical_expr(expr, schema),
        FunctionArg::Named {
            arg: FunctionArgExpr::Wildcard,
            ..
        } => Ok(Expr::Wildcard),
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => sql_expr_to_logical_expr(expr, schema),
        FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => Ok(Expr::Wildcard),
        _ => Err("unimplemented function arg".to_string()),
    }
}

pub fn find_exprs_in_exprs<F>(exprs: &[Expr], f: F) -> Vec<Expr>
where
    F: Fn(&Expr) -> bool,
{
    exprs
        .iter()
        .flat_map(|expr| find_expr_in_exprs(expr, &f))
        .collect()
}

pub fn find_expr_in_exprs<F>(expr: &Expr, f: &F) -> Vec<Expr>
where
    F: Fn(&Expr) -> bool,
{
    let mut exprs = vec![];
    expr.apply(&mut |expr| {
        if f(expr) {
            exprs.push(expr.clone());
            return Ok(VisitRecursion::SkipChildren);
        }
        Ok(VisitRecursion::Continue)
    })
    .expect("this shouldn't fail");
    exprs
}
