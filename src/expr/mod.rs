use std::{
    fmt::{self, Display},
    str::FromStr,
    vec,
};

use crate::{
    schema::Schema,
    tree::{TreeNode, VisitRecursion},
};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AggregateFunctionKind {
    Count,
}

impl AggregateFunctionKind {
    pub fn name(&self) -> &str {
        match self {
            Self::Count => "count",
        }
    }
}

impl FromStr for AggregateFunctionKind {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "count" => Ok(Self::Count),
            _ => Err(format!("unknown aggregate function: {}", s)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DataType {
    Int32,
    Utf8,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BinaryOperator {
    Eq,
}

impl Display for BinaryOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Eq => write!(f, "="),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Column(Column),
    AggregateFunction(AggregateFunction),
    Function(Function),
    Alias(Alias),
    BinaryExpr(BinaryExpr),
    Wildcard,
}

impl Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Column(Column { name, relation }) => {
                if let Some(relation) = relation {
                    write!(f, "{}.{}", relation, name)
                } else {
                    write!(f, "{}", name)
                }
            }
            Self::AggregateFunction(agg) => {
                write!(f, "{}(", agg.kind.name()).unwrap();
                let args = agg
                    .args
                    .iter()
                    .map(|arg| format!("{}", arg))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "{})", args)
            }
            Self::Function(func) => {
                write!(f, "{}(", func.name).unwrap();
                let args = func
                    .args
                    .iter()
                    .map(|arg| format!("{}", arg))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "{})", args)
            }
            Self::Alias(alias) => write!(f, "{} AS {}", alias.expr, alias.alias),
            Self::BinaryExpr(expr) => write!(f, "{} {} {}", expr.left, expr.op, expr.right),
            Self::Wildcard => write!(f, "*"),
        }
    }
}

impl TreeNode for Expr {
    fn apply_children<F>(&self, f: &mut F) -> Result<VisitRecursion, String>
    where
        F: FnMut(&Self) -> Result<VisitRecursion, String>,
    {
        let children = match self {
            Self::Column(_) => vec![],
            Self::AggregateFunction(agg) => agg.args.iter().collect(),
            Self::Function(func) => func.args.iter().collect(),
            Self::Alias(alias) => vec![&*alias.expr],
            Self::BinaryExpr(expr) => vec![&*expr.left, &*expr.right],
            Self::Wildcard => vec![self],
        };
        for child in children {
            match f(child)? {
                VisitRecursion::Continue => {}
                VisitRecursion::SkipChildren => return Ok(VisitRecursion::Continue),
                VisitRecursion::Stop => return Ok(VisitRecursion::Stop),
            }
        }
        Ok(VisitRecursion::Continue)
    }

    fn map_children<F>(&self, f: &mut F) -> Result<Self, String>
    where
        F: FnMut(&Self) -> Result<Self, String>,
    {
        Ok(match self {
            Self::Column(_) => self.clone(),
            Self::AggregateFunction(AggregateFunction { args, kind }) => {
                let args = args.iter().map(f).collect::<Result<Vec<_>, _>>()?;
                Self::AggregateFunction(AggregateFunction::new(*kind, args))
            }
            Self::Function(Function { args, name }) => {
                let args = args.iter().map(f).collect::<Result<Vec<_>, _>>()?;
                Self::Function(Function::new(name, args))
            }
            Self::Alias(alias) => {
                let expr = f(&alias.expr)?;
                Self::Alias(Alias::new(expr, &alias.alias))
            }
            Self::BinaryExpr(expr) => {
                let left = f(&expr.left)?;
                let right = f(&expr.right)?;
                Self::BinaryExpr(BinaryExpr::new(left, expr.op, right))
            }
            Self::Wildcard => self.clone(),
        })
    }
}

impl Expr {
    pub fn alias(&self, alias: &str) -> Self {
        Self::Alias(Alias::new(self.clone(), alias))
    }

    pub fn nullable(&self, schema: &Schema) -> bool {
        match self {
            Self::Column(Column { name, relation }) => match relation.as_ref() {
                Some(relation) => {
                    let fields = schema.fields_by_name_and_qualifier(relation, name);
                    assert_eq!(fields.len(), 1);
                    fields[0].nullable
                }
                None => {
                    let fields = schema.fields_by_name(name);
                    assert_eq!(fields.len(), 1);
                    fields[0].nullable
                }
            },
            Self::AggregateFunction(agg) => match agg.kind {
                AggregateFunctionKind::Count => false,
            },
            Self::Function(_) => {
                unimplemented!()
            }
            Self::Alias(alias) => alias.expr.nullable(schema),
            Self::BinaryExpr(expr) => {
                let left_nullable = expr.left.nullable(schema);
                let right_nullable = expr.right.nullable(schema);
                left_nullable || right_nullable
            }
            Self::Wildcard => unimplemented!(),
        }
    }

    pub fn get_type(&self, schema: &Schema) -> DataType {
        match self {
            Self::Column(Column { name, relation }) => match relation.as_ref() {
                Some(relation) => {
                    let fields = schema.fields_by_name_and_qualifier(relation, name);
                    assert_eq!(fields.len(), 1);
                    fields[0].data_type
                }
                None => {
                    let fields = schema.fields_by_name(name);
                    assert_eq!(fields.len(), 1);
                    fields[0].data_type
                }
            },
            Self::AggregateFunction(agg) => match agg.kind {
                AggregateFunctionKind::Count => DataType::Int32,
            },
            Self::Function(_) => {
                unimplemented!()
            }
            Self::Alias(alias) => alias.expr.get_type(schema),
            Self::BinaryExpr(expr) => {
                let left_type = expr.left.get_type(schema);
                let right_type = expr.right.get_type(schema);
                assert_eq!(left_type, right_type);
                left_type
            }
            Self::Wildcard => unimplemented!(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AggregateFunction {
    pub kind: AggregateFunctionKind,
    pub args: Vec<Expr>,
}

impl AggregateFunction {
    pub fn new(kind: AggregateFunctionKind, args: Vec<Expr>) -> Self {
        Self { kind, args }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BinaryExpr {
    pub left: Box<Expr>,
    pub op: BinaryOperator,
    pub right: Box<Expr>,
}

impl BinaryExpr {
    pub fn new(left: Expr, op: BinaryOperator, right: Expr) -> Self {
        Self {
            left: Box::new(left),
            op,
            right: Box::new(right),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    // Eventually this should be a smarter data type that knows more about the relation
    pub relation: Option<String>,
    pub name: String,
}

impl Column {
    pub fn new(relation: Option<&str>, name: &str) -> Self {
        Self {
            relation: relation.map(|r| r.to_string()),
            name: name.to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Function {
    pub name: String,
    pub args: Vec<Expr>,
}

impl Function {
    pub fn new(name: &str, args: Vec<Expr>) -> Self {
        Self {
            name: name.to_string(),
            args,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Alias {
    pub expr: Box<Expr>,
    pub alias: String,
}

impl Alias {
    pub fn new(expr: Expr, alias: &str) -> Self {
        Self {
            expr: Box::new(expr),
            alias: alias.to_string(),
        }
    }
}
