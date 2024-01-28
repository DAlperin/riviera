use std::fmt::{self, Display};

use crate::{
    expr::{Column, Expr},
    schema::Schema,
    tree::{TreeNode, TreeNodeVisitor, VisitRecursion},
};

pub mod common;
pub mod planner;

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    Projection(Projection),
    Join(Join),
    Sort(Sort),
    Aggregate(Aggregate),
    Scan(Scan),
    SubqueryAlias(SubqueryAlias),
    None,
}

impl Display for LogicalPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut indent_visitor = PlanPrinterVisitor::new(f);
        match self.visit(&mut indent_visitor) {
            Ok(_) => Ok(()),
            Err(_) => Err(fmt::Error),
        }
    }
}

impl LogicalPlan {
    fn display(&self) -> String {
        match self {
            Self::Projection(Projection { expr, .. }) => {
                let exprs = expr.iter().map(|e| format!("{}", e)).collect::<Vec<_>>();
                format!("Projection({})", exprs.join(", "))
            }
            Self::Join(Join { join_type, on, .. }) => {
                format!("{} Join ON {}", join_type, on)
            }
            Self::Sort(Sort { .. }) => "Sort".to_string(),
            Self::Aggregate(Aggregate {
                aggr_expr,
                group_expr,
                ..
            }) => {
                let aggr_exprs = aggr_expr
                    .iter()
                    .map(|e| format!("{}", e))
                    .collect::<Vec<_>>();
                let group_exprs = group_expr
                    .iter()
                    .map(|e| format!("{}", e))
                    .collect::<Vec<_>>();
                format!(
                    "Aggregate aggr_expr=[{}] group_expr=[{}]",
                    aggr_exprs.join(", "),
                    group_exprs.join(", ")
                )
            }
            Self::Scan(Scan { table_name, .. }) => format!("Scan: {}", table_name),
            Self::SubqueryAlias(SubqueryAlias { alias, .. }) => format!("SubqueryAlias: {}", alias),
            Self::None => "None".to_string(),
        }
    }

    fn schema(&self) -> &Schema {
        match self {
            Self::Projection(Projection { schema, .. }) => schema,
            Self::Join(Join { schema, .. }) => schema,
            Self::Sort(Sort { input, .. }) => input.schema(),
            Self::Aggregate(Aggregate { schema, .. }) => schema,
            Self::Scan(Scan { schema, .. }) => schema,
            Self::SubqueryAlias(SubqueryAlias { schema, .. }) => schema,
            Self::None => unimplemented!(),
        }
    }
}

impl TreeNode for LogicalPlan {
    fn apply_children<F>(&self, f: &mut F) -> Result<crate::tree::VisitRecursion, String>
    where
        F: FnMut(&Self) -> Result<crate::tree::VisitRecursion, String>,
    {
        let children = match self {
            Self::Projection(proj) => vec![&*proj.input],
            Self::Join(join) => vec![&*join.left, &*join.right],
            Self::Sort(sort) => vec![&*sort.input],
            Self::Aggregate(aggr) => vec![&*aggr.input],
            Self::Scan(_) => vec![],
            Self::SubqueryAlias(SubqueryAlias { plan, .. }) => vec![&**plan],
            Self::None => vec![],
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
            Self::Projection(proj) => {
                let input = f(&*proj.input)?;
                Self::Projection(Projection {
                    input: Box::new(input),
                    ..proj.clone()
                })
            }
            Self::Join(join) => {
                let left = f(&*join.left)?;
                let right = f(&*join.right)?;
                Self::Join(Join {
                    left: Box::new(left),
                    right: Box::new(right),
                    ..join.clone()
                })
            }
            Self::Sort(sort) => {
                let input = f(&*sort.input)?;
                Self::Sort(Sort {
                    input: Box::new(input),
                    ..sort.clone()
                })
            }
            Self::Aggregate(aggr) => {
                let input = f(&*aggr.input)?;
                Self::Aggregate(Aggregate {
                    input: Box::new(input),
                    ..aggr.clone()
                })
            }
            Self::SubqueryAlias(subq) => {
                let plan = f(&*subq.plan)?;
                Self::SubqueryAlias(SubqueryAlias {
                    plan: Box::new(plan),
                    alias: subq.alias.clone(),
                    ..subq.clone()
                })
            }
            Self::Scan(_) => self.clone(),
            Self::None => self.clone(),
        })
    }
}

pub struct PlanPrinterVisitor<'a, 'b> {
    f: &'a mut fmt::Formatter<'b>,
    indent: usize,
}

impl<'a, 'b> PlanPrinterVisitor<'a, 'b> {
    pub fn new(f: &'a mut fmt::Formatter<'b>) -> Self {
        Self { f, indent: 0 }
    }
}

impl<'a, 'b> TreeNodeVisitor for PlanPrinterVisitor<'a, 'b> {
    type N = LogicalPlan;

    fn pre_visit(&mut self, plan: &Self::N) -> Result<VisitRecursion, String> {
        if self.indent > 0 {
            writeln!(self.f).unwrap();
        }
        write!(self.f, "{:indent$}", "", indent = self.indent * 2).unwrap();
        write!(self.f, "{} {}", plan.display(), plan.schema().display()).unwrap();
        self.indent += 1;
        Ok(VisitRecursion::Continue)
    }

    fn post_visit(&mut self, _plan: &Self::N) -> Result<VisitRecursion, String> {
        self.indent -= 1;
        Ok(VisitRecursion::Continue)
    }
}

#[derive(Debug, Clone)]
pub struct Scan {
    pub table_name: String,
    pub projection: Option<Vec<Column>>,
    pub filters: Vec<Expr>,
    pub schema: Box<Schema>,
}

#[derive(Debug, Clone)]
pub struct Projection {
    pub expr: Vec<Expr>,
    pub input: Box<LogicalPlan>,
    pub schema: Box<Schema>,
}

#[derive(Debug, Clone, Copy)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

impl Display for JoinType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JoinType::Inner => write!(f, "INNER"),
            JoinType::Left => write!(f, "LEFT"),
            JoinType::Right => write!(f, "RIGHT"),
            JoinType::Full => write!(f, "FULL"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Join {
    pub left: Box<LogicalPlan>,
    pub right: Box<LogicalPlan>,
    pub on: Expr,
    pub join_type: JoinType,
    pub schema: Box<Schema>,
}

#[derive(Debug, Clone)]
pub struct Sort {
    pub expr: Vec<Expr>,
    pub input: Box<LogicalPlan>,
}

#[derive(Debug, Clone)]
pub struct Aggregate {
    pub input: Box<LogicalPlan>,
    pub group_expr: Vec<Expr>,
    pub aggr_expr: Vec<Expr>,
    pub schema: Box<Schema>,
}

#[derive(Debug, Clone)]
pub struct SubqueryAlias {
    pub alias: String,
    pub plan: Box<LogicalPlan>,
    pub schema: Box<Schema>,
}
