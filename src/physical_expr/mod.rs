use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, RecordBatch};
use arrow::compute::kernels::cmp::eq;

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Column(Column),
    BinaryExpr(BinaryExpr),
}

pub trait PhysicalExpr {
    fn evalutate(&self, batch: &RecordBatch) -> Result<ArrayRef, String>;
}

impl PhysicalExpr for Expr {
    fn evalutate(&self, batch: &RecordBatch) -> Result<ArrayRef, String> {
        match self {
            Expr::Column(column) => column.evalutate(batch),
            Expr::BinaryExpr(binary_expr) => binary_expr.evalutate(batch),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    pub name: String,
    pub index: usize,
}

impl Column {
    pub fn new(name: &str, index: usize) -> Self {
        Self {
            name: name.to_string(),
            index,
        }
    }
}

impl PhysicalExpr for Column {
    fn evalutate(&self, batch: &RecordBatch) -> Result<ArrayRef, String> {
        Ok(batch.column(self.index).to_owned())
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BinaryOperator {
    Eq,
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

impl PhysicalExpr for BinaryExpr {
    fn evalutate(&self, batch: &RecordBatch) -> Result<ArrayRef, String> {
        let left = self.left.evalutate(batch)?;
        let right = self.right.evalutate(batch)?;

        match self.op {
            BinaryOperator::Eq => Ok(Arc::new(eq(&left, &right).unwrap())),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        expr::DataType,
        schema::{Field, Schema},
    };

    use super::*;
    use arrow::array::Int32Array;

    #[test]
    fn test_binary_expr() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]);
        let column_a = Column::new("a", 0);
        let column_b = Column::new("b", 1);
        let binary_expr = BinaryExpr::new(
            Expr::Column(column_a),
            BinaryOperator::Eq,
            Expr::Column(column_b),
        );
        let batch = RecordBatch::try_new(
            schema.to_arrow(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int32Array::from(vec![1, 4])),
            ],
        )
        .unwrap();
        let result = binary_expr.evalutate(&batch).unwrap();
        let result = result.as_any().downcast_ref::<BooleanArray>().unwrap();

        assert_eq!(result, &BooleanArray::from(vec![true, false]));
    }
}
