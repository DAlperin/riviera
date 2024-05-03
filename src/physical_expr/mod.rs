use arrow::array::{ArrayRef, RecordBatch};

pub struct Column {
    pub name: String,
    pub index: usize,
}

impl PhysicalExpr for Column {
    fn evalutate(&self, batch: &RecordBatch) -> Result<ArrayRef, String> {
        Ok(batch.column(self.index).to_owned())
    }
}

pub trait PhysicalExpr {
    fn evalutate(&self, batch: &RecordBatch) -> Result<ArrayRef, String>;
}
