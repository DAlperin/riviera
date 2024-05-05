use core::hash;
use std::{
    collections::{BTreeMap, HashMap},
    hash::{DefaultHasher, Hash, Hasher},
};

use arrow::{
    array::{Array, ArrayRef, Int32Array, RecordBatch},
    compute::{partition, sort_to_indices, take},
    downcast_primitive_array,
};
use async_trait::async_trait;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use tokio::{
    select,
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
};

use crate::{
    logical_plan::JoinType,
    physical_expr::{Column, Expr, PhysicalExpr},
    schema::Schema,
};

mod graph;

type Change = (Action, Record);

//Whether we are adding or removing a record. For instance if a record gets removed upstream
//we need to propogate that change downstream
#[derive(Clone, Debug, PartialEq)]
enum Action {
    Add,
    Remove,
}

#[derive(Clone, Debug, PartialEq)]
struct Record {
    timestamp: usize,
    data: RecordBatch,
}

#[derive(Clone, Debug, PartialEq)]
enum StreamMessage {
    Data((Action, Record)),
    Control(ControlMessage),
}

#[derive(Clone, Debug, PartialEq)]
enum ControlMessage {
    Watermark(usize),
}

struct WatermarkHolder {
    current_watermark: usize,
    watermarks: Vec<usize>,
}

struct Context {
    outs: Vec<UnboundedSender<StreamMessage>>,
    watermarks: WatermarkHolder,
}

enum Node {
    Source(Box<dyn DataflowNode>),
    Sink(Box<dyn DataflowNode>),
    Operator(Box<dyn DataflowNode>),
}

struct Projection {
    expr: Vec<Expr>,
    output_schema: Schema,
}

#[async_trait]
impl DataflowNode for Projection {
    async fn handle_watermark(&self, context: &mut Context, idx: usize, watermark: usize) {}

    async fn handle_control_message(&self, context: &mut Context, message: ControlMessage) {}

    async fn handle_recordbatch(&mut self, context: &mut Context, _idx: usize, record: Change) {
        let (action, record) = record;
        let indices: Vec<usize> = self
            .expr
            .iter()
            .map(|e| match e {
                Expr::Column(Column { index, .. }) => *index,
                _ => unimplemented!(),
            })
            .collect();

        let new_record = match record.data.project(&indices) {
            Ok(record) => record,
            Err(_) => unimplemented!(),
        };

        let correct_schema = self.output_schema.to_arrow();
        assert!(new_record.schema().eq(&correct_schema));

        let new_record = Record {
            timestamp: record.timestamp,
            data: new_record,
        };

        for out in &context.outs {
            out.send(StreamMessage::Data((action.clone(), new_record.clone())))
                .unwrap();
        }
    }
}

struct InstantJoin {
    //Equijoin conditions
    on: (Vec<Expr>, Vec<Expr>),

    output_schema: Schema,
    left_schema: Schema,
    right_schema: Schema,

    join_type: JoinType,

    left_batch: RecordBatch,
    left_offset: usize,
    right_batch: RecordBatch,
    right_offset: usize,

    //Map from join key to record batches sorted by timestamp
    // left_state: HashMap<u64, BTreeMap<usize, Vec<RecordBatch>>>,
    // right_state: HashMap<u64, BTreeMap<usize, Vec<RecordBatch>>>,

    //Map from join key to record batch indices sorted by timestamp
    left_hashmap: HashMap<u64, BTreeMap<usize, Vec<u64>>>,
    right_hashmap: HashMap<u64, BTreeMap<usize, Vec<u64>>>,
    //Intermediate state for records that have been removed
    // left_deleted: HashMap<u64, BTreeMap<usize, Vec<RecordBatch>>>,
    // right_deleted: HashMap<u64, BTreeMap<usize, Vec<RecordBatch>>>,
}

fn create_hashes(arrays: &[ArrayRef], hashes_buffer: &mut Vec<u64>) -> Vec<u64> {
    for (i, col) in arrays.iter().enumerate() {
        let array = col.as_ref();
        let rehash = i >= 1;

        match array.data_type() {
            arrow::datatypes::DataType::Int32 => {
                let array = array.as_any().downcast_ref::<Int32Array>().unwrap();
                for (i, value) in array.iter().enumerate() {
                    let mut hasher = DefaultHasher::new();
                    value.hash(&mut hasher);
                    if rehash {
                        hashes_buffer[i] = hashes_buffer[i]
                            .wrapping_mul(31)
                            .wrapping_add(hasher.finish());
                    } else {
                        hashes_buffer[i] = hasher.finish();
                    }
                }
            }
            _ => unimplemented!(),
        }
    }
    hashes_buffer.to_vec()
}

#[async_trait]
impl DataflowNode for InstantJoin {
    async fn handle_watermark(&self, context: &mut Context, idx: usize, watermark: usize) {}
    async fn handle_control_message(&self, context: &mut Context, message: ControlMessage) {}
    async fn handle_recordbatch(&mut self, context: &mut Context, idx: usize, record: Change) {
        let (action, record) = record;
        let on = match idx {
            0 => &self.on.0,
            1 => &self.on.1,
            _ => unimplemented!(),
        };

        let offset = match idx {
            0 => self.left_offset,
            1 => self.right_offset,
            _ => unimplemented!(),
        };

        match on.len() {
            1 => {}
            _ => unimplemented!("Only single column joins are supported"),
        }

        let keys: Vec<_> = on
            .iter()
            .map(|expr| expr.evalutate(&record.data).unwrap())
            .collect();

        let hash_buffer = &mut vec![0; record.data.num_rows()];

        let hashes = create_hashes(&keys, hash_buffer);

        let hashes_iter = hashes.iter().enumerate().map(|(i, val)| (i + offset, *val));

        match action {
            Action::Add => {
                let hashmap = match idx {
                    0 => &mut self.left_hashmap,
                    1 => &mut self.right_hashmap,
                    _ => unimplemented!(),
                };
                // Insert into hashmap
                for (row, hash_value) in hashes_iter {
                    let entry = hashmap.entry(hash_value).or_insert_with(BTreeMap::new);
                    let entry = entry.entry(record.timestamp).or_insert_with(Vec::new);
                    entry.push(row as u64);
                }
                println!("{:?}", hashmap);
            }
            Action::Remove => {}
        }

        for out in &context.outs {
            out.send(StreamMessage::Data((
                action.clone(),
                Record {
                    timestamp: record.timestamp,
                    data: record.data.clone(),
                },
            )))
            .unwrap();
        }
    }
}

#[async_trait]
trait DataflowNode {
    async fn handle_watermark(&self, context: &mut Context, idx: usize, watermark: usize);
    async fn handle_watermark_int(&mut self, context: &mut Context, idx: usize, watermark: usize) {
        if watermark <= context.watermarks.watermarks[idx] {
            return;
        }
        context.watermarks.watermarks[idx] = watermark;
        let min_watermark = context.watermarks.watermarks.iter().min().unwrap();
        if *min_watermark > context.watermarks.current_watermark {
            context.watermarks.current_watermark = *min_watermark;
            for out in &context.outs {
                out.send(StreamMessage::Control(ControlMessage::Watermark(
                    *min_watermark,
                )))
                .unwrap();
            }
        }
        self.handle_watermark(context, idx, watermark).await;
    }
    async fn handle_control_message(&self, context: &mut Context, message: ControlMessage);
    async fn handle_recordbatch(&mut self, context: &mut Context, idx: usize, record: Change);
    async fn handle_recordbatch_int(&mut self, context: &mut Context, idx: usize, record: Change) {
        if record.1.timestamp < context.watermarks.current_watermark
            || record.1.timestamp < context.watermarks.watermarks[idx]
        {
            return;
        }
        self.handle_recordbatch(context, idx, record).await;
    }
    async fn run(&mut self, context: &mut Context, mut ins: Vec<UnboundedReceiver<StreamMessage>>) {
        let mut fs = FuturesUnordered::new();
        for (i, stream) in ins.iter_mut().enumerate() {
            let stream = async_stream::stream! {
                while let Some(message) = stream.recv().await {
                    yield (i, message);
                }
            };
            fs.push(Box::pin(stream).into_future());
        }

        loop {
            select! {
                Some((Some((idx, message)), remaining)) = fs.next() => {
                    match message {
                        StreamMessage::Data(change) => {
                            self.handle_recordbatch_int(context, idx, change).await;
                        }
                        StreamMessage::Control(control) => match control {
                            ControlMessage::Watermark(watermark) => {
                                self.handle_watermark_int(context, idx, watermark).await;
                            }
                        }
                    }
                    fs.push(remaining.into_future());
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use crate::{expr::DataType, schema::Field};

    #[tokio::test]
    async fn test_project_node() {
        use super::*;

        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
        ]);

        let output_schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);

        let expr = vec![Expr::Column(Column::new("a", 0))];
        let mut projection = Projection {
            expr,
            output_schema: output_schema.clone(),
        };

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let (output_tx, mut output_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut context = Context {
            outs: vec![output_tx],
            watermarks: WatermarkHolder {
                current_watermark: 0,
                watermarks: vec![0],
            },
        };

        let record = RecordBatch::try_new(
            schema.to_arrow(),
            vec![
                Arc::new(Int32Array::from(vec![1, 4, 7])),
                Arc::new(Int32Array::from(vec![2, 5, 8])),
                Arc::new(Int32Array::from(vec![3, 6, 9])),
            ],
        )
        .unwrap();

        let expected = RecordBatch::try_new(
            output_schema.to_arrow(),
            vec![Arc::new(Int32Array::from(vec![1, 4, 7]))],
        )
        .unwrap();

        let record = Record {
            timestamp: 1,
            data: record,
        };

        let expected_record = Record {
            timestamp: 1,
            data: expected,
        };

        let record = (Action::Add, record);

        tokio::spawn(async move {
            projection.run(&mut context, vec![rx]).await;
        });

        tx.send(StreamMessage::Data(record)).unwrap();

        let message = output_rx.recv().await.unwrap();
        assert_eq!(message, StreamMessage::Data((Action::Add, expected_record)));
    }

    #[tokio::test]
    async fn test_projection_watermark() {
        use super::*;

        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
        ]);

        let output_schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);

        let expr = vec![Expr::Column(Column::new("a", 0))];
        let mut projection = Projection {
            expr,
            output_schema: output_schema.clone(),
        };

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let (output_tx, mut output_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut context = Context {
            outs: vec![output_tx],
            watermarks: WatermarkHolder {
                current_watermark: 0,
                watermarks: vec![0],
            },
        };

        let record = RecordBatch::try_new(
            schema.to_arrow(),
            vec![
                Arc::new(Int32Array::from(vec![1, 4, 7])),
                Arc::new(Int32Array::from(vec![2, 5, 8])),
                Arc::new(Int32Array::from(vec![3, 6, 9])),
            ],
        )
        .unwrap();

        let record = Record {
            timestamp: 1,
            data: record,
        };

        let record = (Action::Add, record);

        tokio::spawn(async move {
            projection.run(&mut context, vec![rx]).await;
        });

        tx.send(StreamMessage::Data(record)).unwrap();

        let _message = output_rx.recv().await.unwrap();

        let watermark = ControlMessage::Watermark(1);
        tx.send(StreamMessage::Control(watermark.clone())).unwrap();

        let message = output_rx.recv().await.unwrap();
        assert_eq!(message, StreamMessage::Control(watermark));
    }

    #[tokio::test]
    async fn test_join() {
        use super::*;

        let left_schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
        ]);

        let right_schema = Schema::new(vec![
            Field::new("d", DataType::Int32, true),
            Field::new("e", DataType::Int32, true),
            Field::new("f", DataType::Int32, true),
        ]);

        let mut join = InstantJoin {
            left_schema: left_schema.clone(),
            right_schema: right_schema.clone(),
            output_schema: Schema::new(vec![Field::new("a", DataType::Int32, true)]),

            on: (
                vec![Expr::Column(Column::new("a", 0))],
                vec![Expr::Column(Column::new("d", 0))],
            ),

            join_type: JoinType::Inner,

            left_batch: RecordBatch::new_empty(left_schema.to_arrow()),
            right_batch: RecordBatch::new_empty(right_schema.to_arrow()),

            left_hashmap: HashMap::new(),
            right_hashmap: HashMap::new(),

            left_offset: 0,
            right_offset: 0,
            // left_state: HashMap::new(),
            // right_state: HashMap::new(),

            // right_deleted: HashMap::new(),
            // left_deleted: HashMap::new(),
        };

        let (left_tx, left_rx) = tokio::sync::mpsc::unbounded_channel();
        let (right_tx, right_rx) = tokio::sync::mpsc::unbounded_channel();
        let (output_tx, mut output_rx) = tokio::sync::mpsc::unbounded_channel();

        let mut context = Context {
            outs: vec![output_tx],
            watermarks: WatermarkHolder {
                current_watermark: 0,
                watermarks: vec![0, 0],
            },
        };

        let left_record = RecordBatch::try_new(
            left_schema.to_arrow(),
            vec![
                Arc::new(Int32Array::from(vec![49, 4, 7, 7])),
                Arc::new(Int32Array::from(vec![2, 5, 8, 12])),
                Arc::new(Int32Array::from(vec![3, 6, 9, 13])),
            ],
        );

        let left_record = Record {
            timestamp: 1,
            data: left_record.unwrap(),
        };

        let left_record = (Action::Add, left_record);

        tokio::spawn(async move {
            join.run(&mut context, vec![left_rx, right_rx]).await;
        });

        left_tx.send(StreamMessage::Data(left_record)).unwrap();

        output_rx.recv().await.unwrap();
    }
}
