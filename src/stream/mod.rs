use std::{
    collections::{BTreeMap, HashMap},
    hash::{DefaultHasher, Hash, Hasher},
    ops::Add,
};

use arrow::{
    array::{new_null_array, ArrayRef, Int32Array, Int64Array, RecordBatch},
    compute::{concat_batches, take},
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
    async fn handle_watermark(&self, context: &mut Context, idx: usize, watermark: usize) {
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
    }

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

    left_hashmap: HashMap<u64, BTreeMap<usize, Vec<u64>>>,
    right_hashmap: HashMap<u64, BTreeMap<usize, Vec<u64>>>,
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
    async fn handle_watermark(&self, context: &mut Context, _idx: usize, _watermark: usize) {
        let min_watermark = context.watermarks.watermarks.iter().min().unwrap();
        if *min_watermark > context.watermarks.current_watermark {
            let prev = context.watermarks.current_watermark;
            context.watermarks.current_watermark = *min_watermark;

            match self.join_type {
                JoinType::Left => {
                    let left: Vec<_> = self
                        .left_hashmap
                        .iter()
                        .flat_map(|(k, v)| {
                            v.iter()
                                .filter(|(ts, _)| **ts > prev && **ts <= *min_watermark)
                                .map(|(ts, v)| (k, (ts, v)))
                                .collect::<Vec<_>>()
                        })
                        .collect();

                    // For left joins, for every record on the left we attempt to find a match on the right
                    // If we don't find a match we need to emit a record with nulls for the right side
                    for (key, (_, left)) in left {
                        //Find the corresponding right records
                        let default = BTreeMap::new();
                        let right = self.right_hashmap.get(key).unwrap_or(&default);
                        //Filter the right records to timestamps in the correct range
                        let right = right
                            .iter()
                            .filter(|(ts, _)| **ts > prev && **ts <= *min_watermark)
                            .flat_map(|(_, v)| v.clone())
                            .collect::<Vec<_>>();

                        let mut left_columns = Vec::new();
                        for col in self.left_batch.columns() {
                            let indices: Int64Array = left.iter().map(|&x| x as i64).collect();
                            let c = take(col, &indices, None).unwrap();
                            left_columns.push(c);
                        }

                        let left = RecordBatch::try_new(self.left_schema.to_arrow(), left_columns)
                            .unwrap();

                        let mut right_columns = Vec::new();
                        //If there are no right records we need to emit a record with nulls
                        if right.is_empty() {
                            for _ in 0..left.num_rows() {
                                for field in self.right_schema.to_arrow().fields() {
                                    let array = new_null_array(field.data_type(), left.num_rows());
                                    right_columns.push(array);
                                }
                            }
                        } else {
                            for col in self.right_batch.columns() {
                                let indices: Int64Array = right.iter().map(|&x| x as i64).collect();
                                let c = take(col, &indices, None).unwrap();
                                right_columns.push(c);
                            }
                        }

                        //For each left record we need to emit a combined record with the right record
                        let right =
                            RecordBatch::try_new(self.right_schema.to_arrow(), right_columns)
                                .unwrap();

                        assert!(left.num_rows() == right.num_rows());

                        let combined_columns = left
                            .columns()
                            .iter()
                            .chain(right.columns().iter())
                            .cloned()
                            .collect();
                        let combined_batch =
                            RecordBatch::try_new(self.output_schema.to_arrow(), combined_columns)
                                .unwrap();

                        for out in &context.outs {
                            out.send(StreamMessage::Data((
                                Action::Add,
                                Record {
                                    timestamp: *min_watermark,
                                    data: combined_batch.clone(),
                                },
                            )))
                            .unwrap();
                        }
                    }
                }
                _ => unimplemented!(),
            };
            // Send updated watermark
            for out in &context.outs {
                out.send(StreamMessage::Control(ControlMessage::Watermark(
                    *min_watermark,
                )))
                .unwrap();
            }
        }
    }
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

        let keys: Vec<_> = on
            .iter()
            .map(|expr| expr.evalutate(&record.data).unwrap())
            .collect();

        let hash_buffer = &mut vec![0; record.data.num_rows()];

        let hashes = create_hashes(&keys, hash_buffer);

        let hashes_iter = hashes.iter().enumerate().map(|(i, val)| (i + offset, *val));

        match action {
            Action::Add => {
                let (hashmap, batch, offset, schema) = match idx {
                    0 => (
                        &mut self.left_hashmap,
                        &mut self.left_batch,
                        &mut self.left_offset,
                        &self.left_schema,
                    ),
                    1 => (
                        &mut self.right_hashmap,
                        &mut self.right_batch,
                        &mut self.right_offset,
                        &self.right_schema,
                    ),
                    _ => unimplemented!(),
                };
                // Insert into hashmap
                for (row, hash_value) in hashes_iter {
                    let entry = hashmap.entry(hash_value).or_insert_with(BTreeMap::new);
                    let entry = entry.entry(record.timestamp).or_insert_with(Vec::new);
                    entry.push(row as u64);
                }

                *batch = concat_batches(&schema.to_arrow(), [&batch, &record.data].iter().cloned())
                    .unwrap();
                *offset = offset.add(record.data.num_rows());
            }
            Action::Remove => {}
        }
    }
}

#[async_trait]
trait DataflowNode {
    async fn handle_watermark(&self, context: &mut Context, idx: usize, watermark: usize);
    async fn handle_watermark_int(&mut self, context: &mut Context, idx: usize, watermark: usize) {
        if watermark <= context.watermarks.watermarks[idx]
            || watermark <= context.watermarks.current_watermark
        {
            return;
        }
        context.watermarks.watermarks[idx] = watermark;
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

    use arrow::{
        array::{Int32Array, RecordBatch, StringArray, UInt8Array},
        compute::{interleave, kernels::interleave, take},
    };

    use crate::{
        expr::DataType,
        schema::{Field, Schema},
    };

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

        let combined_schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
            Field::new("d", DataType::Int32, true),
            Field::new("e", DataType::Int32, true),
            Field::new("f", DataType::Int32, true),
        ]);

        let mut join = InstantJoin {
            left_schema: left_schema.clone(),
            right_schema: right_schema.clone(),
            output_schema: combined_schema,

            on: (
                vec![
                    Expr::Column(Column::new("a", 0)),
                    Expr::Column(Column::new("b", 0)),
                ],
                vec![
                    Expr::Column(Column::new("d", 0)),
                    Expr::Column(Column::new("e", 0)),
                ],
            ),

            join_type: JoinType::Left,

            left_batch: RecordBatch::new_empty(left_schema.to_arrow()),
            right_batch: RecordBatch::new_empty(right_schema.to_arrow()),

            left_hashmap: HashMap::new(),
            right_hashmap: HashMap::new(),

            left_offset: 0,
            right_offset: 0,
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
                Arc::new(Int32Array::from(vec![49, 4, 7, 7, 91])),
                Arc::new(Int32Array::from(vec![2, 5, 8, 12, 92])),
                Arc::new(Int32Array::from(vec![3, 6, 9, 13, 93])),
            ],
        );

        let left_record = Record {
            timestamp: 1,
            data: left_record.unwrap(),
        };

        let left_record = (Action::Add, left_record);

        let right_record = RecordBatch::try_new(
            right_schema.to_arrow(),
            vec![
                Arc::new(Int32Array::from(vec![49, 4, 7, 7])),
                Arc::new(Int32Array::from(vec![2, 5, 8, 12])),
                Arc::new(Int32Array::from(vec![3, 6, 9, 13])),
            ],
        );

        let right_record = Record {
            timestamp: 1,
            data: right_record.unwrap(),
        };

        let right_record = (Action::Add, right_record);

        tokio::spawn(async move {
            join.run(&mut context, vec![left_rx, right_rx]).await;
        });

        left_tx.send(StreamMessage::Data(left_record)).unwrap();
        right_tx.send(StreamMessage::Data(right_record)).unwrap();
        left_tx
            .send(StreamMessage::Control(ControlMessage::Watermark(1)))
            .unwrap();
        right_tx
            .send(StreamMessage::Control(ControlMessage::Watermark(1)))
            .unwrap();

        let message = output_rx.recv().await.unwrap();
        println!("{:?}", message);

        let message = output_rx.recv().await.unwrap();
        println!("{:?}", message);

        let message = output_rx.recv().await.unwrap();
        println!("{:?}", message);

        let message = output_rx.recv().await.unwrap();
        println!("{:?}", message);
    }

    #[tokio::test]
    async fn join_into_project() {
        use super::*;
        let posts_schema = Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
        ]);

        let likes_schema = Schema::new(vec![
            Field::new("like_id", DataType::Int32, true),
            Field::new("post_id", DataType::Int32, true),
        ]);

        let combined_schema = Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("like_id", DataType::Int32, true),
            Field::new("post_id", DataType::Int32, true),
        ]);

        let mut join = InstantJoin {
            left_schema: posts_schema.clone(),
            right_schema: likes_schema.clone(),
            output_schema: combined_schema,

            on: (
                vec![Expr::Column(Column::new("id", 0))],
                vec![Expr::Column(Column::new("post_id", 1))],
            ),

            join_type: JoinType::Left,

            left_batch: RecordBatch::new_empty(posts_schema.to_arrow()),
            right_batch: RecordBatch::new_empty(likes_schema.to_arrow()),

            left_hashmap: HashMap::new(),
            right_hashmap: HashMap::new(),

            left_offset: 0,
            right_offset: 0,
        };

        let (posts_tx, posts_rx) = tokio::sync::mpsc::unbounded_channel();
        let (likes_tx, likes_rx) = tokio::sync::mpsc::unbounded_channel();
        let (output_tx, mut output_rx) = tokio::sync::mpsc::unbounded_channel();

        let mut join_context = Context {
            outs: vec![output_tx],
            watermarks: WatermarkHolder {
                current_watermark: 0,
                watermarks: vec![0, 0],
            },
        };

        let projected_schema = Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("like_id", DataType::Int32, true),
        ]);

        let mut project = Projection {
            expr: vec![
                Expr::Column(Column::new("name", 1)),
                Expr::Column(Column::new("like_id", 2)),
            ],
            output_schema: projected_schema.clone(),
        };

        let (project_output_tx, mut project_output_rx) = tokio::sync::mpsc::unbounded_channel();

        let mut project_context = Context {
            outs: vec![project_output_tx],
            watermarks: WatermarkHolder {
                current_watermark: 0,
                watermarks: vec![0],
            },
        };

        let posts_record = RecordBatch::try_new(
            posts_schema.to_arrow(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(StringArray::from(vec!["hello", "world", "this", "that"])),
            ],
        );

        let posts_record = Record {
            timestamp: 1,
            data: posts_record.unwrap(),
        };

        let posts_record = (Action::Add, posts_record);

        let likes_record = RecordBatch::try_new(
            likes_schema.to_arrow(),
            vec![
                Arc::new(Int32Array::from(vec![20, 21, 22, 23])),
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
            ],
        );

        let likes_record = Record {
            timestamp: 1,
            data: likes_record.unwrap(),
        };

        let likes_record = (Action::Add, likes_record);

        tokio::spawn(async move {
            join.run(&mut join_context, vec![posts_rx, likes_rx]).await;
        });

        tokio::spawn(async move {
            project.run(&mut project_context, vec![output_rx]).await;
        });

        posts_tx.send(StreamMessage::Data(posts_record)).unwrap();
        likes_tx.send(StreamMessage::Data(likes_record)).unwrap();
        posts_tx
            .send(StreamMessage::Control(ControlMessage::Watermark(1)))
            .unwrap();
        likes_tx
            .send(StreamMessage::Control(ControlMessage::Watermark(1)))
            .unwrap();

        let message = project_output_rx.recv().await.unwrap();
        println!("{:?}", message);
    }

    #[tokio::test]
    async fn fucking_around() {
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

        let combined_schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
            Field::new("d", DataType::Int32, true),
            Field::new("e", DataType::Int32, true),
            Field::new("f", DataType::Int32, true),
        ]);

        let left_record = RecordBatch::try_new(
            left_schema.to_arrow(),
            vec![
                Arc::new(Int32Array::from(vec![49, 4, 7, 7])),
                Arc::new(Int32Array::from(vec![2, 5, 8, 12])),
                Arc::new(Int32Array::from(vec![3, 6, 9, 13])),
            ],
        )
        .unwrap();

        let indices = vec![0];
        let indices = Int32Array::from(indices);
        let mut cols = Vec::new();
        for col in left_record.columns() {
            let c = take(col, &indices, None).unwrap();
            cols.push(c);
            //find the row with the value 2
        }

        let new_batch = RecordBatch::try_new(left_schema.to_arrow(), cols).unwrap();
        println!("{:?}", new_batch);
    }
}
