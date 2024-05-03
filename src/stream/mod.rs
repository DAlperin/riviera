use std::pin::Pin;

use arrow::array::RecordBatch;
use async_trait::async_trait;
use futures::stream::FuturesUnordered;
use futures::Stream;
use futures::StreamExt;
use tokio::sync::mpsc;
use tokio::{
    select,
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
};

mod graph;

type Change = (Action, Record);

//Whether we are adding or removing a record. For instance if a record gets removed upstream
//we need to propogate that change downstream
enum Action {
    Add,
    Remove,
}

struct Record {
    timestamp: usize,
    data: RecordBatch,
}

enum StreamMessage {
    Data((Action, Record)),
}

enum ControlMessage {
    Watermark(usize),
}

struct Context {
    control_rx: UnboundedReceiver<ControlMessage>,
}

enum Node {
    Source(Box<dyn DataflowNode>),
    Sink(Box<dyn DataflowNode>),
    Operator(Box<dyn DataflowNode>),
}

#[async_trait]
trait DataflowNode {
    async fn handle_watermark(&self, watermark: usize);
    async fn handle_control_message(&self, message: ControlMessage);
    async fn handle_recordbatch(&self, idx: usize, record: Change);
    async fn random(&self);
    async fn run(
        &self,
        context: &mut Context,
        mut ins: Vec<UnboundedReceiver<StreamMessage>>,
        outs: Vec<UnboundedSender<StreamMessage>>,
    ) {
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
                Some(control_message) = context.control_rx.recv() => {
                    self.handle_control_message(control_message).await;
                }

                Some((Some((idx, message)), remaining)) = fs.next() => {
                    match message {
                        StreamMessage::Data(change) => {
                            self.handle_recordbatch(idx, change).await;
                        }
                    }
                    fs.push(remaining.into_future());
                }
            }
        }
    }
}
