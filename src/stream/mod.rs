use arrow::array::RecordBatch;
use async_trait::async_trait;

mod graph;

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

struct Context {}

#[async_trait]
trait DataflowNode {
    async fn handle_watermark(&self, watermark: usize);
    async fn handle_control_message(&self, message: ControlMessage);
    async fn run(
        &self,
        ins: Vec<UnboundedReceiver<StreamMessage>>,
        outs: Vec<UnboundedSender<StreamMessage>>,
    );
}
