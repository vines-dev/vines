use async_std::task;
use futures::future::FutureExt;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::fs;
use std::fs::File;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
// use vines::FlowResult;
use async_trait::async_trait;
use macros::{VinesOpParams, VinesOp};
use std::any::Any;
use vines;
use vines::dag::OpResults;
use vines::resgiter_node;
use vines::Op;
use vines::OpInfo;
use vines::OpResult;
use vines::OpType;

#[derive(Serialize, Deserialize, Debug, Default)]
struct Val {
    val: i32,
}

struct Req {}

#[derive(Deserialize, Default)]
struct Placeholder {} // TODO: remove

#[VinesOpParams(Val)]
fn calc(graph_args: Arc<Req>, p: Val, input: Arc<OpResults>) -> OpResult {
    let mut r = OpResult::default();
    let mut sum: i32 = 0;
    println!("helllo {:?}", input);
    for idx in 0..input.len() {
        match input.get::<i32>(idx) {
            Ok(val) => sum += val,
            Err(e) => {}
        }
    }
    OpResult::ok(sum + p.val)
}

#[derive(Default)]
struct P {
    x: i32,
    pp: String,
}

#[VinesOp]
fn any_demo<E: Send + Sync>(graph_args: Arc<Req>, input: Arc<OpResults>) -> OpResult {
    OpResult::ok(P::default())
}

fn main() {
    let mut dag = vines::dag::Vines::<Req>::new();
    let data = fs::read_to_string("dag.json").expect("Unable to read file");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .build()
        .unwrap();
    dag.multi_async_register(resgiter_node![calc, any_demo]);
    println!("{:?}", dag.init(&data));
    let my_dag = dag.make_dag(Arc::new(Req{}));
    rt.block_on(my_dag);
}
