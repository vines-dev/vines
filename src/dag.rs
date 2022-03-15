use async_recursion::async_recursion;
use async_trait::async_trait;
use dashmap::DashMap;
use futures::future::FutureExt;
use futures::future::Shared;
use futures::Future;
use futures::StreamExt;
use futures::{future::BoxFuture, ready};
use pin_project::pin_project;
use serde::Deserialize;
use serde_json::value::RawValue;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::{Arc, Mutex, RwLock};
use std::task::{Context, Poll};
use std::time::Duration;
use std::time::SystemTime;
use tower::util::BoxCloneService;
use tower_service::Service;

#[async_trait]
pub trait Op {
    type Req: Send + Sync;
    fn gen_config(input: Box<RawValue>) -> Arc<dyn Send + Any + Sync>;

    async fn call(
        args: Self::Req,
        params: Arc<dyn Any + Send + Sync>,
        input: Arc<OpResults>,
    ) -> OpResult;
}

pub struct OpInfo {
    pub name: &'static str,
    pub method_type: OpType,
    pub has_config: bool,
}

pub enum OpType {
    Op,
}

#[derive(Deserialize, Default)]
pub struct EmptyPlaceHolder {}

#[derive(Debug, Clone)]
pub enum OpResult {
    Ok(Arc<dyn Any + std::marker::Send + std::marker::Sync>),
    Err(String),
    Cancel,
    None,
}

unsafe impl Send for OpResult {}
unsafe impl Sync for OpResult {}

#[derive(Debug)]
pub struct OpResults {
    inner: Vec<Arc<OpResult>>,
}

impl OpResults {
    pub fn get<T: Any>(&self, idx: usize) -> Result<&T, &'static str> {
        if idx >= self.inner.len() {
            Err("out of index")
        } else {
            self.inner[idx].get::<T>()
        }
    }

    pub fn mget<T: Any>(&self) -> Result<Vec<&T>, &'static str> {
        let mut ret = Vec::with_capacity(self.len());
        for idx in 0..self.inner.len() {
            match self.get::<T>(idx) {
                Ok(val) => ret.push(val),
                Err(e) => return Err(e),
            }
        }
        Ok(ret)
    }

    pub fn is_err(&self, idx: usize) -> bool {
        if idx >= self.inner.len() {
            false
        } else {
            self.inner[idx].is_err()
        }
    }

    pub fn get_err(&self, idx: usize) -> Option<&str> {
        if idx >= self.inner.len() {
            None
        } else {
            self.inner[idx].get_err()
        }
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn get_arc(
        &self,
        idx: usize,
    ) -> Option<Arc<dyn Any + std::marker::Send + std::marker::Sync>> {
        if idx >= self.inner.len() {
            None
        } else {
            self.inner[idx].get_arc()
        }
    }

    pub fn at(&self, idx: usize) -> Option<&OpResult> {
        if idx >= self.inner.len() {
            None
        } else {
            Some(&self.inner[idx])
        }
    }
}

impl OpResult {
    pub fn get<T: Any>(&self) -> Result<&T, &'static str> {
        match &self {
            OpResult::Ok(val) => match val.downcast_ref::<T>() {
                Some(val) => Ok(val),
                None => Err("invalid type"),
            },
            OpResult::Cancel => Err("calceled"),
            OpResult::Err(_e) => Err("is a error"),
            OpResult::None => Err("value is none"),
        }
    }

    pub fn is_err(&self) -> bool {
        match &self {
            OpResult::Ok(_) => false,
            OpResult::Err(_) => true,
            OpResult::Cancel => false,
            OpResult::None => false,
        }
    }

    pub fn get_err(&self) -> Option<&str> {
        match &self {
            OpResult::Ok(_) => None,
            OpResult::Err(e) => Some(e),
            OpResult::None => None,
            OpResult::Cancel => None,
        }
    }

    pub fn ok<T: Any + Send + Sync>(t: T) -> OpResult {
        OpResult::Ok(Arc::new(t))
    }

    fn get_arc(&self) -> Option<Arc<dyn Any + std::marker::Send + std::marker::Sync>> {
        match &self {
            OpResult::Ok(val) => Some(Arc::clone(val)),
            OpResult::Err(_e) => None,
            OpResult::None => None,
            OpResult::Cancel => None,
        }
    }
}

impl Default for OpResult {
    fn default() -> Self {
        OpResult::None
    }
}

#[derive(Deserialize, Default, Debug, Clone)]
struct OpConfig {
    name: String,
    op: String,
    deps: Vec<String>,
    #[serde(default)]
    params: Box<RawValue>,
    #[serde(default)]
    necessary: bool,
    #[serde(default)]
    cachable: bool,
}

#[derive(Deserialize, Default, Debug, Clone)]
pub struct DAGConfig {
    ops: Vec<OpConfig>,
}

#[derive(Debug, Clone)]
pub struct DAGOp {
    op_config: OpConfig,
    prevs: Vec<usize>,
    nexts: Vec<usize>,
}

impl<E: Send + Sync> Clone for Vines<E> {
    fn clone(&self) -> Self {
        Vines {
            ops: self.ops.iter().map(|v| Arc::clone(v)).collect(),
            async_op_mapping: self
                .async_op_mapping
                .iter()
                .map(|(k, v)| (k.clone(), Mutex::new(v.lock().unwrap().clone())))
                .collect(),
            cached_repo: self.cached_repo.clone(),
            op_config_repo: self.op_config_repo.iter().map(|v| Arc::clone(v)).collect(),
            has_op_config_repo: self.has_op_config_repo.clone(),
            op_config_generator_repo: self
                .op_config_generator_repo
                .iter()
                .map(|(k, v)| (k.clone(), Arc::clone(v)))
                .collect(),
        }
    }
}

// #[derive(Clone)]
pub struct Vines<E: Send + Sync> {
    ops: Vec<Arc<DAGOp>>,
    async_op_mapping: HashMap<
        String,
        Mutex<
            BoxCloneService<
                (Arc<E>, Arc<dyn Any + Sync + Send>, Arc<OpResults>),
                OpResult,
                &'static str,
            >,
        >,
    >,
    // >,
    // async_op_mapping: HashMap<
    //     String,
    //     Arc<
    //         Mutex<
    //             dyn Service<
    //                     (Arc<E>, Arc<dyn Any + Sync + Send>, Arc<OpResults>),
    //                     Response = OpResult,
    //                     Error = &'static str,
    //                     Future = AsyncOpFuture,
    //                 > + Send
    //                 + Sync,
    //         >,
    //     >,
    // >,
    // cache
    cached_repo: Arc<dashmap::DashMap<String, (Arc<OpResult>, SystemTime)>>,
    // config cache
    op_config_repo: Vec<Arc<dyn Any + std::marker::Send + Sync>>,
    has_op_config_repo: HashMap<String, bool>,
    op_config_generator_repo: HashMap<
        String,
        Arc<dyn Fn(Box<RawValue>) -> Arc<dyn Any + std::marker::Send + Sync> + Send + Sync>,
    >,
    // middlewares: HashMap<
    //     String,
    //     BoxCloneService<
    //         (Arc<E>, Arc<dyn Any + Sync + Send>, Arc<OpResults>),
    //         OpResult,
    //         &'static str,
    //     >,
    // >,
}

impl<E: 'static + Send + Sync> Default for Vines<E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E: 'static + Send + Sync> Vines<E> {
    pub fn new<'a>() -> Vines<E> {
        Vines {
            ops: Vec::new(),
            async_op_mapping: HashMap::new(),
            cached_repo: Arc::new(DashMap::new()),
            op_config_repo: Vec::new(),
            op_config_generator_repo: HashMap::new(),
            has_op_config_repo: HashMap::new(),
            // middlewares: HashMap::new(),
        }
    }

    pub fn async_register<H>(&mut self, op_name: &str, handler: H)
    where
        H: AsyncOp<E>,
    {
        self.async_op_mapping.insert(
            op_name.to_string(),
            Mutex::new(BoxCloneService::new(Vines::<E>::wrap(handler))),
            // Arc::new(Mutex::new(Vines::<E>::wrap(handler))),
        );
    }

    pub fn multi_async_register<H>(
        &mut self,
        handlers_ganerator: &dyn Fn() -> Vec<(
            &'static str,
            H,
            Arc<dyn Fn(Box<RawValue>) -> Arc<(dyn Any + std::marker::Send + Sync)> + Send + Sync>,
            bool,
        )>,
    ) where
        H: AsyncOp<E>,
    {
        for pair in handlers_ganerator() {
            self.async_op_mapping.insert(
                pair.0.to_string(),
                Mutex::new(BoxCloneService::new(Vines::<E>::wrap(pair.1))),
                // Arc::new(Mutex::new(Vines::<E>::wrap(pair.1))),
            );
            self.op_config_generator_repo
                .insert(pair.0.to_string(), pair.2);
            self.has_op_config_repo.insert(pair.0.to_string(), pair.3);
        }
    }

    fn wrap<H>(handler: H) -> AsyncContainer<H>
    where
        H: AsyncOp<E>,
    {
        AsyncContainer { handler }
    }

    pub fn init(&mut self, conf_content: &str) -> Result<(), String> {
        let dag_config: DAGConfig = serde_json::from_str(conf_content).unwrap_or_else(|error| {
            panic!("invalid graph {:?}", error);
        });
        let ops_table: HashMap<String, usize> = dag_config
            .ops
            .iter()
            .zip(0..dag_config.ops.len())
            .map(|(op_config, idx)| (op_config.name.clone(), idx))
            .collect();
        let mut ops_tmp: Vec<Box<DAGOp>> = dag_config
            .ops
            .iter()
            .map(|op_config| {
                // op_config.name.clone(),
                Box::new(DAGOp {
                    op_config: op_config.clone(),
                    nexts: Vec::new(),
                    prevs: Vec::new(),
                })
            })
            .collect();

        for op_config in dag_config.ops.iter() {
            for dep in op_config.deps.iter() {
                // unknown op name
                if !self.async_op_mapping.contains_key(&op_config.op) {
                    return Err(format!(
                        "miss handler {:?}, do you forget to register handler?",
                        op_config.op
                    ));
                }

                // dep equals it self
                if dep == &op_config.name {
                    return Err(format!("{:?} depend itself", op_config.name));
                }
                // duplicate op
                if !ops_table.contains_key(&dep.clone()) {
                    return Err(format!(
                        "{:?}'s dependency {:?} do not exist",
                        op_config.name, dep
                    ));
                }
                ops_tmp[ops_table.get(&op_config.name.clone()).unwrap().clone()]
                    .prevs
                    .push(ops_table.get(&dep.clone()).unwrap().clone());
                ops_tmp[ops_table.get(&dep.clone()).unwrap().clone()]
                    .nexts
                    .push(ops_table.get(&op_config.name).unwrap().clone());
            }

            if *self.has_op_config_repo.get(&op_config.op).unwrap() {
                self.op_config_repo.push(
                    self.op_config_generator_repo.get(&op_config.op).unwrap()(
                        op_config.params.clone(),
                    ),
                );
            }
        }
        self.ops = ops_tmp.iter().map(|v| Arc::new(*v.clone())).collect();

        for root in self
            .ops
            .iter()
            .filter(|op| op.prevs.is_empty())
            .map(|op| ops_table.get(&op.op_config.name.clone()).unwrap().clone())
        {
            if !self.validate_dag(&mut HashSet::new(), root) {
                return Err(("have cycle in the dag").to_string());
            }
        }

        Ok(())
    }

    fn validate_dag<'a>(&self, path: &mut HashSet<usize>, cur: usize) -> bool {
        if path.contains(&cur) {
            return false;
        }
        let op = &self.ops[cur];
        if op.nexts.is_empty() {
            return true;
        }

        path.insert(cur.clone());
        for next in &op.nexts {
            if !self.validate_dag(&mut path.clone(), *next) {
                return false;
            }
        }
        true
    }

    pub async fn make_dag(&self, args: Arc<E>) -> Vec<Arc<OpResult>> {
        let leaf_ops: HashSet<usize> = self
            .ops
            .iter()
            .zip(0..self.ops.len())
            .filter(|(op, idx)| op.nexts.is_empty())
            .map(|(op, idx)| idx)
            .collect();

        // let have_handled: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let ops_ptr: Arc<Vec<Arc<DAGOp>>> =
            Arc::new(self.ops.iter().map(|v| Arc::clone(v)).collect());
        let dag_futures_ptr: Arc<
            RwLock<
                HashMap<
                    usize,
                    Shared<
                        Pin<Box<dyn futures::Future<Output = Arc<OpResult>> + std::marker::Send>>,
                    >,
                >,
            >,
        > = Arc::new(RwLock::new(HashMap::new()));
        // > = Arc::new(HashMap::new());

        leaf_ops.iter().for_each(|leaf| {
            let dag_futures_ptr_copy = Arc::clone(&dag_futures_ptr);
            // (*dag_futures_ptr).insert(
            dag_futures_ptr.write().unwrap().insert(
                *leaf,
                Vines::<E>::dfs_op(
                    dag_futures_ptr_copy,
                    // Arc::clone(&have_handled),
                    Arc::clone(&ops_ptr),
                    *leaf,
                    Arc::new(
                        self.async_op_mapping
                            .iter()
                            .map(|(key, val)| {
                                (
                                    key.clone(),
                                    Arc::new(Mutex::new(val.lock().unwrap().clone())),
                                )
                            })
                            .collect(),
                    ),
                    Arc::clone(&args),
                    Arc::clone(&self.cached_repo),
                    Arc::new(
                        self.op_config_repo
                            .iter()
                            .map(|val| Arc::clone(val))
                            .collect(),
                    ),
                )
                .boxed()
                .shared(),
            );
        });

        let mut leaves: futures::stream::FuturesOrdered<_> = leaf_ops
            .iter()
            .map(|leaf| dag_futures_ptr.read().unwrap().get(&*leaf).unwrap().clone())
            // .map(|leaf| dag_futures_ptr.get(&*leaf).unwrap().clone())
            .collect();

        let mut results = Vec::with_capacity(leaves.len());

        while let Some(item) = leaves.next().await {
            results.push(item);
        }
        results
    }

    // TODO: String to int
    #[async_recursion]
    async fn dfs_op<'a>(
        dag_futures: Arc<
            RwLock<
                HashMap<
                    usize,
                    Shared<
                        Pin<Box<dyn futures::Future<Output = Arc<OpResult>> + std::marker::Send>>,
                    >,
                >,
            >,
        >,
        // have_handled: Arc<Mutex<HashSet<String>>>,
        ops: Arc<Vec<Arc<DAGOp>>>,
        op: usize,
        async_op_mapping: Arc<
            HashMap<
                String,
                Arc<
                    Mutex<
                        BoxCloneService<
                            (Arc<E>, Arc<dyn Any + Sync + Send>, Arc<OpResults>),
                            OpResult,
                            &'static str,
                        >,
                    >,
                >,
            >,
        >,
        //     async_op_mapping: Arc<
        //     HashMap<
        //         String,
        //         Arc<
        //             Mutex<
        //                 dyn Service<
        //                         (Arc<E>, Arc<dyn Any + Sync + Send>, Arc<OpResults>),
        //                         Response = OpResult,
        //                         Error = &'static str,
        //                         Future = AsyncOpFuture,
        //                     > + Send
        //                     + Sync,
        //             >,
        //         >,
        //     >,
        // >,
        args: Arc<E>,
        cached_repo: Arc<dashmap::DashMap<String, (Arc<OpResult>, SystemTime)>>,
        op_config_repo: Arc<Vec<Arc<dyn Any + std::marker::Send + Sync>>>,
    ) -> Arc<OpResult> {
        let mut deps = futures::stream::FuturesOrdered::new();
        if ops[op].prevs.is_empty() {
            deps.push(async { Arc::new(OpResult::default()) }.boxed().shared());
        } else {
            deps = ops[op]
                .prevs
                .iter()
                .map(|prev| {
                    if !dag_futures.read().unwrap().contains_key(&prev) {
                        // if !dag_futures.contains_key(&prev.to_string()) {
                        // let prev_ptr = Arc::new(prev);
                        dag_futures.write().unwrap().insert(
                            // dag_futures.insert(
                            *prev,
                            Vines::<E>::dfs_op(
                                Arc::clone(&dag_futures),
                                Arc::clone(&ops),
                                *prev,
                                Arc::clone(&async_op_mapping),
                                Arc::clone(&args),
                                Arc::clone(&cached_repo),
                                Arc::clone(&op_config_repo),
                            )
                            .boxed()
                            .shared(),
                        );
                    }
                    dag_futures.read().unwrap().get(prev).unwrap().clone()
                    // dag_futures.get(prev).unwrap().clone()
                })
                .collect()
        };

        // blocking
        let mut deps_collector = Vec::with_capacity(deps.len());
        while let Some(item) = deps.next().await {
            deps_collector.push(item);
        }

        let dep_results = Arc::new(OpResults {
            inner: deps_collector,
        });

        let params_ptr = &op_config_repo[op];
        let mut async_handle_fn = async_op_mapping
            .get(&ops[op].op_config.op)
            .unwrap()
            .lock()
            .unwrap()
            .clone();
        // let mut async_handle_fn = BoxCloneService::new(async_op_mapping
        //     .get(&ops.get(&op).unwrap().op_config.op)
        //     .unwrap()
        //     .lock()
        //     .unwrap());
        let arg_ptr = Arc::clone(&args);

        Arc::new(
            async move {
                match async_handle_fn
                    .call((arg_ptr, params_ptr.clone(), dep_results))
                    .await
                {
                    Ok(async_result) => {
                        if async_result.is_err() && ops[op].op_config.necessary {
                            return OpResult::default();
                        } else {
                            async_result
                        }
                    }
                    Err(e) => OpResult::Err(e.to_string()),
                }
            }
            .await,
        )
    }
}

#[async_trait]
pub trait AsyncOp<E>: Clone + Sync + Send + Sized + 'static {
    async fn call(self, q: Arc<E>, e: Arc<dyn Any + Sync + Send>, w: Arc<OpResults>) -> OpResult;
}

#[async_trait]
impl<F, Fut, E> AsyncOp<E> for F
where
    F: FnOnce(Arc<E>, Arc<dyn Any + Sync + Send>, Arc<OpResults>) -> Fut
        + Clone
        + Send
        + Sync
        + 'static,
    Fut: Future<Output = OpResult> + Send,
    E: Send + Sync + 'static,
{
    async fn call(self, q: Arc<E>, e: Arc<dyn Any + Sync + Send>, w: Arc<OpResults>) -> OpResult {
        self(q, e, w).await
    }
}

#[derive(Clone, Copy)]
struct AsyncContainer<B> {
    handler: B,
}

unsafe impl<B> Send for AsyncContainer<B> {}
unsafe impl<B> Sync for AsyncContainer<B> {}

impl<B, E> Service<(Arc<E>, Arc<dyn Any + Sync + Send>, Arc<OpResults>)> for AsyncContainer<B>
where
    B: AsyncOp<E>,
{
    type Response = OpResult;
    type Error = &'static str;
    type Future = AsyncOpFuture;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, q: (Arc<E>, Arc<dyn Any + Sync + Send>, Arc<OpResults>)) -> Self::Future {
        let ft = AsyncOp::call(self.handler.clone(), q.0, q.1, q.2);
        AsyncOpFuture { inner: ft }
    }
}

#[pin_project]
pub struct AsyncOpFuture {
    #[pin]
    inner: BoxFuture<'static, OpResult>,
}

impl Future for AsyncOpFuture {
    type Output = Result<OpResult, &'static str>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let cur = ready!(this.inner.poll(cx));
        Poll::Ready(Ok(cur))
    }
}
