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
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use std::time::SystemTime;
use tower_service::Service;

#[async_trait]
pub trait AnyHandler {
    type Req: Send + Sync;
    fn config_generate(input: Box<RawValue>) -> Arc<dyn Send + Any + Sync>;

    async fn async_calc2(
        _graph_args: Self::Req,
        params: Arc<dyn Any + Send + Sync>,
        input: Arc<OpResults>,
    ) -> OpResult;
}

pub struct HandlerInfo {
    pub name: &'static str,
    pub method_type: HandlerType,
    pub has_config: bool,
}

pub enum HandlerType {
    Async,
    Sync,
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
    prevs: Vec<String>,
    nexts: Vec<String>,
}

#[derive(Clone)]
pub struct Vines<E: Send + Sync> {
    ops: HashMap<String, Box<DAGOp>>,
    async_op_mapping: HashMap<
        String,
        Arc<
            Mutex<
                dyn Service<
                        (Arc<E>, Arc<dyn Any + Sync + Send>, Arc<OpResults>),
                        Response = OpResult,
                        Error = &'static str,
                        Future = AsyncHandlerFuture,
                    > + Send
                    + Sync,
            >,
        >,
    >,
    // cache
    cached_repo: Arc<dashmap::DashMap<String, (Arc<OpResult>, SystemTime)>>,
    // config cache
    op_config_repo: HashMap<String, Arc<dyn Any + std::marker::Send + Sync>>,
    has_op_config_repo: HashMap<String, bool>,
    op_config_generator_repo: HashMap<
        String,
        Arc<dyn Fn(Box<RawValue>) -> Arc<dyn Any + std::marker::Send + Sync> + Send + Sync>,
    >,
}

impl<E: 'static + Send + Sync> Default for Vines<E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E: 'static + Send + Sync> Vines<E> {
    pub fn new<'a>() -> Vines<E> {
        Vines {
            ops: HashMap::new(),
            async_op_mapping: HashMap::new(),
            cached_repo: Arc::new(DashMap::new()),
            op_config_repo: HashMap::new(),
            op_config_generator_repo: HashMap::new(),
            has_op_config_repo: HashMap::new(),
        }
    }

    pub fn async_register<H>(&mut self, op_name: &str, handler: H)
    where
        H: AsyncHandler<E>,
    {
        self.async_op_mapping.insert(
            op_name.to_string(),
            Arc::new(Mutex::new(Vines::<E>::wrap(handler))),
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
        H: AsyncHandler<E>,
    {
        for pair in handlers_ganerator() {
            self.async_op_mapping.insert(
                pair.0.to_string(),
                Arc::new(Mutex::new(Vines::<E>::wrap(pair.1))),
            );
            self.op_config_generator_repo
                .insert(pair.0.to_string(), pair.2);
            self.has_op_config_repo.insert(pair.0.to_string(), pair.3);
        }
    }

    fn wrap<H>(handler: H) -> AsyncContainer<H>
    where
        H: AsyncHandler<E>,
    {
        AsyncContainer { handler }
    }

    pub fn init(&mut self, conf_content: &str) -> Result<(), String> {
        let dag_config: DAGConfig = serde_json::from_str(conf_content).unwrap_or_else(|error| {
            panic!("invalid graph {:?}", error);
        });
        self.ops = dag_config
            .ops
            .iter()
            .map(|op_config| {
                (
                    op_config.name.clone(),
                    Box::new(DAGOp {
                        op_config: op_config.clone(),
                        nexts: Vec::new(),
                        prevs: Vec::new(),
                    }),
                )
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
                if !self.ops.contains_key(&dep.clone()) {
                    return Err(format!(
                        "{:?}'s dependency {:?} do not exist",
                        op_config.name, dep
                    ));
                }
                self.ops
                    .get_mut(&op_config.name.clone())
                    .unwrap()
                    .prevs
                    .push(dep.clone());
                self.ops
                    .get_mut(&dep.clone())
                    .unwrap()
                    .nexts
                    .push(op_config.name.clone());
            }

            if *self.has_op_config_repo.get(&op_config.op).unwrap() {
                self.op_config_repo.insert(
                    op_config.name.clone(),
                    self.op_config_generator_repo.get(&op_config.op).unwrap()(
                        op_config.params.clone(),
                    ),
                );
            }
        }

        for root in self
            .ops
            .values()
            .filter(|op| op.prevs.is_empty())
            .map(|op| op.op_config.name.clone())
        {
            if !self.validate_dag(&mut HashSet::new(), root.to_string()) {
                return Err(("have cycle in the dag").to_string());
            }
        }

        Ok(())
    }

    fn validate_dag<'a>(&self, path: &mut HashSet<String>, cur: String) -> bool {
        if path.contains(&cur) {
            return false;
        }
        let op = self.ops.get(&cur).unwrap();
        if op.nexts.is_empty() {
            return true;
        }

        path.insert(cur.clone());
        for next in &op.nexts {
            if !self.validate_dag(&mut path.clone(), next.to_string()) {
                return false;
            }
        }
        true
    }

    pub async fn make_dag(&self, args: Arc<E>) -> Vec<Arc<OpResult>> {
        let leaf_ops: HashSet<String> = self
            .ops
            .values()
            .filter(|op| op.nexts.is_empty())
            .map(|op| op.op_config.name.clone())
            .collect();

        // let have_handled: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let ops_ptr: Arc<HashMap<String, Arc<DAGOp>>> = Arc::new(
            self.ops
                .iter()
                .map(|(k, v)| (k.clone(), Arc::new(*v.clone())))
                .collect(),
        );
        let dag_futures_ptr: Arc<
            Mutex<
                HashMap<
                    std::string::String,
                    Shared<
                        Pin<Box<dyn futures::Future<Output = Arc<OpResult>> + std::marker::Send>>,
                    >,
                >,
            >,
        > = Arc::new(Mutex::new(HashMap::new()));

        leaf_ops.iter().for_each(|leaf| {
            let dag_futures_ptr_copy = Arc::clone(&dag_futures_ptr);
            dag_futures_ptr.lock().unwrap().insert(
                leaf.to_string(),
                Vines::<E>::dfs_op(
                    dag_futures_ptr_copy,
                    // Arc::clone(&have_handled),
                    Arc::clone(&ops_ptr),
                    leaf.clone(),
                    Arc::new(
                        self.async_op_mapping
                            .iter()
                            .map(|(key, val)| (key.clone(), Arc::clone(val)))
                            .collect(),
                    ),
                    Arc::clone(&args),
                    Arc::clone(&self.cached_repo),
                    Arc::new(
                        self.op_config_repo
                            .iter()
                            .map(|(key, val)| (key.clone(), Arc::clone(val)))
                            .collect(),
                    ),
                    Arc::new(
                        self.has_op_config_repo
                            .iter()
                            .map(|(key, val)| (key.clone(), *val))
                            .collect(),
                    ),
                )
                .boxed()
                .shared(),
            );
        });

        let mut leaves: futures::stream::FuturesOrdered<_> = leaf_ops
            .iter()
            .map(|leaf| dag_futures_ptr.lock().unwrap().get(&*leaf).unwrap().clone())
            .collect();

        let mut results = Vec::with_capacity(leaves.len());

        while let Some(item) = leaves.next().await {
            results.push(item);
        }
        results
    }

    #[async_recursion]
    async fn dfs_op<'a>(
        dag_futures: Arc<
            Mutex<
                HashMap<
                    std::string::String,
                    Shared<
                        Pin<Box<dyn futures::Future<Output = Arc<OpResult>> + std::marker::Send>>,
                    >,
                >,
            >,
        >,
        // have_handled: Arc<Mutex<HashSet<String>>>,
        ops: Arc<HashMap<String, Arc<DAGOp>>>,
        op: String,
        async_op_mapping: Arc<
            HashMap<
                String,
                Arc<
                    Mutex<
                        dyn Service<
                                (Arc<E>, Arc<dyn Any + Sync + Send>, Arc<OpResults>),
                                Response = OpResult,
                                Error = &'static str,
                                Future = AsyncHandlerFuture,
                            > + Send
                            + Sync,
                    >,
                >,
            >,
        >,
        args: Arc<E>,
        cached_repo: Arc<dashmap::DashMap<String, (Arc<OpResult>, SystemTime)>>,
        op_config_repo: Arc<HashMap<String, Arc<dyn Any + std::marker::Send + Sync>>>,
        has_op_config_repo: Arc<HashMap<String, bool>>,
    ) -> Arc<OpResult> {
        let mut deps = futures::stream::FuturesOrdered::new();
        if ops.get(&op).unwrap().prevs.is_empty() {
            deps.push(async { Arc::new(OpResult::default()) }.boxed().shared());
        } else {
            deps = ops
                .get(&op)
                .unwrap()
                .prevs
                .iter()
                .map(|prev| {
                    if !dag_futures.lock().unwrap().contains_key(&prev.to_string()) {
                        let prev_ptr = Arc::new(prev);
                        dag_futures.lock().unwrap().insert(
                            prev.to_string(),
                            Vines::<E>::dfs_op(
                                Arc::clone(&dag_futures),
                                // Arc::clone(&have_handled),
                                Arc::clone(&ops),
                                prev_ptr.to_string(),
                                Arc::clone(&async_op_mapping),
                                Arc::clone(&args),
                                Arc::clone(&cached_repo),
                                Arc::clone(&op_config_repo),
                                Arc::clone(&has_op_config_repo),
                            )
                            .boxed()
                            .shared(),
                        );
                    }
                    dag_futures.lock().unwrap().get(prev).unwrap().clone()
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

        let params_ptr = op_config_repo.get(&op).unwrap();
        let async_handle_fn = Arc::clone(
            async_op_mapping
                .get(&ops.get(&op).unwrap().op_config.op)
                .unwrap(),
        );
        let arg_ptr = Arc::clone(&args);

        let now = SystemTime::now();
        let res = if ops.get(&op).unwrap().op_config.cachable
            && cached_repo.contains_key(&op)
            && now.duration_since(cached_repo.get(&op).unwrap().1).unwrap()
                > Duration::from_secs(60)
        {
            Arc::clone(&cached_repo.get(&op).unwrap().0)
        } else {
            let async_res = Arc::new(
                async {
                    let v = async_handle_fn.lock().unwrap().call((
                        Arc::clone(&arg_ptr),
                        params_ptr.clone(),
                        Arc::clone(&dep_results),
                    ));
                    v.await.unwrap()
                }
                .await,
            );

            if ops.get(&op).unwrap().op_config.cachable {
                cached_repo.insert(op.clone(), (Arc::clone(&async_res), SystemTime::now()));
            }
            async_res
        };

        if res.is_err() && ops.get(&op).unwrap().op_config.necessary {
            return Arc::new(OpResult::default());
        }
        res
    }
}

#[async_trait]
pub trait AsyncHandler<E>: Clone + Sync + Send + Sized + 'static {
    async fn call(self, q: Arc<E>, e: Arc<dyn Any + Sync + Send>, w: Arc<OpResults>) -> OpResult;
}

#[async_trait]
impl<F, Fut, E> AsyncHandler<E> for F
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
    B: AsyncHandler<E>,
{
    type Response = OpResult;
    type Error = &'static str;
    type Future = AsyncHandlerFuture;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, q: (Arc<E>, Arc<dyn Any + Sync + Send>, Arc<OpResults>)) -> Self::Future {
        let ft = AsyncHandler::call(self.handler.clone(), q.0, q.1, q.2);
        AsyncHandlerFuture { inner: ft }
    }
}

#[pin_project]
pub struct AsyncHandlerFuture {
    #[pin]
    inner: BoxFuture<'static, OpResult>,
}

impl Future for AsyncHandlerFuture {
    type Output = Result<OpResult, &'static str>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let cur = ready!(this.inner.poll(cx));
        Poll::Ready(Ok(cur))
    }
}
