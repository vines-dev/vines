pub mod dag;
pub use dag::AsyncOp;
pub use dag::EmptyPlaceHolder;
pub use dag::OpType;
pub use dag::Op;
pub use dag::OpInfo;
pub use dag::OpResult;
pub use dag::OpResults;
pub use dag::Vines;

#[macro_export]
macro_rules! resgiter_node{
    ( $($x:ident),* ) => {
        &|| {
            let mut data: Vec<(
                &'static str,
                fn(
                    Arc<_>,
                    Arc<_>,
                    Arc<_>,
                ) -> Pin<Box<dyn futures::Future<Output = OpResult> + std::marker::Send>>,
                Arc<dyn Fn(Box<serde_json::value::RawValue>) -> Arc<(dyn Any + std::marker::Send + Sync)> + Send + Sync>,
                bool,
            )> = Vec::new();
            $(
                #[allow(unused_assignments)]
                {
                    data.push((
                        $x::generate_config().name,
                        $x::call,
                        Arc::new($x::gen_config),
                        $x::generate_config().has_config,
                    ));
                }
            )*
            data
        }

    };
}
