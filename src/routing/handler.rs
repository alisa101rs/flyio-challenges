use std::{
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use futures_util::future::BoxFuture;
use tower::Service;

use crate::{
    request::FromRequest,
    response::IntoResponse,
    routing::{Request, Response},
};

pub trait Handler<T>: Clone + Send + Sized + 'static {
    type Future: Future<Output = Result<Response, eyre::Report>> + Send + 'static;

    fn call(self, req: Request) -> Self::Future;
}

pub trait HandlerExt<T>: Handler<T> {
    fn into_service(self) -> HandlerService<Self, T> {
        HandlerService::new(self)
    }
}

impl<T, H> HandlerExt<T> for H where H: Handler<T> {}

pub struct HandlerService<H, T> {
    handler: H,
    ph: PhantomData<fn() -> T>,
}

impl<H, T> Clone for HandlerService<H, T>
where
    H: Clone,
{
    fn clone(&self) -> Self {
        Self {
            handler: self.handler.clone(),
            ph: PhantomData,
        }
    }
}

impl<T, H: Handler<T>> HandlerService<H, T> {
    pub fn new(handler: H) -> Self {
        Self {
            handler,
            ph: PhantomData,
        }
    }
}

impl<T, H> Service<Request> for HandlerService<H, T>
where
    H: Handler<T> + Clone + Send + 'static,
{
    type Response = Response;
    type Error = eyre::Report;
    type Future = BoxFuture<'static, <<H as Handler<T>>::Future as Future>::Output>;

    #[inline]
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request) -> Self::Future {
        use futures_util::future::FutureExt;

        let handler = self.handler.clone();
        let future = Handler::call(handler, req);

        future.boxed()
    }
}

impl<F, Fut, Res> Handler<((),)> for F
where
    F: FnOnce() -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Res> + Send,
    Res: IntoResponse,
{
    type Future = Pin<Box<dyn Future<Output = Result<Response, eyre::Report>> + Send>>;

    fn call(self, _req: Request) -> Self::Future {
        Box::pin(async move { Ok(self().await.into_response()) })
    }
}

macro_rules! impl_handler {
    ($($ty:ident),*) => {
        #[allow(non_snake_case, unused_mut, unused_parens)]
        impl<F, Fut, Res, $($ty,)*> Handler<($($ty,)*)> for F
        where
            F: FnOnce($($ty,)*) -> Fut + Clone + Send + 'static,
            Fut: Future<Output = Res> + Send,
            Res: IntoResponse,
            $( $ty: FromRequest + Send, )*
        {
            type Future = Pin<Box<dyn Future<Output = Result<Response, eyre::Report>> + Send>>;

            fn call(self, req: Request) -> Self::Future {
                Box::pin(async move {
                    $(
                        let $ty = match $ty::from_request(&req) {
                            Ok(value) => value,
                            Err(error) => return Err(error),
                        };
                    )*

                    let res = self($($ty,)*).await;

                    Ok(res.into_response())
                })
            }
        }
    };
}

macro_rules! all_the_tuples {
    ($name:ident) => {
        $name!(T1);
        $name!(T1, T2);
        $name!(T1, T2, T3);
        $name!(T1, T2, T3, T4);
        $name!(T1, T2, T3, T4, T5);
        $name!(T1, T2, T3, T4, T5, T6);
        $name!(T1, T2, T3, T4, T5, T6, T7);
        $name!(T1, T2, T3, T4, T5, T6, T7, T8);
        $name!(T1, T2, T3, T4, T5, T6, T7, T8, T9);
        $name!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10);
        $name!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11);
        $name!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12);
        $name!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13);
        $name!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14);
        $name!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15);
        $name!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16);
    };
}

all_the_tuples!(impl_handler);
