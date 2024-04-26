use std::task::{Context, Poll};

use derive_more::DebugCustom;
use parking_lot::Mutex;
use route_future::RouteFuture;
use tower::{
    util::{BoxCloneService, Oneshot},
    Layer, Service, ServiceExt,
};

use crate::{request::Request, response::Response};

type Error = eyre::Report;

#[derive(DebugCustom)]
#[debug(fmt = "Route")]
pub struct Route(Mutex<BoxCloneService<Request, Response, Error>>);

impl Clone for Route {
    fn clone(&self) -> Self {
        Self::new(self.0.lock().clone())
    }
}

impl Route {
    pub(crate) fn new<T>(svc: T) -> Self
    where
        T: Service<Request, Response = Response, Error = Error> + Clone + Send + 'static,
        T::Future: Send + 'static,
    {
        Self(Mutex::new(BoxCloneService::new(svc)))
    }

    pub(crate) fn oneshot_inner(
        &mut self,
        req: Request,
    ) -> Oneshot<BoxCloneService<Request, Response, Error>, Request> {
        self.0.lock().clone().oneshot(req)
    }

    pub(crate) fn layer<L>(self, layer: L) -> Route
    where
        L: Layer<Route> + Clone + Send + 'static,
        L::Service: Service<Request, Response = Response, Error = Error> + Clone + Send + 'static,
        <L::Service as Service<Request>>::Future: Send + 'static,
    {
        Route::new(layer.layer(self))
    }
}

impl Service<Request> for Route {
    type Error = Error;
    type Future = RouteFuture;
    type Response = Response;

    #[inline]
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    #[inline]
    fn call(&mut self, req: Request) -> Self::Future {
        RouteFuture::from_future(self.oneshot_inner(req))
    }
}

pub mod route_future {
    use std::{
        future::Future,
        pin::Pin,
        task::{Context, Poll},
    };

    use pin_project_lite::pin_project;
    use tower::util::{BoxCloneService, Oneshot};

    use crate::routing::{Request, Response};

    pin_project! {
        /// Response future for [`Route`].
        pub struct RouteFuture {
            #[pin]
            kind: RouteFutureKind,
        }
    }

    pin_project! {
        #[project = RouteFutureKindProj]
        enum RouteFutureKind {
            Future {
                #[pin]
                future: Oneshot<
                    BoxCloneService<Request, Response, super::Error>,
                    Request,
                >,
            },
            Response {
                response: Option<Response>,
            }
        }
    }

    impl RouteFuture {
        pub(crate) fn from_future(
            future: Oneshot<BoxCloneService<Request, Response, super::Error>, Request>,
        ) -> Self {
            Self {
                kind: RouteFutureKind::Future { future },
            }
        }
    }

    impl Future for RouteFuture {
        type Output = Result<Response, super::Error>;

        #[inline]
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let this = self.project();

            let res = match this.kind.project() {
                RouteFutureKindProj::Future { future } => match future.poll(cx) {
                    Poll::Ready(Ok(res)) => res,
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                    Poll::Pending => return Poll::Pending,
                },
                RouteFutureKindProj::Response { response } => {
                    response.take().expect("future polled after completion")
                }
            };

            Poll::Ready(Ok(res))
        }
    }
}
