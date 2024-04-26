use std::{
    collections::HashMap,
    sync::Arc,
    task::{Context, Poll},
};

use eyre::bail;
use smol_str::SmolStr;
use tower::{Layer, Service};

mod handler;
mod route;

pub use self::{
    handler::{Handler, HandlerExt},
    route::{route_future::RouteFuture, Route},
};
pub use crate::{request::Request, response::Response};

#[derive(Clone)]
pub struct Router {
    inner: Arc<RouterInner>,
}

impl Router {
    pub fn new() -> Router {
        Router {
            inner: Arc::new(Default::default()),
        }
    }

    pub fn route<H, T>(self, path: &str, handler: H) -> Self
    where
        H: Handler<T>,
        T: 'static,
    {
        self.tap_inner_mut(|this| {
            this.router.route(path, Route::new(handler.into_service()));
        })
    }

    pub fn layer<L>(self, layer: L) -> Router
    where
        L: Layer<Route> + Clone + Send + 'static,
        L::Service:
            Service<Request, Response = Response, Error = eyre::Report> + Clone + Send + 'static,
        <L::Service as Service<Request>>::Future: Send + 'static,
    {
        self.map_inner(|this| RouterInner {
            router: this.router.layer(layer.clone()),
            fallback: this.fallback.layer(layer.clone()),
        })
    }

    fn map_inner<F>(self, f: F) -> Router
    where
        F: FnOnce(RouterInner) -> RouterInner,
    {
        Router {
            inner: Arc::new(f(self.into_inner())),
        }
    }

    fn into_inner(self) -> RouterInner {
        Arc::into_inner(self.inner).expect("Router not to be shared yet")
    }

    fn tap_inner_mut<F>(self, f: F) -> Self
    where
        F: FnOnce(&mut RouterInner),
    {
        let mut inner = self.into_inner();
        f(&mut inner);
        Router {
            inner: Arc::new(inner),
        }
    }
}

struct RouterInner {
    router: CommandRouter,
    fallback: Route,
}

impl Default for RouterInner {
    fn default() -> Self {
        async fn default_handler() -> eyre::Result<()> {
            bail!("Unknown command")
        }

        Self {
            router: Default::default(),
            fallback: Route::new(default_handler.into_service()),
        }
    }
}

#[derive(Default)]
struct CommandRouter {
    routes: HashMap<SmolStr, Route>,
}

impl CommandRouter {
    fn call(&self, req: Request) -> Result<RouteFuture, Request> {
        match self.routes.get(&req.method) {
            Some(route) => Ok(RouteFuture::from_future(route.clone().oneshot_inner(req))),
            None => Err(req),
        }
    }

    fn route(&mut self, path: &str, route: Route) {
        self.routes.insert(path.into(), route);
    }

    fn layer<L>(self, layer: L) -> CommandRouter
    where
        L: Layer<Route> + Clone + Send + 'static,
        L::Service:
            Service<Request, Response = Response, Error = eyre::Report> + Clone + Send + 'static,
        <L::Service as Service<Request>>::Future: Send + 'static,
    {
        let routes = self
            .routes
            .into_iter()
            .map(|(id, route)| {
                let route = route.layer(layer.clone());
                (id, route)
            })
            .collect();

        CommandRouter { routes }
    }
}

impl Service<Request> for Router {
    type Error = eyre::Report;
    type Future = RouteFuture;
    type Response = Response;

    #[inline]
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    #[inline]
    fn call(&mut self, req: Request) -> Self::Future {
        let req = match self.inner.router.call(req) {
            Ok(future) => return future,
            Err(req) => req,
        };

        RouteFuture::from_future(self.inner.fallback.clone().oneshot_inner(req))
    }
}
