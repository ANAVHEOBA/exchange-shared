use axum::{
    body::Body,
    http::{Request, StatusCode},
    response::{IntoResponse, Response},
};
use governor::{
    clock::DefaultClock,
    state::{InMemoryState, NotKeyed},
    Quota, RateLimiter,
};
use std::{future::Future, num::NonZeroU32, pin::Pin, sync::Arc};
use tower::{Layer, Service};

pub type GlobalRateLimiter = Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>;

pub fn create_rate_limiter(burst: u32) -> GlobalRateLimiter {
    // 60 tokens per minute (1 per second sustained), with burst capacity
    // Allows `burst` requests initially, then 1 per second sustained
    let quota = Quota::per_minute(NonZeroU32::new(60).unwrap())
        .allow_burst(NonZeroU32::new(burst).unwrap());
    Arc::new(RateLimiter::direct(quota))
}

#[derive(Clone)]
pub struct RateLimitLayer {
    limiter: GlobalRateLimiter,
}

impl RateLimitLayer {
    pub fn new(limiter: GlobalRateLimiter) -> Self {
        Self { limiter }
    }
}

impl<S> Layer<S> for RateLimitLayer {
    type Service = RateLimitService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RateLimitService {
            inner,
            limiter: self.limiter.clone(),
        }
    }
}

#[derive(Clone)]
pub struct RateLimitService<S> {
    inner: S,
    limiter: GlobalRateLimiter,
}

impl<S> Service<Request<Body>> for RateLimitService<S>
where
    S: Service<Request<Body>, Response = Response> + Clone + Send + 'static,
    S::Future: Send,
{
    type Response = Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let limiter = self.limiter.clone();
        let mut inner = self.inner.clone();

        Box::pin(async move {
            if limiter.check().is_err() {
                return Ok(StatusCode::TOO_MANY_REQUESTS.into_response());
            }
            inner.call(request).await
        })
    }
}
