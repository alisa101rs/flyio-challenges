use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use flyio_rs::{
    request::{Extension, Payload},
    response::IntoResponse,
    routing::Router,
    serve, setup_network,
    trace::setup_tracing,
};
use serde::{Deserialize, Serialize};
use tracing::{field::debug, instrument, Span};

#[tokio::main]
async fn main() -> eyre::Result<()> {
    setup_tracing()?;

    let (network, rpc, messages) = setup_network().await?;
    let my_id = network.id.strip_prefix("n").unwrap().parse().unwrap();
    let generator = SnowflakeGenerator::create(my_id);

    let router = Router::new()
        .route("generate", generate)
        .layer(Extension(generator));

    serve(router, rpc, messages).await?;

    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GenerateResponse {
    id: u64,
}

#[derive(Debug, Clone)]
struct SnowflakeGenerator {
    id: u16,
}

impl SnowflakeGenerator {
    fn create(id: u16) -> Self {
        assert!(id < 0b1_1111_1111);
        Self { id }
    }

    fn next(&self) -> Snowflake {
        static COUNTER: AtomicU64 = AtomicU64::new(0);

        let mut current_value = COUNTER.load(Ordering::SeqCst);
        let id = loop {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            let ts = current_value >> 22;
            let c = (current_value & Snowflake::MAX_COUNTER as u64) as u16;
            if now < ts {
                continue;
            }
            if c == Snowflake::MAX_COUNTER {
                panic!("Rate exceeded")
            }

            let new_value = if ts == now {
                current_value + 1
            } else {
                let mut new_value = 0;
                new_value |= now << 22;
                new_value |= (self.id as u64) << 13;
                new_value
            };

            match COUNTER.compare_exchange(
                current_value,
                new_value,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => break new_value,
                Err(v) => {
                    current_value = v;
                    tracing::warn!("Contention");
                }
            }
        };

        Snowflake {
            timestamp: id >> 22,
            node: self.id,
            sequence_number: (id & Snowflake::MAX_COUNTER as u64) as u16,
        }
    }
}

///
///  0                    42                  51                 64
///  +--------------------+-------------------+-------------------+
///  | epoch milliseconds |        node       |    counter        |
///  +--------------------+-------------------+-------------------+
///  |   ~69 years        |  2^9 = 512 nodes  | 2^13 = 8192 id/ms |
///  +--------------------+-------------------+-------------------+
///
#[derive(Debug, Clone, Copy)]
struct Snowflake {
    timestamp: u64,
    node: u16,
    sequence_number: u16,
}

impl Snowflake {
    pub const MAX_COUNTER: u16 = 0b1_1111_1111_1111;
    fn encode(&self) -> u64 {
        let mut value = 0;
        value |= self.timestamp << 22;
        value |= (self.node as u64) << 13;
        value |= self.sequence_number as u64;
        value
    }
}

#[instrument(skip(generator), fields(id), ret)]
async fn generate(Extension(generator): Extension<SnowflakeGenerator>) -> impl IntoResponse {
    let id = generator.next();
    Span::current().record("id", debug(id));
    Payload(GenerateResponse { id: id.encode() })
}
