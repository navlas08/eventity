//! Isolated resources for explicitly requested integration tests.
use crate::{EventityPg, RabbitMQ};
use sqlx::{PgPool, postgres::PgPoolOptions};

pub(crate) struct Resources {
    pub schema: String,
    pub pool: PgPool,
    pub store: EventityPg,
    pub broker: RabbitMQ,
    pub channel: lapin::Channel,
    admin: PgPool,
    connection: lapin::Connection,
}
impl Resources {
    pub async fn new() -> Self {
        let db = std::env::var("DATABASE_URL").expect("set DATABASE_URL for integration tests");
        let amqp = std::env::var("AMQP_URL").expect("set AMQP_URL for integration tests");
        let schema = format!("eventity_test_{}", uuid::Uuid::now_v7().simple());
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&db)
            .await
            .unwrap();
        // Identifier contains only a fixed prefix and a generated hexadecimal UUID.
        sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
            .execute(&admin)
            .await
            .unwrap();
        let path = schema.clone();
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .after_connect(move |conn, _| {
                let path = path.clone();
                Box::pin(async move {
                    sqlx::query("SELECT set_config('search_path', $1, false)")
                        .bind(path)
                        .execute(conn)
                        .await?;
                    Ok(())
                })
            })
            .connect(&db)
            .await
            .unwrap();
        let store = EventityPg::new(pool.clone());
        store.migrate().await.unwrap();
        let broker = RabbitMQ::new(&amqp).await.unwrap();
        let connection = lapin::Connection::connect(&amqp, lapin::ConnectionProperties::default())
            .await
            .unwrap();
        let channel = connection.create_channel().await.unwrap();
        channel
            .exchange_declare(
                schema.clone().into(),
                lapin::ExchangeKind::Fanout,
                Default::default(),
                Default::default(),
            )
            .await
            .unwrap();
        channel
            .queue_declare(
                schema.clone().into(),
                lapin::options::QueueDeclareOptions::durable(),
                Default::default(),
            )
            .await
            .unwrap();
        channel
            .queue_bind(
                schema.clone().into(),
                schema.clone().into(),
                "".into(),
                Default::default(),
                Default::default(),
            )
            .await
            .unwrap();
        Self {
            schema,
            pool,
            store,
            broker,
            channel,
            admin,
            connection,
        }
    }
    pub async fn cleanup(self) {
        self.channel
            .queue_delete(self.schema.clone().into(), Default::default())
            .await
            .unwrap();
        self.channel
            .exchange_delete(self.schema.clone().into(), Default::default())
            .await
            .unwrap();
        self.connection
            .close(200, "test complete".into())
            .await
            .unwrap();
        self.pool.close().await;
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "DROP SCHEMA {} CASCADE",
            self.schema
        )))
        .execute(&self.admin)
        .await
        .unwrap();
    }
}
