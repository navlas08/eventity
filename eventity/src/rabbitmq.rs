use crate::EventityError;
use lapin::options::{
    BasicConsumeOptions, BasicPublishOptions, BasicQosOptions, ConfirmSelectOptions,
    ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions,
};
use lapin::types::{AMQPValue, FieldTable, ShortString};
use lapin::{
    BasicProperties, Channel, Connection, ConnectionProperties, Consumer, ExchangeKind, Queue,
};
#[cfg(test)]
use serde_json::Value;
use sqlx::types::uuid;
use std::{
    collections::HashSet,
    num::NonZeroU16,
    sync::atomic::{AtomicUsize, Ordering},
};

macro_rules! recover_channel_call {
    ($channel:expr, $future:expr) => {{
        let result: lapin::Result<_> = match $future.await {
            Ok(value) => Ok(value),
            Err(error) => match $channel.wait_for_recovery(error).await {
                Ok(()) => $future.await,
                Err(error) => Err(error),
            },
        };
        result
    }};
}

/// Lapin-backed RabbitMQ connection used by a message bus.
///
/// Connections enable Lapin's automatic recovery. Exchanges, queues, bindings,
/// and consumers are declared by registered event subscriptions and replayed
/// by Lapin after a recoverable connection failure. Publishing uses a separate
/// connection so broker flow control cannot block consumer acknowledgements.
/// Create one instance and share it with the bus builder.
pub struct RabbitMQ {
    connection: Connection,
    publishers: Box<[PublishingConnection]>,
    next_publisher: AtomicUsize,
}

struct PublishingConnection {
    connection: Connection,
    publisher: tokio::sync::Mutex<Option<Publisher>>,
}

struct Publisher {
    channel: Channel,
    exchanges: HashSet<ShortString>,
}

impl RabbitMQ {
    /// Opens consumer and publisher connections with Lapin automatic recovery.
    ///
    /// Each initial connection attempt times out after 15 seconds. A later
    /// recoverable disconnect is handled by Lapin's Reconnect and topology
    /// recovery mechanism.
    pub async fn new(connection_string: impl AsRef<str>) -> Result<Self, EventityError> {
        Self::new_with_publisher_connections(connection_string, NonZeroU16::MIN).await
    }

    /// Opens a bounded pool of publishing connections alongside the consumer
    /// connection. Every connection reuses a confirm-enabled channel. Increase
    /// this only when profiling shows publishing is the bottleneck; each entry
    /// adds a broker TCP connection and its own recovery state.
    pub async fn new_with_publisher_connections(
        connection_string: impl AsRef<str>,
        count: NonZeroU16,
    ) -> Result<Self, EventityError> {
        let uri = connection_string.as_ref();
        let connection = Self::connect(uri).await?;
        let mut publishers = Vec::with_capacity(usize::from(count.get()));
        for _ in 0..count.get() {
            publishers.push(PublishingConnection {
                connection: Self::connect(uri).await?,
                publisher: tokio::sync::Mutex::new(None),
            });
        }
        Ok(Self {
            connection,
            publishers: publishers.into_boxed_slice(),
            next_publisher: AtomicUsize::new(0),
        })
    }

    async fn connect(uri: &str) -> Result<Connection, EventityError> {
        Ok(tokio::time::timeout(
            std::time::Duration::from_secs(15),
            Connection::connect(uri, ConnectionProperties::default().enable_auto_recover()),
        )
        .await
        .map_err(|_| EventityError::ConnectionTimeout)??)
    }

    async fn create_channel(&self) -> Result<Channel, EventityError> {
        Ok(self.connection.create_channel().await?)
    }
    pub(crate) async fn declare_exchange(&self, name: &ShortString) -> Result<(), EventityError> {
        let channel = self.create_channel().await?;
        recover_channel_call!(
            channel,
            channel.exchange_declare(
                name.clone(),
                ExchangeKind::Fanout,
                ExchangeDeclareOptions::default(),
                FieldTable::default(),
            )
        )?;
        Ok(())
    }
    pub(crate) async fn declare_queue(
        &self,
        name: &ShortString,
        dead_letter_exchange: &ShortString,
    ) -> Result<Queue, EventityError> {
        let mut arguments = FieldTable::default();
        arguments.insert(
            "x-dead-letter-exchange".into(),
            AMQPValue::LongString(dead_letter_exchange.to_string().into()),
        );
        arguments.insert(
            "x-dead-letter-routing-key".into(),
            AMQPValue::LongString(name.to_string().into()),
        );
        let channel = self.create_channel().await?;
        Ok(recover_channel_call!(
            channel,
            channel.queue_declare(
                name.clone(),
                QueueDeclareOptions::durable(),
                arguments.clone(),
            )
        )?)
    }
    pub(crate) async fn declare_dead_letter_topology(
        &self,
        exchange: ShortString,
        queue_name: ShortString,
    ) -> Result<(), EventityError> {
        let channel = self.create_channel().await?;
        recover_channel_call!(
            channel,
            channel.exchange_declare(
                exchange.clone(),
                ExchangeKind::Direct,
                ExchangeDeclareOptions::default(),
                FieldTable::default(),
            )
        )?;
        let dead_queue: ShortString = format!("{queue_name}.dead").into();
        let queue = recover_channel_call!(
            channel,
            channel.queue_declare(
                dead_queue.clone(),
                QueueDeclareOptions::durable(),
                FieldTable::default(),
            )
        )?;
        recover_channel_call!(
            channel,
            channel.queue_bind(
                queue.name().clone(),
                exchange.clone(),
                queue_name.clone(),
                QueueBindOptions::default(),
                FieldTable::default(),
            )
        )?;
        Ok(())
    }
    pub(crate) async fn declare_consumer(
        &self,
        queue: ShortString,
        prefetch: u16,
    ) -> Result<(Consumer, Channel), EventityError> {
        let channel = self.create_channel().await?;
        recover_channel_call!(
            channel,
            channel.basic_qos(prefetch, BasicQosOptions::default())
        )?;
        let consumer = recover_channel_call!(
            channel,
            channel.basic_consume(
                queue.clone(),
                "eventity_consumer".into(),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
        )?;
        Ok((consumer, channel))
    }
    pub(crate) async fn bind_queue(
        &self,
        queue: Queue,
        exchange: ShortString,
    ) -> Result<(), EventityError> {
        let channel = self.create_channel().await?;
        recover_channel_call!(
            channel,
            channel.queue_bind(
                queue.name().clone(),
                exchange.clone(),
                "".into(),
                QueueBindOptions::default(),
                FieldTable::default(),
            )
        )?;
        Ok(())
    }
    #[cfg(test)]
    pub(crate) async fn publish(&self, message: Message) -> Result<(), EventityError> {
        self.publish_raw(
            message.event_id,
            &message.exchange_name,
            &serde_json::to_vec(&message.payload)?,
        )
        .await
    }

    pub(crate) async fn publish_raw(
        &self,
        event_id: uuid::Uuid,
        exchange: &ShortString,
        payload: &[u8],
    ) -> Result<(), EventityError> {
        tokio::time::timeout(std::time::Duration::from_secs(15), async {
            let channel = self.publisher_channel(exchange).await?;
            let confirmation = recover_channel_call!(
                channel,
                channel.basic_publish(
                    exchange.clone(),
                    "".into(),
                    BasicPublishOptions {
                        mandatory: true,
                        ..Default::default()
                    },
                    payload,
                    BasicProperties::default()
                        .with_message_id(event_id.to_string().into())
                        .with_delivery_mode(2),
                )
            )?;
            let confirmation = match confirmation.await {
                Ok(confirmation) => confirmation,
                Err(error) => {
                    channel.wait_for_recovery(error).await?;
                    return Err(EventityError::PublishNotConfirmed);
                }
            };
            match confirmation {
                lapin::Confirmation::Ack(None) => Ok(()),
                lapin::Confirmation::Ack(Some(_)) => Err(EventityError::UnroutableMessage),
                lapin::Confirmation::Nack(_) => Err(EventityError::PublishRejected),
                lapin::Confirmation::NotRequested => Err(EventityError::PublishNotConfirmed),
            }
        })
        .await
        .map_err(|_| EventityError::PublishTimeout)?
    }

    // Serialize initialization only. Publishing and waiting for confirms must
    // happen outside this lock so that confirms can be pipelined.
    pub(crate) async fn publisher_channel(
        &self,
        exchange: &ShortString,
    ) -> Result<Channel, EventityError> {
        let index = self.next_publisher.fetch_add(1, Ordering::Relaxed) % self.publishers.len();
        let connection = &self.publishers[index];
        let mut publisher = connection.publisher.lock().await;
        if publisher
            .as_ref()
            .is_some_and(|p| !p.channel.status().connected() && !p.channel.status().reconnecting())
        {
            *publisher = None;
        }
        if publisher.is_none() {
            let channel = connection.connection.create_channel().await?;
            recover_channel_call!(
                channel,
                channel.confirm_select(ConfirmSelectOptions::default())
            )?;
            *publisher = Some(Publisher {
                channel,
                exchanges: HashSet::new(),
            });
        }
        let publisher = publisher.as_mut().expect("publisher initialized above");
        if !publisher.exchanges.contains(exchange) {
            recover_channel_call!(
                publisher.channel,
                publisher.channel.exchange_declare(
                    exchange.clone(),
                    ExchangeKind::Fanout,
                    ExchangeDeclareOptions::default(),
                    FieldTable::default()
                )
            )?;
            publisher.exchanges.insert(exchange.clone());
        }
        Ok(publisher.channel.clone())
    }
}

#[cfg(test)]
pub(crate) struct Message {
    pub(crate) event_id: uuid::Uuid,
    pub(crate) exchange_name: ShortString,
    pub(crate) payload: Value,
}
