# Eventity

Rust-like MediatR or Wolverine.NET PostgreSQL, RabbitMQ and transactional outbox/inbox pattern.

```toml
[dependencies]
eventity = "0.1"
```

```rust,ignore
let bus = MessageBus::builder()
    .broker(Arc::new(RabbitMQ::new(amqp_uri).await?))
    .store(Arc::new(EventityPg::new(pool)))
    .request_handler(CreateOrderHandler)
    .event_handler(OrderCreatedHandler)
    .build()
    .await?;

let order_id = bus.invoke(CreateOrder { /* ... */ }).await?;
bus.shutdown().await?;
```
