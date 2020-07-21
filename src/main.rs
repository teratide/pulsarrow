use arrow::util::pretty::print_batches;
use futures::TryStreamExt;
use pulsar::{
    message::proto::command_subscribe::SubType, Consumer, Error as PulsarError, Executor, Pulsar,
    TokioExecutor,
};
use pulsarrow::RandomData;

async fn prod<E: Executor>(pulsar: &Pulsar<E>, topic: &str) -> Result<(), PulsarError> {
    let mut producer = pulsar
        .producer()
        .with_topic(topic)
        .with_name("arrow-producer")
        .build()
        .await?;

    loop {
        producer.send(RandomData::default()).await?;
        tokio::time::delay_for(std::time::Duration::from_millis(200)).await;
    }
}

#[tokio::main]
async fn main() -> Result<(), PulsarError> {
    let url = std::env::args()
        .skip(1)
        .next()
        .expect("missing broker service url");

    let topic = "non-persistent://public/default/test";
    let pulsar = Pulsar::builder(url, TokioExecutor).build().await?;

    let mut consumer: Consumer<RandomData, _> = pulsar
        .consumer()
        .with_topic(topic)
        .with_consumer_name("consumer")
        .with_subscription_type(SubType::Exclusive)
        .with_subscription("sub")
        .build()
        .await?;

    let p = tokio::spawn(async move { prod(&pulsar, topic).await });

    while let Some(msg) = consumer.try_next().await? {
        consumer.ack(&msg).await?;
        let _ = match msg.deserialize() {
            Ok(Some(data)) => print_batches(&[data]).expect("print failed"),
            Ok(None) => println!("empty response"),
            Err(e) => {
                eprintln!("could not deserialize message: {:?}", e);
                break;
            }
        };
    }

    p.await.expect("producer error")?;

    Ok(())
}
