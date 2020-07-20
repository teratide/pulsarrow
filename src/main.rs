use arrow::{
    array::{Array, UInt64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::{RecordBatch, RecordBatchReader},
    util::pretty::print_batches,
};
use futures::TryStreamExt;
use pulsar::{
    message::proto::command_subscribe::SubType, producer, Consumer, DeserializeMessage,
    Error as PulsarError, Executor, Pulsar, SerializeMessage, TokioExecutor,
};
use std::sync::Arc;

pub struct RandomData(RecordBatch);

impl Default for RandomData {
    fn default() -> Self {
        RandomData(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "rand",
                    DataType::UInt64,
                    false,
                )])),
                vec![Arc::new(UInt64Array::from(
                    (0..2).map(|_| rand::random::<u64>()).collect::<Vec<u64>>(),
                )) as Arc<dyn Array>],
            )
            .expect("recordbatch failed"),
        )
    }
}

impl SerializeMessage for RandomData {
    fn serialize_message(input: Self) -> Result<producer::Message, PulsarError> {
        let mut payload = Vec::new();
        {
            let mut writer =
                arrow::ipc::writer::StreamWriter::try_new(&mut payload, &input.0.schema())
                    .expect("writer failed");
            writer.write(&input.0).expect("failed to write batch");
            writer.finish().expect("writer failed");
        }
        Ok(producer::Message {
            payload,
            ..Default::default()
        })
    }
}

impl DeserializeMessage for RandomData {
    type Output = Result<Option<RecordBatch>, arrow::error::ArrowError>;
    fn deserialize_message(payload: &pulsar::Payload) -> Self::Output {
        arrow::ipc::reader::StreamReader::try_new(&payload.data[..])
            .expect("reader failed")
            .next_batch()
    }
}

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
