use arrow::{
    array::{Array, UInt64Array, StructArray, ArrayEqual},
    datatypes::{DataType, Field, Schema},
    record_batch::{RecordBatch, RecordBatchReader}, buffer::Buffer,
};
use pulsar::{producer, DeserializeMessage, Error as PulsarError, SerializeMessage};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct RandomData(RecordBatch);

impl PartialEq for RandomData {
    fn eq(&self, other: &Self) -> bool {
        self.0.schema().eq(&other.0.schema()) && {
            let a: StructArray = self.0.clone().into();
            let b: StructArray = other.0.clone().into();
            a.equals(&b)
        }
    }   
}

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
                    (0..100)
                        .map(|_| rand::random::<u64>())
                        .collect::<Vec<u64>>(),
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

#[derive(Clone, Default, Debug, PartialEq)]
pub struct RawRandomData(RandomData);

impl SerializeMessage for RawRandomData {
    fn serialize_message(input: Self) -> Result<producer::Message, PulsarError> {
        let payload = unsafe { input.0.0.column(0).data_ref().buffers()[0].typed_data::<u8>() }.to_vec();
        Ok(producer::Message {
            payload,
            ..Default::default()
        })
    }
}

impl DeserializeMessage for RawRandomData {
    type Output = Self;
    fn deserialize_message(payload: &pulsar::Payload) -> Self::Output {
        let buffer = Buffer::from(&payload.data);
        let array = Arc::new(UInt64Array::new(100, buffer, 0, 0));
        let schema = Arc::new(Schema::new(vec![Field::new(
                    "rand",
                    DataType::UInt64,
                    false,
                )]));
        RawRandomData(RandomData(RecordBatch::try_new(schema, vec![array as Arc<dyn Array>]).unwrap()))
    }
    
}

#[cfg(test)]
mod tests {
    use super::*;
    use pulsar::Payload;

    #[test]
    fn validate_raw() {
        let input = RawRandomData::default();
        let data = SerializeMessage::serialize_message(input.clone()).unwrap().payload;
        let output = RawRandomData::deserialize_message(&Payload { data, metadata: Default::default() });
        assert_eq!(input, output);  
    }
}