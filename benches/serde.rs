use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use pulsar::{DeserializeMessage, Payload, SerializeMessage};
use pulsarrow::{RandomData, RawRandomData};

pub fn ipc(c: &mut Criterion) {
    c.benchmark_group("ipc")
        .throughput(Throughput::Bytes(100 * std::mem::size_of::<u64>() as u64))
        .bench_function("serialize", |b| {
            let input = RandomData::default();
            b.iter(|| SerializeMessage::serialize_message(black_box(input.clone())))
        })
        .bench_function("deserialize", |b| {
            let payload = Payload {
                data: SerializeMessage::serialize_message(RandomData::default())
                    .unwrap()
                    .payload,
                metadata: Default::default(),
            };
            b.iter(|| {
                RandomData::deserialize_message(black_box(&payload)).unwrap();
            })
        });
}

pub fn raw(c: &mut Criterion) {
    c.benchmark_group("raw")
        .throughput(Throughput::Bytes(100 * std::mem::size_of::<u64>() as u64))
        .bench_function("serialize", |b| {
            let input = RawRandomData::default();
            b.iter(|| SerializeMessage::serialize_message(black_box(input.clone())));
        })
        .bench_function("deserialize", |b| {
            let payload = Payload {
                data: SerializeMessage::serialize_message(RawRandomData::default())
                    .unwrap()
                    .payload,
                metadata: Default::default(),
            };
            b.iter(|| {
                RawRandomData::deserialize_message(black_box(&payload));
            })
        });
}

criterion_group!(serde, ipc, raw);
criterion_main!(serde);
