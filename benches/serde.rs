use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use pulsar::{DeserializeMessage, Payload, SerializeMessage};
use pulsarrow::{RandomData, RawRandomData};

pub fn ipc(c: &mut Criterion) {
    let mut group = c.benchmark_group("ipc");
    for size in [1, 100, 1_000, 10_000, 100_000, 1_000_000].iter() {
        group
            .throughput(Throughput::Bytes(size * std::mem::size_of::<u64>() as u64))
            .bench_with_input(BenchmarkId::new("serialize", size), size, |b, &size| {
                let input = RandomData::new(size as usize);
                b.iter(|| SerializeMessage::serialize_message(black_box(input.clone())))
            })
            .bench_with_input(BenchmarkId::new("deserialize", size), size, |b, &size| {
                let payload = Payload {
                    data: SerializeMessage::serialize_message(RandomData::new(size as usize))
                        .unwrap()
                        .payload,
                    metadata: Default::default(),
                };
                b.iter(|| {
                    RandomData::deserialize_message(black_box(&payload)).unwrap();
                })
            });
    }
    group.finish();
}

pub fn raw(c: &mut Criterion) {
    let mut group = c.benchmark_group("raw");
    for size in [1, 100, 1_000, 10_000, 100_000, 1_000_000].iter() {
        group
            .throughput(Throughput::Bytes(size * std::mem::size_of::<u64>() as u64))
            .bench_with_input(BenchmarkId::new("serialize", size), size, |b, &size| {
                let input = RawRandomData::new(size as usize);
                b.iter(|| SerializeMessage::serialize_message(black_box(input.clone())))
            })
            .bench_with_input(BenchmarkId::new("deserialize", size), size, |b, &size| {
                let payload = Payload {
                    data: SerializeMessage::serialize_message(RawRandomData::new(size as usize))
                        .unwrap()
                        .payload,
                    metadata: Default::default(),
                };
                b.iter(|| {
                    RawRandomData::deserialize_message(black_box(&payload));
                })
            });
    }
    group.finish();
}

criterion_group!(serde, ipc, raw);
criterion_main!(serde);
