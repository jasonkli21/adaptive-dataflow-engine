use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;
fn smoke(c: &mut Criterion) {
    c.bench_function("blake3-4k", |b| {
        b.iter(|| blake3::hash(black_box(&[42_u8; 4096])));
    });
}
criterion_group!(benches, smoke);
criterion_main!(benches);
