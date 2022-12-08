use criterion::{Criterion, criterion_group, criterion_main};

use bench::sync_example;

fn fibonacci(n: u64) -> u64 {
    let mut a = 0;
    let mut b = 1;

    match n {
        0 => b,
        _ => {
            for _ in 0..n {
                let c = a + b;
                a = b;
                b = c;
            }
            b
        }
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("sync_example", |b| b.iter(|| sync_example()));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);