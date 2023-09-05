use criterion::{criterion_group, criterion_main, Criterion};

use bench::{setup, sync_example_multiclient_server, sync_example_single_client_server};

fn criterion_benchmark(c: &mut Criterion) {
    {
        let (mut client_managers, mut server_manager) = setup(5000, 1, 5001);
        c.bench_function("sync_example_single_client_server", |b| {
            b.iter(|| {
                sync_example_single_client_server(
                    server_manager.get_source().0.clone().as_str(),
                    &mut client_managers[0],
                    &mut server_manager,
                )
            })
        });
    }

    {
        let (mut client_managers, mut server_manager) = setup(5002, 4, 5003);
        c.bench_function("sync_example_multiclient_server 4", |b| {
            b.iter(|| {
                sync_example_multiclient_server(
                    server_manager.get_source().0.clone().as_str(),
                    &mut client_managers,
                    &mut server_manager,
                )
            })
        });
    }

    {
        let (mut client_managers, mut server_manager) = setup(5010, 100, 25000);
        c.bench_function("sync_example_multiclient_server 100", |b| {
            b.iter(|| {
                sync_example_multiclient_server(
                    server_manager.get_source().0.clone().as_str(),
                    &mut client_managers,
                    &mut server_manager,
                )
            })
        });
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default().measurement_time(std::time::Duration::from_secs(30)).sample_size(50000);
    targets = criterion_benchmark
}
criterion_main!(benches);
