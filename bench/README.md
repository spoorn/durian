# Benchmarks

cargo bench --bench benchmark -- --verbose

```
sync_example_single_client_server
                        time:   [123.28 µs 123.61 µs 123.97 µs]
                        change: [-1.5237% -1.0963% -0.6643%] (p = 0.00 < 0.05)
                        Change within noise threshold.
Found 48 outliers among 1000 measurements (4.80%)
  20 (2.00%) high mild
  28 (2.80%) high severe

sync_example_multiclient_server 4
                        time:   [200.75 µs 202.19 µs 203.69 µs]
                        change: [+5.4631% +6.2732% +7.2231%] (p = 0.00 < 0.05)
                        Performance has regressed.
Found 58 outliers among 1000 measurements (5.80%)
  41 (4.10%) high mild
  17 (1.70%) high severe
  
sync_example_multiclient_server 100
                        time:   [3.8047 ms 3.8123 ms 3.8201 ms]
                        change: [-8.0208% -7.4117% -6.8040%] (p = 0.00 < 0.05)
                        Performance has improved.
Found 21 outliers among 1000 measurements (2.10%)
  1 (0.10%) low mild
  19 (1.90%) high mild
  1 (0.10%) high severe
```