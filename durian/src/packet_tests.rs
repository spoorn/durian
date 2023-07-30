use hashbrown::HashMap;
use std::time::{Duration, SystemTime};

use durian_macros::bincode_packet;
use tokio::sync::mpsc;
use tokio::time::sleep;

use durian::packet::PacketManager;

use crate as durian;
use crate::{register_receive, register_send, ClientConfig, ErrorType, ServerConfig};

#[bincode_packet]
#[derive(Debug, PartialEq, Eq, Hash)]
struct Test {
    id: i32,
}

#[bincode_packet]
#[derive(Debug, PartialEq, Eq, Hash)]
struct Other {
    name: String,
    id: i32,
}

// TODO: Test sync versions
#[tokio::test]
async fn receive_packet_e2e_async() {
    let mut manager = PacketManager::new_for_async();

    let (tx, mut rx) = mpsc::channel(100);
    let server_addr = "127.0.0.1:5000";
    let client_addr = "127.0.0.1:5001";

    // Server
    let server = tokio::spawn(async move {
        let mut m = PacketManager::new_for_async();
        assert!(m.register_send_packet::<Test>().is_ok());
        assert!(m.register_send_packet::<Other>().is_ok());
        assert!(m.register_receive_packet::<Test>(TestPacketBuilder).is_ok());
        assert!(m.register_receive_packet::<Other>(OtherPacketBuilder).is_ok());
        let server_config = ServerConfig::new_listening(server_addr, 1, 2, 2);
        assert!(m.async_init_server(server_config).await.is_ok());
        let client_connections = m.async_get_client_connections().await;
        assert_eq!(client_connections.len(), 1);
        assert_eq!(client_connections[0], (client_addr.to_string(), 0u32));

        let mut packets_to_send = 0;
        let mut test_packets = HashMap::new();
        let mut other_packets = HashMap::new();
        let start_time = SystemTime::now();
        while start_time.elapsed().unwrap() < Duration::from_secs(3) {
            if packets_to_send < 50 {
                assert!(m.async_send::<Test>(Test { id: 5 }).await.is_ok());
                assert!(m.async_send::<Test>(Test { id: 8 }).await.is_ok());
                assert!(m
                    .async_send::<Other>(Other { name: "spoorn".to_string(), id: 4 })
                    .await
                    .is_ok());
                assert!(m
                    .async_send::<Other>(Other { name: "kiko".to_string(), id: 6 })
                    .await
                    .is_ok());
                packets_to_send += 1;
            }

            let test_res = m.async_received::<Test, TestPacketBuilder>(false).await;
            assert!(test_res.is_ok());
            if let Some(packets) = test_res.unwrap().take() {
                packets.into_iter().for_each(|packet| {
                    *test_packets.entry(packet).or_insert_with(|| 0) += 1;
                });
            }
            let other_res = m.async_received::<Other, OtherPacketBuilder>(false).await;
            assert!(other_res.is_ok());
            if let Some(packets) = other_res.unwrap().take() {
                packets.into_iter().for_each(|packet| {
                    *other_packets.entry(packet).or_insert_with(|| 0) += 1;
                });
            }

            // Early terminate if we already found all expected early
            if packets_to_send >= 50
                && (test_packets.values().cloned().collect::<Vec<i32>>().iter().sum::<i32>()
                    + other_packets.values().cloned().collect::<Vec<i32>>().iter().sum::<i32>())
                    >= 200
            {
                break;
            }
        }

        assert!(matches!(test_packets.get(&Test { id: 6 }), Some(i) if *i == 50));
        assert!(matches!(test_packets.get(&Test { id: 9 }), Some(i) if *i == 50));
        assert!(
            matches!(other_packets.get(&Other { name: "mango".to_string(), id: 1 }), Some(i) if *i == 50)
        );
        assert!(
            matches!(other_packets.get(&Other { name: "luna".to_string(), id: 3 }), Some(i) if *i == 50)
        );

        rx.recv().await;

        // loop {
        //     // Have to use tokio's sleep so it can yield to the tokio executor
        //     // https://stackoverflow.com/questions/70798841/why-does-a-tokio-thread-wait-for-a-blocking-thread-before-continuing?rq=1
        //     //sleep(Duration::from_millis(100)).await;
        // }
    });

    // Client
    assert!(manager.register_receive_packet::<Test>(TestPacketBuilder).is_ok());
    assert!(manager.register_receive_packet::<Other>(OtherPacketBuilder).is_ok());
    assert!(manager.register_send_packet::<Test>().is_ok());
    assert!(manager.register_send_packet::<Other>().is_ok());
    let client_config = ClientConfig::new(client_addr, server_addr, 2, 2);
    let client = manager.async_init_client(client_config).await;
    assert!(manager.async_get_client_connections().await.is_empty());
    println!("{:#?}", client);
    assert!(client.is_ok());

    let mut packets_to_send = 0;
    let mut test_packets = HashMap::new();
    let mut other_packets = HashMap::new();
    let start_time = SystemTime::now();
    while start_time.elapsed().unwrap() < Duration::from_secs(3) {
        // Send packets
        if packets_to_send < 50 {
            assert!(manager.async_send::<Test>(Test { id: 6 }).await.is_ok());
            assert!(manager.async_send::<Test>(Test { id: 9 }).await.is_ok());
            assert!(manager
                .async_send::<Other>(Other { name: "mango".to_string(), id: 1 })
                .await
                .is_ok());
            assert!(manager
                .async_send::<Other>(Other { name: "luna".to_string(), id: 3 })
                .await
                .is_ok());
            packets_to_send += 1;
        }

        let test_res = manager.async_received::<Test, TestPacketBuilder>(false).await;
        assert!(test_res.is_ok());
        if let Some(packets) = test_res.unwrap().take() {
            packets.into_iter().for_each(|packet| {
                *test_packets.entry(packet).or_insert_with(|| 0) += 1;
            });
        };
        let other_res = manager.async_received::<Other, OtherPacketBuilder>(false).await;
        assert!(other_res.is_ok());
        if let Some(packets) = other_res.unwrap().take() {
            packets.into_iter().for_each(|packet| {
                *other_packets.entry(packet).or_insert_with(|| 0) += 1;
            });
        }

        // Early terminate if we already found all expected early
        if packets_to_send >= 50
            && (test_packets.values().cloned().collect::<Vec<i32>>().iter().sum::<i32>()
                + other_packets.values().cloned().collect::<Vec<i32>>().iter().sum::<i32>())
                >= 200
        {
            break;
        }
    }

    assert!(matches!(test_packets.get(&Test { id: 5 }), Some(i) if *i == 50));
    assert!(matches!(test_packets.get(&Test { id: 8 }), Some(i) if *i == 50));
    assert!(
        matches!(other_packets.get(&Other { name: "spoorn".to_string(), id: 4 }), Some(i) if *i == 50)
    );
    assert!(
        matches!(other_packets.get(&Other { name: "kiko".to_string(), id: 6 }), Some(i) if *i == 50)
    );

    tx.send(0).await.unwrap();
    assert!(server.await.is_ok());
}

// Run with
// $env:RUST_LOG="debug"; cargo test receive_packet_e2e_async_broadcast -- --nocapture
#[test_log::test(tokio::test)]
async fn receive_packet_e2e_async_broadcast() {
    let mut manager = PacketManager::new_for_async();

    let (tx, mut rx) = mpsc::channel(100);
    let server_addr = "127.0.0.1:6000";
    let client_addr = "127.0.0.1:6001";
    let client2_addr = "127.0.0.1:6002";

    let num_receive = 100;
    let client2_max_receive = 40;
    let num_send = 100;

    // Server
    let server = tokio::spawn(async move {
        let mut m = PacketManager::new_for_async();
        assert!(m.register_send_packet::<Test>().is_ok());
        assert!(m.register_send_packet::<Other>().is_ok());
        assert!(m.register_receive_packet::<Test>(TestPacketBuilder).is_ok());
        assert!(m.register_receive_packet::<Other>(OtherPacketBuilder).is_ok());
        let server_config = ServerConfig::new_listening(server_addr, 2, 2, 2);
        assert!(m.async_init_server(server_config).await.is_ok());
        let client_connections = m.async_get_client_connections().await;
        assert_eq!(client_connections.len(), 2);
        let client1_is_first = client_connections[0].0 == client_addr
            && client_connections[0].1 == 0u32
            || client_connections[1].0 == client_addr && client_connections[1].1 == 0u32;
        if client1_is_first {
            assert!(client_connections.contains(&(client_addr.to_string(), 0u32)));
            assert!(client_connections.contains(&(client2_addr.to_string(), 1u32)));
        } else {
            assert!(client_connections.contains(&(client_addr.to_string(), 1u32)));
            assert!(client_connections.contains(&(client2_addr.to_string(), 0u32)));
        }

        let mut sent_packets = 0;
        let mut recv_packets = 0;
        let mut client2_recv_packets = 0;
        let mut all_test_packets = Vec::new();
        let mut all_other_packets = Vec::new();
        let mut closed_client2 = false;
        loop {
            //println!("server sent {}, received {}", sent_packets, recv_packets);
            //println!("all_test_packets len {}", all_test_packets.len());
            //println!("all_other_packets len {}", all_other_packets.len());
            if sent_packets < num_send {
                assert!(m.async_broadcast::<Test>(Test { id: 5 }).await.is_ok());
                assert!(m.async_broadcast::<Test>(Test { id: 8 }).await.is_ok());
                assert!(m
                    .async_broadcast::<Other>(Other { name: "spoorn".to_string(), id: 4 })
                    .await
                    .is_ok());
                assert!(m
                    .async_broadcast::<Other>(Other { name: "kiko".to_string(), id: 6 })
                    .await
                    .is_ok());
                sent_packets += 4;
            }

            // When halfway, try disconnecting one of the clients
            if client2_recv_packets >= client2_max_receive && !closed_client2 {
                m.async_close_connection(client2_addr).await.unwrap();
                closed_client2 = true;
            }

            if recv_packets >= num_receive + client2_max_receive && sent_packets >= num_send {
                println!("server done, sent {}, received {}", sent_packets, recv_packets);
                break;
            }

            let test_res = m.async_received_all::<Test, TestPacketBuilder>(true).await;
            println!("{:?}", test_res);
            assert!(test_res.is_ok());
            let mut received_all = test_res.unwrap();
            assert_eq!(received_all.len(), if closed_client2 { 1 } else { 2 });
            let (addr, unwrapped) = received_all.remove(0);
            assert!(m.async_get_client_id(addr.clone()).await.is_some());
            if unwrapped.is_some() {
                let mut packets = unwrapped.unwrap();
                recv_packets += packets.len();
                client2_recv_packets += packets.len() / 2;
                all_test_packets.append(&mut packets);
            }

            if !closed_client2 {
                let (addr2, unwrapped2) = received_all.remove(0);
                assert!((addr == client_addr || addr == client2_addr) && (addr != addr2));
                assert!(m.async_get_client_id(addr2).await.is_some());
                if unwrapped2.is_some() {
                    let mut packets = unwrapped2.unwrap();
                    recv_packets += packets.len();
                    client2_recv_packets += packets.len() / 2;
                    all_test_packets.append(&mut packets);
                }
            }

            let other_res = m.async_received_all::<Other, OtherPacketBuilder>(true).await;
            assert!(other_res.is_ok());
            let mut received_all = other_res.unwrap();
            assert_eq!(received_all.len(), if closed_client2 { 1 } else { 2 });
            let (addr, unwrapped) = received_all.remove(0);
            assert!(m.async_get_client_id(addr.clone()).await.is_some());
            if unwrapped.is_some() {
                let mut packets = unwrapped.unwrap();
                recv_packets += packets.len();
                client2_recv_packets += packets.len() / 2;
                all_other_packets.append(&mut packets);
            }

            if !closed_client2 {
                let (addr2, unwrapped2) = received_all.remove(0);
                assert!((addr == client_addr || addr == client2_addr) && (addr != addr2));
                assert!(m.async_get_client_id(addr2).await.is_some());
                if unwrapped2.is_some() {
                    assert!(unwrapped2.is_some());
                    let mut packets = unwrapped2.unwrap();
                    recv_packets += packets.len();
                    client2_recv_packets += packets.len() / 2;
                    all_other_packets.append(&mut packets);
                }
            }
        }

        assert_eq!(all_test_packets.len(), (num_receive + client2_max_receive) / 2);
        assert_eq!(all_other_packets.len(), (num_receive + client2_max_receive) / 2);
        for (i, packet) in all_test_packets.into_iter().enumerate() {
            if i % 2 == 0 {
                assert_eq!(packet, Test { id: 6 });
            } else {
                assert_eq!(packet, Test { id: 9 });
            }
        }
        for (i, packet) in all_other_packets.into_iter().enumerate() {
            if i % 2 == 0 {
                assert_eq!(packet, Other { name: "mango".to_string(), id: 1 });
            } else {
                assert_eq!(packet, Other { name: "luna".to_string(), id: 3 });
            }
        }

        rx.recv().await;
        // loop {
        //     // Have to use tokio's sleep so it can yield to the tokio executor
        //     // https://stackoverflow.com/questions/70798841/why-does-a-tokio-thread-wait-for-a-blocking-thread-before-continuing?rq=1
        //     //sleep(Duration::from_millis(100)).await;
        // }
        println!("server exit");
    });

    let (client2_tx, mut client2_rx) = mpsc::channel(100);
    let client2 = tokio::spawn(async move {
        let mut manager = PacketManager::new_for_async();
        assert!(manager.register_receive_packet::<Test>(TestPacketBuilder).is_ok());
        assert!(manager.register_receive_packet::<Other>(OtherPacketBuilder).is_ok());
        assert!(manager.register_send_packet::<Test>().is_ok());
        assert!(manager.register_send_packet::<Other>().is_ok());
        let client_config = ClientConfig::new(client2_addr, server_addr, 2, 2);
        assert!(manager.async_init_client(client_config).await.is_ok());
        assert!(manager.async_get_client_connections().await.is_empty());

        let mut sent_packets = 0;
        let mut recv_packets = 0;
        let mut all_test_packets = Vec::new();
        let mut all_other_packets = Vec::new();
        loop {
            println!("client2 sent {}, received {}", sent_packets, recv_packets);
            if sent_packets < num_send {
                assert!(manager.async_broadcast::<Test>(Test { id: 6 }).await.is_ok());
                assert!(manager.async_broadcast::<Test>(Test { id: 9 }).await.is_ok());
                assert!(manager
                    .async_broadcast::<Other>(Other { name: "mango".to_string(), id: 1 })
                    .await
                    .is_ok());
                assert!(manager
                    .async_broadcast::<Other>(Other { name: "luna".to_string(), id: 3 })
                    .await
                    .is_ok());
                sent_packets += 4;
            }

            if recv_packets == client2_max_receive {
                println!("client2 done, sent {}, received {}", sent_packets, recv_packets);
                sleep(Duration::from_secs(4)).await;
                break;
            }

            let test_res = manager.async_received_all::<Test, TestPacketBuilder>(true).await;
            assert!(test_res.is_ok());
            let mut received_all = test_res.unwrap();
            if received_all.is_empty() {
                println!("client2 got empty packets.  Exiting loop");
                break;
            }
            assert!(!received_all.is_empty());
            let (addr, unwrapped) = received_all.remove(0);
            assert_eq!(addr, server_addr);
            assert!(unwrapped.is_some());
            let mut packets = unwrapped.unwrap();
            recv_packets += packets.len();
            all_test_packets.append(&mut packets);
            let other_res = manager.async_received::<Other, OtherPacketBuilder>(true).await;
            assert!(other_res.is_ok());
            let unwrapped = other_res.unwrap();
            assert!(unwrapped.is_some());
            let mut packets = unwrapped.unwrap();
            recv_packets += packets.len();
            all_other_packets.append(&mut packets);
        }

        assert_eq!(all_test_packets.len(), client2_max_receive / 2);
        assert_eq!(all_other_packets.len(), client2_max_receive / 2);
        for (i, packet) in all_test_packets.into_iter().enumerate() {
            if i % 2 == 0 {
                assert_eq!(packet, Test { id: 5 });
            } else {
                assert_eq!(packet, Test { id: 8 });
            }
        }
        for (i, packet) in all_other_packets.into_iter().enumerate() {
            if i % 2 == 0 {
                assert_eq!(packet, Other { name: "spoorn".to_string(), id: 4 });
            } else {
                assert_eq!(packet, Other { name: "kiko".to_string(), id: 6 });
            }
        }

        client2_rx.recv().await;
        println!("client2 exit");
    });

    // Client
    assert!(manager.register_receive_packet::<Test>(TestPacketBuilder).is_ok());
    assert!(manager.register_receive_packet::<Other>(OtherPacketBuilder).is_ok());
    assert!(manager.register_send_packet::<Test>().is_ok());
    assert!(manager.register_send_packet::<Other>().is_ok());
    let client_config = ClientConfig::new(client_addr, server_addr, 2, 2);
    let client = manager.async_init_client(client_config).await;
    assert!(manager.async_get_client_connections().await.is_empty());
    println!("client1: {:#?}", client);

    assert!(client.is_ok());

    let mut sent_packets = 0;
    let mut recv_packets = 0;
    let mut all_test_packets = Vec::new();
    let mut all_other_packets = Vec::new();
    loop {
        println!("client1 sent {}, received {}", sent_packets, recv_packets);
        // Send packets
        if sent_packets < num_send {
            assert!(manager.async_broadcast::<Test>(Test { id: 6 }).await.is_ok());
            assert!(manager.async_broadcast::<Test>(Test { id: 9 }).await.is_ok());
            assert!(manager
                .async_broadcast::<Other>(Other { name: "mango".to_string(), id: 1 })
                .await
                .is_ok());
            assert!(manager
                .async_broadcast::<Other>(Other { name: "luna".to_string(), id: 3 })
                .await
                .is_ok());
            sent_packets += 4;
        }

        if recv_packets == num_receive {
            println!("client1 done, sent {}, received {}", sent_packets, recv_packets);
            break;
        }

        let test_res = manager.async_received_all::<Test, TestPacketBuilder>(true).await;
        assert!(test_res.is_ok());
        let mut received_all = test_res.unwrap();
        assert!(!received_all.is_empty());
        let (addr, unwrapped) = received_all.remove(0);
        assert_eq!(addr, server_addr);
        assert!(unwrapped.is_some());
        let mut packets = unwrapped.unwrap();
        recv_packets += packets.len();
        all_test_packets.append(&mut packets);
        let other_res = manager.async_received::<Other, OtherPacketBuilder>(true).await;
        assert!(other_res.is_ok());
        let unwrapped = other_res.unwrap();
        assert!(unwrapped.is_some());
        let mut packets = unwrapped.unwrap();
        recv_packets += packets.len();
        all_other_packets.append(&mut packets);
    }

    assert_eq!(all_test_packets.len(), num_receive / 2);
    assert_eq!(all_other_packets.len(), num_receive / 2);
    for (i, packet) in all_test_packets.into_iter().enumerate() {
        if i % 2 == 0 {
            assert_eq!(packet, Test { id: 5 });
        } else {
            assert_eq!(packet, Test { id: 8 });
        }
    }
    for (i, packet) in all_other_packets.into_iter().enumerate() {
        if i % 2 == 0 {
            assert_eq!(packet, Other { name: "spoorn".to_string(), id: 4 });
        } else {
            assert_eq!(packet, Other { name: "kiko".to_string(), id: 6 });
        }
    }

    println!("client1 exit");

    println!("send server exit");
    tx.send(0).await.unwrap();
    assert!(server.await.is_ok());
    println!("send client2 exit");
    client2_tx.send(0).await.unwrap();
    assert!(client2.await.is_ok());
}

#[tokio::test]
async fn receive_packet_e2e_async_send_to() {
    let mut manager = PacketManager::new_for_async();

    let (tx, mut rx) = mpsc::channel(100);
    let server_addr = "127.0.0.1:7000";
    let client_addr = "127.0.0.1:7001";
    let client2_addr = "127.0.0.1:7002";

    let num_receive = 100;
    let client2_max_receive = 40;
    let num_send = 100;

    // Server
    let server = tokio::spawn(async move {
        let mut m = PacketManager::new_for_async();
        assert!(m.register_send_packet::<Test>().is_ok());
        assert!(m.register_send_packet::<Other>().is_ok());
        assert!(m.register_receive_packet::<Test>(TestPacketBuilder).is_ok());
        assert!(m.register_receive_packet::<Other>(OtherPacketBuilder).is_ok());
        let server_config = ServerConfig::new_with_max_clients(server_addr, 2, 2, 2);
        assert!(m.async_init_server(server_config).await.is_ok());
        let client_connections = m.async_get_client_connections().await;
        assert_eq!(client_connections.len(), 2);
        let client1_is_first = client_connections[0].0 == client_addr
            && client_connections[0].1 == 0u32
            || client_connections[1].0 == client_addr && client_connections[1].1 == 0u32;
        if client1_is_first {
            assert!(client_connections.contains(&(client_addr.to_string(), 0u32)));
            assert!(client_connections.contains(&(client2_addr.to_string(), 1u32)));
        } else {
            assert!(client_connections.contains(&(client_addr.to_string(), 1u32)));
            assert!(client_connections.contains(&(client2_addr.to_string(), 0u32)));
        }

        let mut sent_packets = 0;
        let mut recv_packets = 0;
        let mut all_test_packets = Vec::new();
        let mut all_other_packets = Vec::new();
        let mut client2_closed = false;
        loop {
            println!("server sent {}, received {}", sent_packets, recv_packets);
            println!("all_test_packets len {}", all_test_packets.len());
            println!("all_other_packets len {}", all_other_packets.len());
            if sent_packets < num_send {
                assert!(m
                    .async_send_to::<Test>(client_addr.to_string(), Test { id: 5 })
                    .await
                    .is_ok());
                assert!(m
                    .async_send_to::<Test>(client_addr.to_string(), Test { id: 8 })
                    .await
                    .is_ok());
                assert!(m
                    .async_send_to::<Other>(
                        client_addr.to_string(),
                        Other { name: "spoorn".to_string(), id: 4 },
                    )
                    .await
                    .is_ok());
                assert!(m
                    .async_send_to::<Other>(
                        client_addr.to_string(),
                        Other { name: "kiko".to_string(), id: 6 },
                    )
                    .await
                    .is_ok());
                if !client2_closed {
                    if let Err(e) =
                        m.async_send_to::<Test>(client2_addr.to_string(), Test { id: 5 }).await
                    {
                        match e.error_type {
                            ErrorType::Unexpected => {
                                panic!("Couldn't send Test to client 2 {:?}", e);
                            }
                            ErrorType::Disconnected => {}
                        }
                    }
                    if let Err(e) =
                        m.async_send_to::<Test>(client2_addr.to_string(), Test { id: 8 }).await
                    {
                        match e.error_type {
                            ErrorType::Unexpected => {
                                panic!("Couldn't send Test to client 2 {:?}", e);
                            }
                            ErrorType::Disconnected => {}
                        }
                    }
                    if let Err(e) = m
                        .async_send_to::<Other>(
                            client2_addr.to_string(),
                            Other { name: "spoorn".to_string(), id: 4 },
                        )
                        .await
                    {
                        match e.error_type {
                            ErrorType::Unexpected => {
                                panic!("Couldn't send Test to client 2 {:?}", e);
                            }
                            ErrorType::Disconnected => {}
                        }
                    }
                    if let Err(e) = m
                        .async_send_to::<Other>(
                            client2_addr.to_string(),
                            Other { name: "kiko".to_string(), id: 6 },
                        )
                        .await
                    {
                        match e.error_type {
                            ErrorType::Unexpected => {
                                panic!("Couldn't send Test to client 2 {:?}", e);
                            }
                            ErrorType::Disconnected => {}
                        }
                    }
                }
                sent_packets += 4; // to each client
            }

            if recv_packets >= num_receive + client2_max_receive {
                println!("server done, sent {}, received {}", sent_packets, recv_packets);
                break;
            }

            let test_res = m.async_received_all::<Test, TestPacketBuilder>(true).await;
            println!("{:?}", test_res);
            assert!(test_res.is_ok());
            let mut received_all = test_res.unwrap();
            if received_all.len() == 1 {
                client2_closed = true;
            }
            assert_eq!(received_all.len(), if client2_closed { 1 } else { 2 });
            let (addr, unwrapped) = received_all.remove(0);
            assert!(m.async_get_client_id(addr.clone()).await.is_some());
            if unwrapped.is_some() {
                let mut packets = unwrapped.unwrap();
                recv_packets += packets.len();
                all_test_packets.append(&mut packets);
            }
            if !client2_closed {
                let (addr2, unwrapped2) = received_all.remove(0);
                assert!((addr == client_addr || addr == client2_addr) && (addr != addr2));
                assert!(m.async_get_client_id(addr2.clone()).await.is_some());
                if unwrapped2.is_some() {
                    let mut packets = unwrapped2.unwrap();
                    recv_packets += packets.len();
                    all_test_packets.append(&mut packets);
                }
            }

            let other_res = m.async_received_all::<Other, OtherPacketBuilder>(true).await;
            assert!(other_res.is_ok());
            let mut received_all = other_res.unwrap();
            if received_all.len() == 1 {
                client2_closed = true;
            }
            assert_eq!(received_all.len(), if client2_closed { 1 } else { 2 });
            let (addr, unwrapped) = received_all.remove(0);
            if unwrapped.is_some() {
                let mut packets = unwrapped.unwrap();
                recv_packets += packets.len();
                all_other_packets.append(&mut packets);
            }

            if !client2_closed {
                let (addr2, unwrapped2) = received_all.remove(0);
                assert!((addr == client_addr || addr == client2_addr) && (addr != addr2));
                if unwrapped2.is_some() {
                    assert!(unwrapped2.is_some());
                    let mut packets = unwrapped2.unwrap();
                    recv_packets += packets.len();
                    all_other_packets.append(&mut packets);
                }
            }
        }

        assert_eq!(all_test_packets.len(), (num_receive + client2_max_receive) / 2);
        assert_eq!(all_other_packets.len(), (num_receive + client2_max_receive) / 2);
        for (i, packet) in all_test_packets.into_iter().enumerate() {
            if i % 2 == 0 {
                assert_eq!(packet, Test { id: 6 });
            } else {
                assert_eq!(packet, Test { id: 9 });
            }
        }
        for (i, packet) in all_other_packets.into_iter().enumerate() {
            if i % 2 == 0 {
                assert_eq!(packet, Other { name: "mango".to_string(), id: 1 });
            } else {
                assert_eq!(packet, Other { name: "luna".to_string(), id: 3 });
            }
        }

        rx.recv().await;
        // loop {
        //     // Have to use tokio's sleep so it can yield to the tokio executor
        //     // https://stackoverflow.com/questions/70798841/why-does-a-tokio-thread-wait-for-a-blocking-thread-before-continuing?rq=1
        //     //sleep(Duration::from_millis(100)).await;
        // }
        println!("server exit");
    });

    let (client2_tx, mut client2_rx) = mpsc::channel(100);
    let client2 = tokio::spawn(async move {
        let mut manager = PacketManager::new_for_async();
        assert!(manager.register_receive_packet::<Test>(TestPacketBuilder).is_ok());
        assert!(manager.register_receive_packet::<Other>(OtherPacketBuilder).is_ok());
        assert!(manager.register_send_packet::<Test>().is_ok());
        assert!(manager.register_send_packet::<Other>().is_ok());
        let client_config = ClientConfig::new(client2_addr, server_addr, 2, 2);
        assert!(manager.async_init_client(client_config).await.is_ok());
        assert!(manager.async_get_client_connections().await.is_empty());

        let mut sent_packets = 0;
        let mut recv_packets = 0;
        let mut all_test_packets = Vec::new();
        let mut all_other_packets = Vec::new();
        loop {
            println!("client2 sent {}, received {}", sent_packets, recv_packets);
            if sent_packets < client2_max_receive {
                // Either send or send_to should work since there is only one recipient
                assert!(manager.async_send::<Test>(Test { id: 6 }).await.is_ok());
                assert!(manager
                    .async_send_to::<Test>(server_addr.to_string(), Test { id: 9 })
                    .await
                    .is_ok());
                assert!(manager
                    .async_send::<Other>(Other { name: "mango".to_string(), id: 1 })
                    .await
                    .is_ok());
                assert!(manager
                    .async_send_to::<Other>(
                        server_addr.to_string(),
                        Other { name: "luna".to_string(), id: 3 },
                    )
                    .await
                    .is_ok());
                sent_packets += 4;
            }

            // Close connection early for testing
            if recv_packets >= client2_max_receive {
                manager.async_close_connection(server_addr).await.unwrap();
                println!("client2 closed connection");
                break;
            }

            // Fallback to exit loop, but should error
            if recv_packets == num_receive {
                println!("client2 done, sent {}, received {}", sent_packets, recv_packets);
                sleep(Duration::from_secs(4)).await;
                break;
            }

            let test_res = manager.async_received_all::<Test, TestPacketBuilder>(true).await;
            assert!(test_res.is_ok());
            let mut received_all = test_res.unwrap();
            assert!(!received_all.is_empty());
            let (addr, unwrapped) = received_all.remove(0);
            assert_eq!(addr, server_addr);
            assert!(unwrapped.is_some());
            let mut packets = unwrapped.unwrap();
            recv_packets += packets.len();
            all_test_packets.append(&mut packets);
            let other_res = manager.async_received::<Other, OtherPacketBuilder>(true).await;
            assert!(other_res.is_ok());
            let unwrapped = other_res.unwrap();
            assert!(unwrapped.is_some());
            let mut packets = unwrapped.unwrap();
            recv_packets += packets.len();
            all_other_packets.append(&mut packets);
        }

        assert_eq!(all_test_packets.len(), client2_max_receive / 2);
        assert_eq!(all_other_packets.len(), client2_max_receive / 2);
        for (i, packet) in all_test_packets.into_iter().enumerate() {
            if i % 2 == 0 {
                assert_eq!(packet, Test { id: 5 });
            } else {
                assert_eq!(packet, Test { id: 8 });
            }
        }
        for (i, packet) in all_other_packets.into_iter().enumerate() {
            if i % 2 == 0 {
                assert_eq!(packet, Other { name: "spoorn".to_string(), id: 4 });
            } else {
                assert_eq!(packet, Other { name: "kiko".to_string(), id: 6 });
            }
        }

        client2_rx.recv().await;
        println!("client2 exit");
    });

    // Client
    let receive_results =
        register_receive!(manager, (Test, TestPacketBuilder), (Other, OtherPacketBuilder));
    assert!(receive_results.iter().all(|r| r.is_ok()));
    let send_results = register_send!(manager, Test, Other);
    assert!(send_results.iter().all(|r| r.is_ok()));
    let client_config = ClientConfig::new(client_addr, server_addr, 2, 2);
    let client = manager.async_init_client(client_config).await;
    assert!(manager.async_get_client_connections().await.is_empty());
    println!("client1: {:#?}", client);

    assert!(client.is_ok());

    let mut sent_packets = 0;
    let mut recv_packets = 0;
    let mut all_test_packets = Vec::new();
    let mut all_other_packets = Vec::new();
    loop {
        println!("client1 sent {}, received {}", sent_packets, recv_packets);
        // Send packets
        if sent_packets < num_send {
            // Either send or send_to should work since there is only one recipient
            assert!(manager.async_send::<Test>(Test { id: 6 }).await.is_ok());
            assert!(manager
                .async_send_to::<Test>(server_addr.to_string(), Test { id: 9 })
                .await
                .is_ok());
            assert!(manager
                .async_send::<Other>(Other { name: "mango".to_string(), id: 1 })
                .await
                .is_ok());
            assert!(manager
                .async_send_to::<Other>(
                    server_addr.to_string(),
                    Other { name: "luna".to_string(), id: 3 },
                )
                .await
                .is_ok());
            sent_packets += 4;
        }

        if recv_packets == num_receive {
            println!("client1 done, sent {}, received {}", sent_packets, recv_packets);
            break;
        }

        let test_res = manager.async_received_all::<Test, TestPacketBuilder>(true).await;
        assert!(test_res.is_ok());
        let mut received_all = test_res.unwrap();
        assert!(!received_all.is_empty());
        let (addr, unwrapped) = received_all.remove(0);
        assert_eq!(addr, server_addr);
        assert!(unwrapped.is_some());
        let mut packets = unwrapped.unwrap();
        recv_packets += packets.len();
        all_test_packets.append(&mut packets);
        let other_res = manager.async_received::<Other, OtherPacketBuilder>(true).await;
        assert!(other_res.is_ok());
        let unwrapped = other_res.unwrap();
        assert!(unwrapped.is_some());
        let mut packets = unwrapped.unwrap();
        recv_packets += packets.len();
        all_other_packets.append(&mut packets);
    }

    assert_eq!(all_test_packets.len(), num_receive / 2);
    assert_eq!(all_other_packets.len(), num_receive / 2);
    for (i, packet) in all_test_packets.into_iter().enumerate() {
        if i % 2 == 0 {
            assert_eq!(packet, Test { id: 5 });
        } else {
            assert_eq!(packet, Test { id: 8 });
        }
    }
    for (i, packet) in all_other_packets.into_iter().enumerate() {
        if i % 2 == 0 {
            assert_eq!(packet, Other { name: "spoorn".to_string(), id: 4 });
        } else {
            assert_eq!(packet, Other { name: "kiko".to_string(), id: 6 });
        }
    }

    println!("client1 exit");

    println!("send server exit");
    tx.send(0).await.unwrap();
    assert!(server.await.is_ok());
    println!("send client2 exit");
    client2_tx.send(0).await.unwrap();
    assert!(client2.await.is_ok());
}

#[tokio::test]
async fn receive_packet_e2e_async_send_to_with_finish_connection() {
    let mut manager = PacketManager::new_for_async();

    let (tx, mut rx) = mpsc::channel(100);
    let server_addr = "127.0.0.1:8000";
    let client_addr = "127.0.0.1:8001";
    let client2_addr = "127.0.0.1:8002";

    let num_receive = 100;
    let client2_max_receive = 40;
    let num_send = 100;

    // Server
    let server = tokio::spawn(async move {
        let mut m = PacketManager::new_for_async();
        let send_results = register_send!(m, Test, Other);
        assert!(send_results.iter().all(|r| r.is_ok()));
        let receive_results =
            register_receive!(m, [(Test, TestPacketBuilder), (Other, OtherPacketBuilder)]);
        assert!(receive_results.iter().all(|r| r.is_ok()));
        let server_config = ServerConfig::new_with_max_clients(server_addr, 2, 2, 2);
        assert!(m.async_init_server(server_config).await.is_ok());
        let client_connections = m.async_get_client_connections().await;
        assert_eq!(client_connections.len(), 2);
        let client1_is_first = client_connections[0].0 == client_addr
            && client_connections[0].1 == 0u32
            || client_connections[1].0 == client_addr && client_connections[1].1 == 0u32;
        if client1_is_first {
            assert!(client_connections.contains(&(client_addr.to_string(), 0u32)));
            assert!(client_connections.contains(&(client2_addr.to_string(), 1u32)));
        } else {
            assert!(client_connections.contains(&(client_addr.to_string(), 1u32)));
            assert!(client_connections.contains(&(client2_addr.to_string(), 0u32)));
        }

        let mut sent_packets = 0;
        let mut recv_packets = 0;
        let mut all_test_packets = Vec::new();
        let mut all_other_packets = Vec::new();
        let mut client2_closed = false;
        loop {
            println!("server sent {}, received {}", sent_packets, recv_packets);
            println!("all_test_packets len {}", all_test_packets.len());
            println!("all_other_packets len {}", all_other_packets.len());
            if sent_packets < num_send {
                assert!(m
                    .async_send_to::<Test>(client_addr.to_string(), Test { id: 5 })
                    .await
                    .is_ok());
                assert!(m
                    .async_send_to::<Test>(client_addr.to_string(), Test { id: 8 })
                    .await
                    .is_ok());
                assert!(m
                    .async_send_to::<Other>(
                        client_addr.to_string(),
                        Other { name: "spoorn".to_string(), id: 4 },
                    )
                    .await
                    .is_ok());
                assert!(m
                    .async_send_to::<Other>(
                        client_addr.to_string(),
                        Other { name: "kiko".to_string(), id: 6 },
                    )
                    .await
                    .is_ok());
                if !client2_closed {
                    if let Err(e) =
                        m.async_send_to::<Test>(client2_addr.to_string(), Test { id: 5 }).await
                    {
                        match e.error_type {
                            ErrorType::Unexpected => {
                                panic!("Couldn't send Test to client 2 {:?}", e);
                            }
                            ErrorType::Disconnected => {}
                        }
                    }
                    if let Err(e) =
                        m.async_send_to::<Test>(client2_addr.to_string(), Test { id: 8 }).await
                    {
                        match e.error_type {
                            ErrorType::Unexpected => {
                                panic!("Couldn't send Test to client 2 {:?}", e);
                            }
                            ErrorType::Disconnected => {}
                        }
                    }
                    if let Err(e) = m
                        .async_send_to::<Other>(
                            client2_addr.to_string(),
                            Other { name: "spoorn".to_string(), id: 4 },
                        )
                        .await
                    {
                        match e.error_type {
                            ErrorType::Unexpected => {
                                panic!("Couldn't send Test to client 2 {:?}", e);
                            }
                            ErrorType::Disconnected => {}
                        }
                    }
                    if let Err(e) = m
                        .async_send_to::<Other>(
                            client2_addr.to_string(),
                            Other { name: "kiko".to_string(), id: 6 },
                        )
                        .await
                    {
                        match e.error_type {
                            ErrorType::Unexpected => {
                                panic!("Couldn't send Test to client 2 {:?}", e);
                            }
                            ErrorType::Disconnected => {}
                        }
                    }
                }
                sent_packets += 4; // to each client
            }

            if recv_packets >= num_receive + client2_max_receive {
                println!("server done, sent {}, received {}", sent_packets, recv_packets);
                break;
            }

            let test_res = m.async_received_all::<Test, TestPacketBuilder>(true).await;
            println!("{:?}", test_res);
            assert!(test_res.is_ok());
            let mut received_all = test_res.unwrap();
            if received_all.len() == 1 {
                client2_closed = true;
            }
            assert_eq!(received_all.len(), if client2_closed { 1 } else { 2 });
            let (addr, unwrapped) = received_all.remove(0);
            assert!(m.async_get_client_id(addr.clone()).await.is_some());
            if unwrapped.is_some() {
                let mut packets = unwrapped.unwrap();
                recv_packets += packets.len();
                all_test_packets.append(&mut packets);
            }
            if !client2_closed {
                let (addr2, unwrapped2) = received_all.remove(0);
                assert!((addr == client_addr || addr == client2_addr) && (addr != addr2));
                assert!(m.async_get_client_id(addr2.clone()).await.is_some());
                if unwrapped2.is_some() {
                    let mut packets = unwrapped2.unwrap();
                    recv_packets += packets.len();
                    all_test_packets.append(&mut packets);
                }
            }

            // When a client calls finish_connection(), it's possible the send streams are flushed in different
            // order than invoked, so it's possible we already read all packets at this point in between
            // reading the 2 packet types.  i.e. last Other packets are already flushed and read by server, and
            // then the last Test packets are read on subsequent iteration.
            if recv_packets >= num_receive + client2_max_receive {
                println!("server done, sent {}, received {}", sent_packets, recv_packets);
                break;
            }

            let other_res = m.async_received_all::<Other, OtherPacketBuilder>(true).await;
            assert!(other_res.is_ok());
            let mut received_all = other_res.unwrap();
            if received_all.len() == 1 {
                client2_closed = true;
            }
            assert_eq!(received_all.len(), if client2_closed { 1 } else { 2 });
            let (addr, unwrapped) = received_all.remove(0);
            if unwrapped.is_some() {
                let mut packets = unwrapped.unwrap();
                recv_packets += packets.len();
                all_other_packets.append(&mut packets);
            }

            if !client2_closed {
                let (addr2, unwrapped2) = received_all.remove(0);
                assert!((addr == client_addr || addr == client2_addr) && (addr != addr2));
                if unwrapped2.is_some() {
                    assert!(unwrapped2.is_some());
                    let mut packets = unwrapped2.unwrap();
                    recv_packets += packets.len();
                    all_other_packets.append(&mut packets);
                }
            }
        }

        assert_eq!(all_test_packets.len(), (num_receive + client2_max_receive) / 2);
        assert_eq!(all_other_packets.len(), (num_receive + client2_max_receive) / 2);
        for (i, packet) in all_test_packets.into_iter().enumerate() {
            if i % 2 == 0 {
                assert_eq!(packet, Test { id: 6 });
            } else {
                assert_eq!(packet, Test { id: 9 });
            }
        }
        for (i, packet) in all_other_packets.into_iter().enumerate() {
            if i % 2 == 0 {
                assert_eq!(packet, Other { name: "mango".to_string(), id: 1 });
            } else {
                assert_eq!(packet, Other { name: "luna".to_string(), id: 3 });
            }
        }

        rx.recv().await;
        // loop {
        //     // Have to use tokio's sleep so it can yield to the tokio executor
        //     // https://stackoverflow.com/questions/70798841/why-does-a-tokio-thread-wait-for-a-blocking-thread-before-continuing?rq=1
        //     //sleep(Duration::from_millis(100)).await;
        // }
        println!("server exit");
    });

    let (client2_tx, mut client2_rx) = mpsc::channel(100);
    let client2 = tokio::spawn(async move {
        let mut manager = PacketManager::new_for_async();
        let receive_results =
            register_receive!(manager, [(Test, TestPacketBuilder), (Other, OtherPacketBuilder)]);
        assert!(receive_results.iter().all(|r| r.is_ok()));
        let send_results = register_send!(manager, [Test, Other]);
        assert!(send_results.iter().all(|r| r.is_ok()));
        let client_config = ClientConfig::new(client2_addr, server_addr, 2, 2);
        assert!(manager.async_init_client(client_config).await.is_ok());
        assert!(manager.async_get_client_connections().await.is_empty());

        let mut sent_packets = 0;
        let mut recv_packets = 0;
        let mut all_test_packets = Vec::new();
        let mut all_other_packets = Vec::new();
        loop {
            println!("client2 sent {}, received {}", sent_packets, recv_packets);
            if sent_packets < client2_max_receive {
                // Either send or send_to should work since there is only one recipient
                assert!(manager.async_send::<Test>(Test { id: 6 }).await.is_ok());
                assert!(manager
                    .async_send_to::<Test>(server_addr.to_string(), Test { id: 9 })
                    .await
                    .is_ok());
                assert!(manager
                    .async_send::<Other>(Other { name: "mango".to_string(), id: 1 })
                    .await
                    .is_ok());
                assert!(manager
                    .async_send_to::<Other>(
                        server_addr.to_string(),
                        Other { name: "luna".to_string(), id: 3 },
                    )
                    .await
                    .is_ok());
                sent_packets += 4;
            }

            // Close connection early for testing
            if recv_packets >= client2_max_receive {
                manager.async_finish_connection(server_addr).await.unwrap();
                println!(
                    "client2 finished connection, sent {}, received {}",
                    sent_packets, recv_packets
                );
                break;
            }

            // Fallback to exit loop, but should error
            if recv_packets == num_receive {
                println!("client2 done, sent {}, received {}", sent_packets, recv_packets);
                sleep(Duration::from_secs(4)).await;
                break;
            }

            let test_res = manager.async_received_all::<Test, TestPacketBuilder>(true).await;
            assert!(test_res.is_ok());
            let mut received_all = test_res.unwrap();
            assert!(!received_all.is_empty());
            let (addr, unwrapped) = received_all.remove(0);
            assert_eq!(addr, server_addr);
            assert!(unwrapped.is_some());
            let mut packets = unwrapped.unwrap();
            recv_packets += packets.len();
            all_test_packets.append(&mut packets);
            let other_res = manager.async_received::<Other, OtherPacketBuilder>(true).await;
            assert!(other_res.is_ok());
            let unwrapped = other_res.unwrap();
            assert!(unwrapped.is_some());
            let mut packets = unwrapped.unwrap();
            recv_packets += packets.len();
            all_other_packets.append(&mut packets);
        }

        assert_eq!(all_test_packets.len(), client2_max_receive / 2);
        assert_eq!(all_other_packets.len(), client2_max_receive / 2);
        for (i, packet) in all_test_packets.into_iter().enumerate() {
            if i % 2 == 0 {
                assert_eq!(packet, Test { id: 5 });
            } else {
                assert_eq!(packet, Test { id: 8 });
            }
        }
        for (i, packet) in all_other_packets.into_iter().enumerate() {
            if i % 2 == 0 {
                assert_eq!(packet, Other { name: "spoorn".to_string(), id: 4 });
            } else {
                assert_eq!(packet, Other { name: "kiko".to_string(), id: 6 });
            }
        }

        client2_rx.recv().await;
        println!("client2 exit");
    });

    // Client
    let receive_results =
        register_receive!(manager, (Test, TestPacketBuilder), (Other, OtherPacketBuilder));
    assert!(receive_results.iter().all(|r| r.is_ok()));
    let send_results = register_send!(manager, Test, Other);
    assert!(send_results.iter().all(|r| r.is_ok()));
    let client_config = ClientConfig::new(client_addr, server_addr, 2, 2);
    let client = manager.async_init_client(client_config).await;
    assert!(manager.async_get_client_connections().await.is_empty());
    println!("client1: {:#?}", client);

    assert!(client.is_ok());

    let mut sent_packets = 0;
    let mut recv_packets = 0;
    let mut all_test_packets = Vec::new();
    let mut all_other_packets = Vec::new();
    loop {
        println!("client1 sent {}, received {}", sent_packets, recv_packets);
        // Send packets
        if sent_packets < num_send {
            // Either send or send_to should work since there is only one recipient
            assert!(manager.async_send::<Test>(Test { id: 6 }).await.is_ok());
            assert!(manager
                .async_send_to::<Test>(server_addr.to_string(), Test { id: 9 })
                .await
                .is_ok());
            assert!(manager
                .async_send::<Other>(Other { name: "mango".to_string(), id: 1 })
                .await
                .is_ok());
            assert!(manager
                .async_send_to::<Other>(
                    server_addr.to_string(),
                    Other { name: "luna".to_string(), id: 3 },
                )
                .await
                .is_ok());
            sent_packets += 4;
        }

        if recv_packets == num_receive {
            println!("client1 done, sent {}, received {}", sent_packets, recv_packets);
            break;
        }

        let test_res = manager.async_received_all::<Test, TestPacketBuilder>(true).await;
        assert!(test_res.is_ok());
        let mut received_all = test_res.unwrap();
        assert!(!received_all.is_empty());
        let (addr, unwrapped) = received_all.remove(0);
        assert_eq!(addr, server_addr);
        assert!(unwrapped.is_some());
        let mut packets = unwrapped.unwrap();
        recv_packets += packets.len();
        all_test_packets.append(&mut packets);
        let other_res = manager.async_received::<Other, OtherPacketBuilder>(true).await;
        assert!(other_res.is_ok());
        let unwrapped = other_res.unwrap();
        assert!(unwrapped.is_some());
        let mut packets = unwrapped.unwrap();
        recv_packets += packets.len();
        all_other_packets.append(&mut packets);
    }

    assert_eq!(all_test_packets.len(), num_receive / 2);
    assert_eq!(all_other_packets.len(), num_receive / 2);
    for (i, packet) in all_test_packets.into_iter().enumerate() {
        if i % 2 == 0 {
            assert_eq!(packet, Test { id: 5 });
        } else {
            assert_eq!(packet, Test { id: 8 });
        }
    }
    for (i, packet) in all_other_packets.into_iter().enumerate() {
        if i % 2 == 0 {
            assert_eq!(packet, Other { name: "spoorn".to_string(), id: 4 });
        } else {
            assert_eq!(packet, Other { name: "kiko".to_string(), id: 6 });
        }
    }

    println!("client1 exit");

    println!("send server exit");
    tx.send(0).await.unwrap();
    assert!(server.await.is_ok());
    println!("send client2 exit");
    client2_tx.send(0).await.unwrap();
    assert!(client2.await.is_ok());
}
