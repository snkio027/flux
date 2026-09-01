use std::time::Duration;

use anyhow::{Context, Result, bail};
use flux::{
    config::{AppConfig, AutoOffsetReset, KafkaConfig},
    run,
};
use rdkafka::{
    ClientConfig,
    consumer::{BaseConsumer, Consumer},
    producer::{FutureProducer, FutureRecord, Producer},
    topic_partition_list::{Offset, TopicPartitionList},
    util::Timeout,
};
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn successful_records_reach_the_broker_committed_prefix() -> Result<()> {
    const TOPIC: &str = "vehicle-signals";
    const GROUP: &str = "flux-mock-integration";
    let _ = tracing_subscriber::fmt()
        .with_env_filter("flux=debug,rdkafka=debug")
        .with_test_writer()
        .try_init();

    let producer: FutureProducer = ClientConfig::new()
        .set("test.mock.num.brokers", "1")
        .create()
        .context("create producer")?;
    let cluster = producer
        .client()
        .mock_cluster()
        .context("producer did not expose its mock cluster")?;
    cluster
        .create_topic(TOPIC, 1, 1)
        .context("create mock topic")?;
    let bootstrap_servers = cluster.bootstrap_servers();
    drop(cluster);
    let mut expected_next_offset = None;
    for index in 0..3 {
        let key = format!("vehicle-{index}");
        let payload = format!("signal-{index}");
        let delivery = producer
            .send(
                FutureRecord::to(TOPIC).key(&key).payload(&payload),
                Timeout::After(Duration::from_secs(2)),
            )
            .await
            .map_err(|(error, _)| error)
            .context("produce mock record")?;
        expected_next_offset = Some(delivery.offset + 1);
    }
    let expected_next_offset = expected_next_offset.context("no records were produced")?;

    let config = AppConfig {
        kafka: KafkaConfig {
            bootstrap_servers: vec![bootstrap_servers.clone()],
            group_id: GROUP.to_owned(),
            topics: vec![TOPIC.to_owned()],
            client_id: "flux-integration-test".to_owned(),
            auto_offset_reset: AutoOffsetReset::Earliest,
            auto_commit_interval_ms: 100,
            session_timeout_ms: 6_000,
            max_poll_interval_ms: 10_000,
            work_queue_capacity: 2,
            completion_queue_capacity: 2,
            memory_budget_bytes: 1_024,
            record_accounting_overhead_bytes: 128,
        },
    };
    let shutdown = CancellationToken::new();
    let mut ingress = tokio::spawn(run(config, shutdown.clone()));

    let observer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("group.id", GROUP)
        .create()
        .context("create committed-offset observer")?;

    tokio::select! {
        result = &mut ingress => {
            result.context("ingress task panicked")??;
            bail!("ingress exited before committing the produced records");
        }
        result = timeout(Duration::from_secs(10), async {
            loop {
                if committed_offset(&observer, TOPIC)? == Some(expected_next_offset) {
                    return Ok::<_, anyhow::Error>(());
                }
                sleep(Duration::from_millis(50)).await;
            }
        }) => {
            result.context("timed out waiting for committed safe prefix")??;
        }
    }

    shutdown.cancel();
    timeout(Duration::from_secs(5), &mut ingress)
        .await
        .context("ingress did not shut down")?
        .context("ingress task panicked")??;

    if committed_offset(&observer, TOPIC)? != Some(expected_next_offset) {
        bail!("final committed offset was not the next offset after all records");
    }
    Ok(())
}

fn committed_offset(consumer: &BaseConsumer, topic: &str) -> Result<Option<i64>> {
    let mut query = TopicPartitionList::new();
    query.add_partition(topic, 0);
    let result = consumer
        .committed_offsets(query, Duration::from_secs(1))
        .context("query committed offset")?;
    Ok(
        match result
            .find_partition(topic, 0)
            .context("committed result omitted partition")?
            .offset()
        {
            Offset::Offset(offset) => Some(offset),
            _ => None,
        },
    )
}
