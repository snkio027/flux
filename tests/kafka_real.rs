use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use flux::{
    config::{
        AppConfig, AutoOffsetReset, BackpressureConfig, IngressConfig, KafkaConfig, ShutdownConfig,
    },
    run_with_sink,
};
use rdkafka::{
    ClientConfig,
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    consumer::{BaseConsumer, Consumer},
    producer::{FutureProducer, FutureRecord},
    topic_partition_list::{Offset, TopicPartitionList},
    util::Timeout,
};
use testcontainers_modules::{kafka::apache, testcontainers::runners::AsyncRunner};
use tokio::{sync::mpsc, time::timeout};
use tokio_util::sync::CancellationToken;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker; executed by the real-kafka CI job"]
#[allow(
    clippy::too_many_lines,
    reason = "the full restart/replay sequence is kept in one test for auditability"
)]
async fn crash_restart_replays_from_first_unacked_gap() -> Result<()> {
    const TOPIC: &str = "vehicle-signals-replay";
    const GROUP: &str = "flux-real-replay";
    const RECORD_COUNT: usize = 100;
    const BLOCKED_INDEX: usize = 40;

    let kafka = apache::Kafka::default()
        .start()
        .await
        .context("start real Kafka container")?;
    let bootstrap_servers = format!(
        "127.0.0.1:{}",
        kafka
            .get_host_port_ipv4(apache::KAFKA_PORT)
            .await
            .context("resolve Kafka port")?
    );
    create_topic(&bootstrap_servers, TOPIC).await?;

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("message.timeout.ms", "10000")
        .create()
        .context("create real Kafka producer")?;
    let mut offsets = Vec::with_capacity(RECORD_COUNT);
    for index in 0..RECORD_COUNT {
        let payload = format!("signal-{index}");
        let delivery = producer
            .send(
                FutureRecord::<(), _>::to(TOPIC).payload(&payload),
                Timeout::After(Duration::from_secs(10)),
            )
            .await
            .map_err(|(error, _)| error)
            .context("produce real Kafka record")?;
        offsets.push(delivery.offset);
    }
    let blocked_offset = offsets[BLOCKED_INDEX];
    let expected_end = offsets[RECORD_COUNT - 1] + 1;

    let (seen_tx, mut seen_rx) = mpsc::unbounded_channel();
    let first_run = tokio::spawn(run_with_sink(
        test_config(&bootstrap_servers, GROUP),
        CancellationToken::new(),
        move |mut records, completions| async move {
            while let Some(record) = records.recv().await {
                let offset = record.offset();
                seen_tx
                    .send(offset)
                    .map_err(|_| anyhow!("first-run observer closed"))?;
                if offset == blocked_offset {
                    drop(record);
                } else {
                    completions
                        .send(record.succeed())
                        .await
                        .map_err(|_| anyhow!("first-run completion receiver closed"))?;
                }
            }
            Ok(())
        },
    ));

    for _ in 0..RECORD_COUNT {
        timeout(Duration::from_secs(30), seen_rx.recv())
            .await
            .context("timed out waiting for first-run delivery")?
            .context("first-run sink closed early")?;
    }
    let observer = offset_observer(&bootstrap_servers, GROUP)?;
    wait_for_committed(&observer, TOPIC, blocked_offset).await?;

    first_run.abort();
    let abort_error = timeout(Duration::from_secs(10), first_run)
        .await
        .context("crashed ingress task did not stop")?
        .err()
        .context("ingress completed instead of being aborted")?;
    if !abort_error.is_cancelled() {
        bail!("crashed ingress task failed unexpectedly: {abort_error}");
    }

    let first_committed = committed_offset(&observer, TOPIC)?;
    if first_committed != Some(blocked_offset) {
        bail!(
            "crash changed the committed safe prefix: committed={first_committed:?}, gap={blocked_offset}"
        );
    }

    let (replay_tx, mut replay_rx) = mpsc::unbounded_channel();
    let second_shutdown = CancellationToken::new();
    let second_run = tokio::spawn(run_with_sink(
        test_config(&bootstrap_servers, GROUP),
        second_shutdown.clone(),
        move |mut records, completions| async move {
            while let Some(record) = records.recv().await {
                let offset = record.offset();
                replay_tx
                    .send(offset)
                    .map_err(|_| anyhow!("replay observer closed"))?;
                completions
                    .send(record.succeed())
                    .await
                    .map_err(|_| anyhow!("replay completion receiver closed"))?;
            }
            Ok(())
        },
    ));

    let replay_start = timeout(Duration::from_secs(30), replay_rx.recv())
        .await
        .context("timed out waiting for replay")?
        .context("replay sink closed early")?;
    if replay_start != blocked_offset {
        bail!(
            "restart did not resume exactly at the first unacked gap: replay_start={replay_start}, gap={blocked_offset}, end={expected_end}"
        );
    }
    wait_for_committed(&observer, TOPIC, expected_end).await?;

    second_shutdown.cancel();
    timeout(Duration::from_secs(10), second_run)
        .await
        .context("second ingress did not stop")?
        .context("second ingress task panicked")??;
    Ok(())
}

async fn create_topic(bootstrap_servers: &str, topic: &str) -> Result<()> {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .create()
        .context("create Kafka admin client")?;
    let results = admin
        .create_topics(
            &[NewTopic::new(topic, 1, TopicReplication::Fixed(1))],
            &AdminOptions::new().operation_timeout(Some(Duration::from_secs(10))),
        )
        .await
        .context("create real Kafka topic")?;
    for result in results {
        result.map_err(|(name, error)| anyhow!("create topic {name}: {error}"))?;
    }
    Ok(())
}

fn test_config(bootstrap_servers: &str, group_id: &str) -> AppConfig {
    AppConfig {
        kafka: KafkaConfig {
            bootstrap_servers: vec![bootstrap_servers.to_owned()],
            group_id: group_id.to_owned(),
            topics: vec!["vehicle-signals-replay".to_owned()],
            client_id: "flux-real-kafka-test".to_owned(),
            auto_offset_reset: AutoOffsetReset::Earliest,
            auto_commit_interval_ms: 100,
            session_timeout_ms: 6_000,
            max_poll_interval_ms: 30_000,
            prefetch_max_kbytes: 4 * 1024,
        },
        ingress: IngressConfig {
            work_queue_capacity: 256,
            completion_queue_capacity: 256,
            memory_budget_bytes: 4 * 1024 * 1024,
            max_payload_bytes: 1024,
            backpressure: BackpressureConfig {
                pause_high_watermark_percent: 80,
                resume_low_watermark_percent: 50,
            },
        },
        shutdown: ShutdownConfig { grace_ms: 5_000 },
    }
}

fn offset_observer(bootstrap_servers: &str, group_id: &str) -> Result<BaseConsumer> {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("group.id", group_id)
        .create()
        .context("create committed-offset observer")
}

async fn wait_for_committed(consumer: &BaseConsumer, topic: &str, expected: i64) -> Result<()> {
    timeout(Duration::from_secs(30), async {
        loop {
            if committed_offset(consumer, topic)? == Some(expected) {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .context("timed out waiting for committed replay prefix")?
}

fn committed_offset(consumer: &BaseConsumer, topic: &str) -> Result<Option<i64>> {
    let mut query = TopicPartitionList::new();
    query.add_partition(topic, 0);
    let result = consumer
        .committed_offsets(query, Duration::from_secs(2))
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
