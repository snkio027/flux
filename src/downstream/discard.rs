use anyhow::{Result, anyhow};
use tokio::sync::mpsc;

use crate::{Completion, IngressRecord};

/// v1 proof sink. Replace it with the real processor while preserving the
/// consume-record-into-completion ownership protocol.
pub(crate) async fn run_discard_sink(
    mut records: mpsc::Receiver<IngressRecord>,
    completions: mpsc::Sender<Completion>,
) -> Result<()> {
    while let Some(record) = records.recv().await {
        completions
            .send(record.succeed())
            .await
            .map_err(|_| anyhow!("completion receiver closed"))?;
    }
    Ok(())
}
