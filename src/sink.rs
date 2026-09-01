use anyhow::{Result, anyhow};
use tokio::sync::mpsc;

use crate::ingress::{Completion, IngressRecord};

/// v1 proof sink: accepts ownership, acknowledges success, then releases the
/// record and its byte-budget permit. Replace this task with the real processor
/// without changing ingress acknowledgement semantics.
pub async fn run_discard_sink(
    mut records: mpsc::Receiver<IngressRecord>,
    completions: mpsc::Sender<Completion>,
) -> Result<()> {
    while let Some(record) = records.recv().await {
        let token = record.token.clone();
        completions
            .send(Completion::Succeeded(token))
            .await
            .map_err(|_| anyhow!("completion receiver closed"))?;
        drop(record);
    }
    Ok(())
}
