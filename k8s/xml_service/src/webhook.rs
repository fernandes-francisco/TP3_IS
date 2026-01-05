use crate::types::{ProcessingStatus, RequestMetadata, WebhookPayload};

pub async fn send_status(
    client: &reqwest::Client,
    meta: &RequestMetadata,
    status: ProcessingStatus,
    db_id: Option<i32>,
    msg: &str
) {
    let payload = WebhookPayload {
        id_requisicao: meta.request_id.clone(),
        status: status.clone(),
        db_id,
        message: msg.to_string(),
        batch_index: meta.batch_index,
        total_batch: meta.total_batch,
    };

    println!("Sending Webhook: {:?} [Part {}/{}]", status, meta.batch_index, meta.total_batch);

    let res = client.post(&meta.webhook_url)
        .json(&payload)
        .send()
        .await;

    if let Err(e) = res {
        eprintln!("Failed to send webhook: {}", e);
    }
}