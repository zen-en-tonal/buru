/// Utility functions for retrying async operations.
pub async fn retry<F, Fut, T, E>(mut op: F) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
{
    let max_retries = 3;
    for attempt in 0..max_retries {
        let result = op().await;
        match result {
            Ok(v) => return Ok(v),
            Err(_) if attempt + 1 < max_retries => {
                // backoff: simple fixed delay or exponential if needed
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                continue;
            }
            Err(e) => return Err(e),
        }
    }

    unreachable!("Retry loop should return before exceeding max_retries")
}
