use std::time::Duration;
use tokio::time::sleep;

pub const TRIGGER_NETWORK_INTERRUPT: bool = false;
pub const GROUP: [usize; 10] = [0, 1, 1, 0, 1, 1, 0, 0, 0, 0];
pub const NETWORK_DELAY: u64 = 200;

pub async fn attack(from_node_id: usize, to_node_id: usize) {
    if TRIGGER_NETWORK_INTERRUPT {
        network_interrupt(from_node_id, to_node_id).await;
    }
}

pub async fn network_interrupt(from_node_id: usize, to_node_id: usize) {
    if GROUP[from_node_id] == GROUP[to_node_id] {
        return;
    }
    let delay = Duration::from_millis(NETWORK_DELAY);
    sleep(delay).await;
}
