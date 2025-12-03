use std::time::Duration;
use tokio::time::sleep;

pub const TRIGGER_NETWORK_INTERRUPT: bool = false;
pub const SET_NUM: usize = 2;
pub const GROUP: [usize; 4] = [0, 1, 1, 0];

pub async fn attack(from_node_id: usize, to_node_id: usize) {
    if TRIGGER_NETWORK_INTERRUPT {
        network_interrupt(from_node_id, to_node_id).await;
    }
}

pub async fn network_interrupt(from_node_id: usize, to_node_id: usize) {
    if GROUP[from_node_id] == GROUP[to_node_id] {
        return;
    }
    let delay = Duration::from_millis(200 as u64);
    sleep(delay).await;
}
