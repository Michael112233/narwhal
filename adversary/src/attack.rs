use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime};
use tokio::time::sleep;
use log::info;

pub static TRIGGER_NETWORK_INTERRUPT: AtomicBool = AtomicBool::new(false);
pub const NETWORK_PARTITION: bool = false;

pub const ATTACK_START_TIME_SEC: u64 = 50; 
pub const ATTACK_DURATION_SEC: u64 = 30;    

pub const GROUP: [usize; 10] = [0, 1, 1, 0, 1, 1, 0, 0, 0, 0];
pub const NETWORK_DELAY: u64 = 8000;

pub fn start_attack_scheduler() {
    let start_time = Instant::now();

    tokio::spawn(async move {
        sleep(Duration::from_secs(ATTACK_START_TIME_SEC)).await;
        // start network partition attack
        TRIGGER_NETWORK_INTERRUPT.store(true, Ordering::Relaxed);
        info!("[Attack] Network attack starts at {}s", start_time.elapsed().as_secs());
        sleep(Duration::from_secs(ATTACK_DURATION_SEC)).await;
        // stop attack
        TRIGGER_NETWORK_INTERRUPT.store(false, Ordering::Relaxed);
        let end_time = Instant::now();
        info!("[Attack] Attack ends at {}s", end_time.elapsed().as_secs());
    });
}

pub async fn attack(from_node_id: usize, to_node_id: usize) {
    if TRIGGER_NETWORK_INTERRUPT.load(Ordering::Relaxed) {
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