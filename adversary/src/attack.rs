use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime};
use tokio::time::sleep;

// 使用 AtomicBool 实现运行时可变
pub static TRIGGER_NETWORK_INTERRUPT: AtomicBool = AtomicBool::new(false);

// 添加触发时间配置（相对于程序启动的时间，单位：秒）
pub const ATTACK_START_TIME_SEC: u64 = 10; // 在程序启动后10秒触发
pub const ATTACK_DURATION_SEC: u64 = 5;    // 持续5秒

pub const GROUP: [usize; 10] = [0, 1, 1, 0, 1, 1, 0, 0, 0, 0];
pub const NETWORK_DELAY: u64 = 200;

// 启动一个后台任务来管理攻击时间
pub fn start_attack_scheduler() {
    let start_time = Instant::now();
    
    tokio::spawn(async move {
        // 等待到触发时间
        sleep(Duration::from_secs(ATTACK_START_TIME_SEC)).await;
        
        // 启用攻击
        TRIGGER_NETWORK_INTERRUPT.store(true, Ordering::Relaxed);
        println!("[ATTACK] Network interrupt enabled at {}s", start_time.elapsed().as_secs());
        
        // 持续指定时间
        sleep(Duration::from_secs(ATTACK_DURATION_SEC)).await;
        
        // 禁用攻击
        TRIGGER_NETWORK_INTERRUPT.store(false, Ordering::Relaxed);
        println!("[ATTACK] Network interrupt disabled at {}s", start_time.elapsed().as_secs());
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