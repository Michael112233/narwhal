// network/src/bandwidth_monitor.rs
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::watch::{self};
use tokio::time::interval;
use log::info;

// 辅助函数：格式化数字（添加千位分隔符）
fn format_number(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    let mut count = 0;
    for ch in s.chars().rev() {
        if count > 0 && count % 3 == 0 {
            result.push(',');
        }
        result.push(ch);
        count += 1;
    }
    result.chars().rev().collect()
}

// bandwidth monitor
#[derive(Clone)]
pub struct BandwidthStats {
    bytes_received: Arc<AtomicU64>,
    messages_received: Arc<AtomicU64>,
    start_time: Instant,
    channel_name: String,
    // Per-wave statistics
    wave_bytes: Arc<AtomicU64>,
    wave_messages: Arc<AtomicU64>,
    wave_start_time: Arc<std::sync::Mutex<Option<Instant>>>,
}

impl BandwidthStats {
    pub fn new(channel_name: String) -> Self {
        Self {
            bytes_received: Arc::new(AtomicU64::new(0)),
            messages_received: Arc::new(AtomicU64::new(0)),
            start_time: Instant::now(),
            channel_name,
            wave_bytes: Arc::new(AtomicU64::new(0)),
            wave_messages: Arc::new(AtomicU64::new(0)),
            wave_start_time: Arc::new(std::sync::Mutex::new(Some(Instant::now()))),
        }
    }

    pub fn record(&self, bytes: usize) {
        self.bytes_received.fetch_add(bytes as u64, Ordering::Relaxed);
        self.messages_received.fetch_add(1, Ordering::Relaxed);
        // Also record for current wave
        self.wave_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
        self.wave_messages.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get_stats(&self) -> (u64, u64, f64) {
        let duration = self.start_time.elapsed().as_secs_f64();
        let bytes = self.bytes_received.load(Ordering::Relaxed);
        let messages = self.messages_received.load(Ordering::Relaxed);

        let bps = if duration > 0.0 {
            (bytes as f64 * 8.0) / duration
        } else {
            0.0
        };

        (bytes, messages, bps)
    }

    pub fn get_wave_stats(&self) -> (u64, u64, f64) {
        let wave_start = self.wave_start_time.lock().unwrap();
        let duration = wave_start
            .map(|t| t.elapsed().as_secs_f64())
            .unwrap_or(0.0);
        let bytes = self.wave_bytes.load(Ordering::Relaxed);
        let messages = self.wave_messages.load(Ordering::Relaxed);

        let bps = if duration > 0.0 {
            (bytes as f64 * 8.0) / duration
        } else {
            0.0
        };

        (bytes, messages, bps)
    }

    pub fn reset_wave_stats(&self) {
        self.wave_bytes.store(0, Ordering::Relaxed);
        self.wave_messages.store(0, Ordering::Relaxed);
        *self.wave_start_time.lock().unwrap() = Some(Instant::now());
    }

    pub fn channel_name(&self) -> &str {
        &self.channel_name
    }
}

// Monitored receiver
pub struct MonitoredReceiver<T> {
    receiver: Receiver<T>,
    stats: BandwidthStats,
}

impl<T> MonitoredReceiver<T> {
    pub fn new(receiver: Receiver<T>, channel_name: String) -> Self {
        Self {
            receiver, 
            stats: BandwidthStats::new(channel_name)
        }
    }

    pub async fn recv(&mut self) -> Option<T> {
        self.receiver.recv().await
    }

    pub fn stats(&self) -> &BandwidthStats {
        &self.stats
    }
    }

// 独立的函数，不依赖于 MonitoredReceiver 的泛型类型
pub fn spawn_bandwidth_monitor(stats: Vec<BandwidthStats>, interval_secs: u64) {
    let stats_for_signal = stats.clone();
    let _ = tokio::spawn(async move {
        spawn_bandwidth_monitor_impl(stats, stats_for_signal, interval_secs, None).await;
    });
}

// 带round信息的版本（定期输出）
pub fn spawn_bandwidth_monitor_with_round(
    stats: Vec<BandwidthStats>, 
    interval_secs: u64,
    consensus_round: Option<Arc<AtomicU64>>
) {
    let stats_for_signal = stats.clone();
    let _ = tokio::spawn(async move {
        spawn_bandwidth_monitor_impl(stats, stats_for_signal, interval_secs, consensus_round).await;
    });
}

// 基于wave的带宽监控版本
pub fn spawn_bandwidth_monitor_with_wave(
    stats: Vec<BandwidthStats>,
    wave_notifier: tokio::sync::watch::Receiver<u64>,
) {
    let stats_for_signal = stats.clone();
    let _ = tokio::spawn(async move {
        spawn_bandwidth_monitor_wave_impl(stats, stats_for_signal, wave_notifier).await;
    });
}

#[cfg(unix)]
async fn spawn_bandwidth_monitor_impl(
    stats: Vec<BandwidthStats>,
    stats_for_signal: Vec<BandwidthStats>,
    interval_secs: u64,
    consensus_round: Option<Arc<AtomicU64>>,
) {
    // 创建 Unix 信号处理器（适用于 macOS/Linux）
    let mut sigterm = tokio::signal::unix::signal(
        tokio::signal::unix::SignalKind::terminate()
    ).expect("Failed to install SIGTERM handler");
    let mut sigint = tokio::signal::unix::signal(
        tokio::signal::unix::SignalKind::interrupt()
    ).expect("Failed to install SIGINT handler");
    
            let mut interval = interval(Duration::from_secs(interval_secs));
    let mut tick_count = 0u64;
    // 每10次输出（即每10秒）输出一次完整摘要
    const SUMMARY_INTERVAL: u64 = 10;

            loop {
        tokio::select! {
            // 定期输出统计信息
            _ = interval.tick() => {
                tick_count += 1;
                // Get current round if available
                let current_round = consensus_round.as_ref()
                    .map(|r| r.load(Ordering::Relaxed));
                
                if let Some(round) = current_round {
                    info!("=== Channel Bandwidth Statistics (Round: {}) ===", round);
                } else {
                    info!("=== Channel Bandwidth Statistics ===");
                }
                
                for stat in &stats {
                    let (bytes, messages, bps) = stat.get_stats();
                    let mbps = bps / 1_000_000.0;
                    let gbps = mbps / 1000.0;
                    
                    if let Some(round) = current_round {
                        info!(
                            "Channel: {} | Round: {} | {:.2} Mbps ({:.2} Gbps) | Total: {} bytes | Messages: {}",
                            stat.channel_name(),
                            round,
                            mbps,
                            gbps,
                            format_number(bytes),
                            format_number(messages)
                        );
                    } else {
                        info!(
                            "Channel: {} | {:.2} Mbps ({:.2} Gbps) | Total: {} bytes | Messages: {}",
                            stat.channel_name(),
                            mbps,
                            gbps,
                            format_number(bytes),
                            format_number(messages)
                        );
                    }
                }
                info!("===========================================");
                
                // 每 SUMMARY_INTERVAL 次输出一次完整摘要
                if tick_count % SUMMARY_INTERVAL == 0 {
                    info!("Periodic bandwidth summary (every {} seconds):", interval_secs * SUMMARY_INTERVAL);
                    print_final_summary(&stats_for_signal);
                }
            }
            
            // 处理 SIGTERM 信号（kill 命令默认发送）
            _ = sigterm.recv() => {
                info!("Received SIGTERM, generating final bandwidth summary...");
                print_final_summary(&stats_for_signal);
                // 给一点时间让日志输出完成
                tokio::time::sleep(Duration::from_millis(200)).await;
                break;
            }
            
            // 处理 SIGINT 信号（Ctrl+C）
            _ = sigint.recv() => {
                info!("Received SIGINT, generating final bandwidth summary...");
                print_final_summary(&stats_for_signal);
                tokio::time::sleep(Duration::from_millis(200)).await;
                break;
            }
        }
    }
}

#[cfg(not(unix))]
async fn spawn_bandwidth_monitor_impl(
    stats: Vec<BandwidthStats>,
    stats_for_signal: Vec<BandwidthStats>,
    interval_secs: u64,
    consensus_round: Option<Arc<AtomicU64>>,
) {
    // Windows 上的 Ctrl+C 处理
    let mut ctrl_c = tokio::signal::ctrl_c();
    
    let mut interval = interval(Duration::from_secs(interval_secs));
    let mut tick_count = 0u64;
    // 每10次输出（即每10秒）输出一次完整摘要
    const SUMMARY_INTERVAL: u64 = 10;

    loop {
        tokio::select! {
            // 定期输出统计信息
            _ = interval.tick() => {
                tick_count += 1;
                // Get current round if available
                let current_round = consensus_round.as_ref()
                    .map(|r| r.load(Ordering::Relaxed));
                
                if let Some(round) = current_round {
                    info!("=== Channel Bandwidth Statistics (Round: {}) ===", round);
                } else {
                    info!("=== Channel Bandwidth Statistics ===");
                }
                
                for stat in &stats {
                    let (bytes, messages, bps) = stat.get_stats();
                    let mbps = bps / 1_000_000.0;
                    let gbps = mbps / 1000.0;
                    
                    if let Some(round) = current_round {
                        info!(
                            "Channel: {} | Round: {} | {:.2} Mbps ({:.2} Gbps) | Total: {} bytes | Messages: {}",
                            stat.channel_name(),
                            round,
                            mbps,
                            gbps,
                            format_number(bytes),
                            format_number(messages)
                        );
                    } else {
                        info!(
                            "Channel: {} | {:.2} Mbps ({:.2} Gbps) | Total: {} bytes | Messages: {}",
                            stat.channel_name(), 
                            mbps,
                            gbps,
                            format_number(bytes),
                            format_number(messages)
                        );
                    }
                }
                info!("===========================================");
                
                // 每 SUMMARY_INTERVAL 次输出一次完整摘要
                if tick_count % SUMMARY_INTERVAL == 0 {
                    info!("Periodic bandwidth summary (every {} seconds):", interval_secs * SUMMARY_INTERVAL);
                    print_final_summary(&stats_for_signal);
            }
            }
            
            // Windows 上的 Ctrl+C 处理
            _ = ctrl_c => {
                info!("Received Ctrl+C, generating final bandwidth summary...");
                print_final_summary(&stats_for_signal);
                tokio::time::sleep(Duration::from_millis(200)).await;
                break;
            }
        }
    }
    }

    pub fn generate_summary(stats: &[BandwidthStats]) -> String {
        let mut summary = String::new();
        summary.push_str("\n");
        summary.push_str("-----------------------------------------\n");
        summary.push_str(" BANDWIDTH SUMMARY:\n");
        summary.push_str("-----------------------------------------\n");
        summary.push_str(" + CHANNEL STATISTICS:\n");
        
        let mut total_bytes = 0u64;
        let mut total_messages = 0u64;
        let mut total_bps = 0.0;
        let mut channel_count = 0;

        // sort by the name of channels 
        let mut sorted_stats: Vec<_> = stats.iter().collect();
        sorted_stats.sort_by_key(|s| s.channel_name());

        for stat in &sorted_stats {
            let (bytes, messages, bps) = stat.get_stats();
            let duration = stat.start_time.elapsed().as_secs_f64();
            let mbps = bps / 1_000_000.0;
            let gbps = mbps / 1000.0;
            
            total_bytes += bytes;
            total_messages += messages;
            total_bps += bps;
            channel_count += 1;

            summary.push_str(&format!(
                "  {}:\n",
                stat.channel_name()
            ));
            summary.push_str(&format!(
                "    Bandwidth: {:.2} Mbps ({:.2} Gbps)\n",
                mbps, gbps
            ));
            summary.push_str(&format!(
            "    Total Bytes: {} B ({:.2} MB)\n",
            format_number(bytes),
            bytes as f64 / 1_000_000.0
            ));
            summary.push_str(&format!(
            "    Total Messages: {}\n",
            format_number(messages)
            ));
            summary.push_str(&format!(
                "    Duration: {:.2} s\n",
                duration
            ));
            summary.push_str("\n");
        }

         // summary information
        summary.push_str(" + SUMMARY:\n");
        summary.push_str(&format!(
            "  Total Channels: {}\n",
            channel_count
        ));
        summary.push_str(&format!(
            "  Total Bandwidth: {:.2} Mbps ({:.2} Gbps)\n",
            total_bps / 1_000_000.0,
            total_bps / 1_000_000_000.0
        ));
        summary.push_str(&format!(
        "  Total Bytes Received: {} B ({:.2} MB)\n",
        format_number(total_bytes),
            total_bytes as f64 / 1_000_000.0
        ));
        summary.push_str(&format!(
        "  Total Messages Received: {}\n",
        format_number(total_messages)
        ));
        summary.push_str(&format!(
            "  Average Bandwidth per Channel: {:.2} Mbps\n",
            if channel_count > 0 {
                (total_bps / 1_000_000.0) / channel_count as f64
            } else {
                0.0
            }
        ));
        summary.push_str("-----------------------------------------\n");

        summary
    }

    pub fn print_final_summary(stats: &[BandwidthStats]) {
    let summary = generate_summary(stats);
        info!("\n\n{}", summary);
}

// 基于wave的带宽监控实现
#[cfg(unix)]
async fn spawn_bandwidth_monitor_wave_impl(
    stats: Vec<BandwidthStats>,
    stats_for_signal: Vec<BandwidthStats>,
    mut wave_notifier: watch::Receiver<u64>,
) {
    let mut sigterm = tokio::signal::unix::signal(
        tokio::signal::unix::SignalKind::terminate()
    ).expect("Failed to install SIGTERM handler");
    let mut sigint = tokio::signal::unix::signal(
        tokio::signal::unix::SignalKind::interrupt()
    ).expect("Failed to install SIGINT handler");
    
    let mut last_wave = 0u64;
    
    loop {
        tokio::select! {
            // 监听wave更新
            _ = wave_notifier.changed() => {
                let current_wave = *wave_notifier.borrow();
                if current_wave > last_wave {
                    // 输出当前wave的统计
                    info!("=== Wave {} Bandwidth Statistics ===", current_wave);
                    
                    for stat in &stats {
                        let (bytes, messages, bps) = stat.get_wave_stats();
                        let mbps = bps / 1_000_000.0;
                        let gbps = mbps / 1000.0;
                        
                        info!(
                            "Channel: {} | Wave: {} | {:.2} Mbps ({:.2} Gbps) | Wave Bytes: {} | Wave Messages: {}",
                            stat.channel_name(),
                            current_wave,
                            mbps,
                            gbps,
                            format_number(bytes),
                            format_number(messages)
                        );
                    }
                    info!("===========================================");
                    
                    // 重置wave统计
                    for stat in &stats {
                        stat.reset_wave_stats();
                    }
                    
                    last_wave = current_wave;
                }
            }
            
            // 处理 SIGTERM 信号
            _ = sigterm.recv() => {
                info!("Received SIGTERM, generating final bandwidth summary...");
                print_final_summary(&stats_for_signal);
                tokio::time::sleep(Duration::from_millis(200)).await;
                break;
            }
            
            // 处理 SIGINT 信号
            _ = sigint.recv() => {
                info!("Received SIGINT, generating final bandwidth summary...");
                print_final_summary(&stats_for_signal);
                tokio::time::sleep(Duration::from_millis(200)).await;
                break;
            }
        }
    }
}

#[cfg(not(unix))]
async fn spawn_bandwidth_monitor_wave_impl(
    stats: Vec<BandwidthStats>,
    stats_for_signal: Vec<BandwidthStats>,
    mut wave_notifier: watch::Receiver<u64>,
) {
    let mut ctrl_c = tokio::signal::ctrl_c();
    let mut last_wave = 0u64;
    
    loop {
        tokio::select! {
            // 监听wave更新
            _ = wave_notifier.changed() => {
                let current_wave = *wave_notifier.borrow();
                if current_wave > last_wave {
                    // 输出当前wave的统计
                    info!("=== Wave {} Bandwidth Statistics ===", current_wave);
                    
                    for stat in &stats {
                        let (bytes, messages, bps) = stat.get_wave_stats();
                        let mbps = bps / 1_000_000.0;
                        let gbps = mbps / 1000.0;
                        
                        info!(
                            "Channel: {} | Wave: {} | {:.2} Mbps ({:.2} Gbps) | Wave Bytes: {} | Wave Messages: {}",
                            stat.channel_name(),
                            current_wave,
                            mbps,
                            gbps,
                            format_number(bytes),
                            format_number(messages)
                        );
                    }
                    info!("===========================================");
                    
                    // 重置wave统计
                    for stat in &stats {
                        stat.reset_wave_stats();
                    }
                    
                    last_wave = current_wave;
                }
            }
            
            // Windows 上的 Ctrl+C 处理
            _ = ctrl_c => {
                info!("Received Ctrl+C, generating final bandwidth summary...");
                print_final_summary(&stats_for_signal);
                tokio::time::sleep(Duration::from_millis(200)).await;
                break;
            }
        }
    }
}