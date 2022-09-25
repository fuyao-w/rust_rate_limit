use std::ops::{Add, Sub};
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::{Duration, Instant};

#[cfg(test)]
mod tests {
    use std::panic;

    use super::*;

    #[test]
    fn take_available_test() {
        let mut b = TokenBucket::new(1000, 100, Duration::from_secs(1));
        sleep(Duration::from_secs(1));
        assert_eq!(100, b.take_available(100));
        sleep(Duration::from_secs(1));
        assert_eq!(100, b.take_available(100));
    }

    #[test]
    fn try_take_test() {
        let mut b = TokenBucket::new(1000, 100, Duration::from_secs(1));
        assert!(!b.try_take(100, Duration::ZERO));
        sleep(Duration::from_secs(1));
        assert!(b.try_take(100, Duration::ZERO));
        sleep(Duration::from_secs(1));
        assert!(b.try_take(100, Duration::ZERO));

        assert!(b.try_take(100, Duration::from_secs(1)));
    }

    #[test]
    fn take_test() {
        let mut b = TokenBucket::new(1000, 100, Duration::from_secs(1));
        let begin = Instant::now();
        b.take(200);
        assert_eq!(Instant::now().sub(begin).as_secs(), 2)
    }

    #[test]
    fn available_test() {
        let mut b = TokenBucket::new(1000, 100, Duration::from_secs(1));
        sleep(Duration::from_secs(1));
        assert_eq!(100, b.available());
    }

    #[test]
    fn time_test() {
        let b = TokenBucket::new(100000, 1, Duration::from_secs(1));
        assert_eq!(1, b.current_tick(Instant::now().add(Duration::from_secs(1))));
    }

    #[test]
    fn adjust_test() {
        let mut b = TokenBucket::new(100000, 1, Duration::from_secs(1));
        assert_eq!(0, b.current_tick(Instant::now()));
        assert_eq!(1, b.current_tick(Instant::now().add(Duration::from_secs(1))));
        assert_eq!(100, b.current_tick(Instant::now().add(Duration::from_secs(100))));

        b.adjust_available_tokens(100);
        assert_eq!(100, b.available_tokens);
        b.adjust_available_tokens(200);
        assert_eq!(100, b.available_tokens);
        b.adjust_available_tokens(400);
        assert_eq!(200, b.available_tokens);
        b.adjust_available_tokens(10000);
        assert_eq!(9600, b.available_tokens);
    }

    #[test]
    fn new_test() {
        let f = |c, q, f, reason: String| {
            if panic::catch_unwind(|| {
                TokenBucket::new(c, q, Duration::from_secs(f));
            }).is_ok() {
                panic!("{}", reason.add("should panic"));
            }
        };

        f(0, 1, 1, "capacity".to_string());
        f(1, 0, 1, "quantum".to_string());
        f(1, 1, 0, "duration".to_string());
    }
}

pub trait RateLimit {
    fn available(&mut self) -> u64;
    fn take(&mut self, count: u64) -> bool;
    fn take_available(&mut self, _count: u64) -> u64 {
        panic!("not implement")
    }
    fn try_take(&mut self, _count: u64, _max_wait: Duration) -> bool {
        panic!("not implement")
    }
}

const INFINITY_DURATION: Duration = Duration::MAX;


struct TokenBucket {
    capacity: u64,
    fill_interval: Duration,
    // 单次填充令牌数
    quantum: u64,
    available_tokens: i64,
    create_time: Instant,
    // 最新的间隔次数，当需要等待的时候计算截止时间
    last_tick: u64,

    mu: Arc<Mutex<i64>>,
}


impl TokenBucket {
    pub fn new(capacity: u64, quantum: u64, fill_interval: Duration) -> TokenBucket {
        if capacity <= 0 {
            panic!("capacity is not > 0")
        }
        if quantum <= 0 {
            panic!("quantum is not > 0")
        }
        if fill_interval < Duration::from_secs(1) {
            panic!("fill interval is not >= 1 sec")
        }
        TokenBucket {
            capacity,
            fill_interval,
            quantum,
            available_tokens: 0,
            create_time: Instant::now(),
            last_tick: 0,
            mu: Arc::new(Mutex::new(0)),
        }
    }
    fn current_tick(&self, now: Instant) -> u64 {
        let sub = now - self.create_time;
        let b = sub / self.fill_interval.as_secs() as u32;
        return b.as_secs();
    }
    // adjustAvailableTokens 调整当前桶中应该有的令牌数量
    fn adjust_available_tokens(&mut self, tick: u64) {
        let last_tick = self.last_tick;
        self.last_tick = tick;
        if self.available_tokens as u64 >= self.capacity {
            return;
        }
        self.available_tokens = ((tick - last_tick) * self.quantum) as i64;
        if self.available_tokens >= self.capacity as i64 {
            self.available_tokens = self.capacity as i64
        }
    }
    fn inner_take(&mut self, count: u64, now: Instant, max_wait: Duration) -> Result<Duration, ()> {
        if count <= 0 {
            return Ok(Duration::from_secs(0));
        }
        if count > self.capacity {
            return Err(());
        }
        let tick = self.current_tick(now);
        self.adjust_available_tokens(tick);
        let new_available = self.available_tokens - count as i64;
        if new_available > 0 {
            self.available_tokens = new_available;
            return Ok(Duration::from_secs(0));
        }
        let end_tick = (0 - new_available + self.quantum as i64 - 1) / self.quantum as i64;
        let expected_end_time = self.create_time.add(self.fill_interval * end_tick as u32);
        let wait_time = expected_end_time.sub(now);
        if wait_time <= max_wait {
            self.available_tokens = new_available;
            return Ok(wait_time);
        }
        Err(())
    }
}


impl RateLimit for TokenBucket {
    fn available(&mut self) -> u64 {
        let mu = self.mu.clone();
        let _lock = mu.lock();
        self.adjust_available_tokens(self.current_tick(Instant::now()));
        self.available_tokens as u64
    }

    fn take(&mut self, count: u64) -> bool {
        let mu = self.mu.clone();
        let lock = mu.lock();
        let res = self.inner_take(count, Instant::now(), INFINITY_DURATION);

        drop(lock);
        if let Ok(wait_time) = res {
            sleep(wait_time);
            return true;
        };
        false
    }

    fn take_available(&mut self, count: u64) -> u64 {
        if count <= 0 {
            return 0;
        }
        let mu = self.mu.clone();
        let lock = mu.lock();

        self.adjust_available_tokens(self.current_tick(Instant::now()));
        if self.available_tokens >= count as i64 {
            let real_count = self.available_tokens;
            self.available_tokens = 0;
            drop(lock);
            return real_count as u64;
        }
        drop(lock);
        0
    }

    fn try_take(&mut self, count: u64, max_wait: Duration) -> bool {
        let mu = self.mu.clone();
        let lock = mu.lock();
        let res = self.inner_take(count, Instant::now(), max_wait);
        drop(lock);
        if let Ok(wait_time) = res {
            sleep(wait_time);
            return true;
        }
        false
    }
}
