use std::env;

const LOG_LEVEL_NONE: &str = "none";
const LOG_LEVEL_DEBUG: &str = "debug";
const LOG_LEVEL_INFO: &str = "info";
const LOG_LEVEL_DEFAULT: &str = LOG_LEVEL_DEBUG;
pub const LOG_RATE: usize = 100000;

pub struct Logger {
    log_level: String
}

impl Logger {
    pub fn new(log_level: String) -> Logger {
        Logger {
            log_level: log_level
        }
    }

    pub fn debug(&self, msg: String) {
        if self.log_level == LOG_LEVEL_NONE {
            return
        }

        if self.log_level == LOG_LEVEL_DEBUG {
            println!("{}", msg)
        }
    }

    pub fn info(&self, msg: String) {
        if self.log_level == LOG_LEVEL_NONE {
            return
        }
        
        println!("{}", msg)
    }
}

pub fn logger_create() -> Logger {
    let mut log_level = LOG_LEVEL_DEFAULT.to_string();
    if let Ok(level) = env::var("LOG_LEVEL") {
        log_level = level;
    }

    let logger = Logger::new(log_level);

    logger
}
