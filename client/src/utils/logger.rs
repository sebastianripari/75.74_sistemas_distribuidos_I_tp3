const LOG_LEVEL_NONE: &str = "none";
const LOG_LEVEL_DEBUG: &str = "debug";
const LOG_LEVEL_INFO: &str = "info";

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

