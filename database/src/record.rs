use std::hash::{Hash, Hasher};

// TODO: Move this to file::Record and delete this
#[derive(Debug)]
pub struct Record {
    key: String,
    value: String,
}

// Only hash the key as this is what defines what is "unique"
impl Hash for Record {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.key.hash(state);
    }
}

impl Eq for Record {}

impl PartialEq for Record {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }

    fn ne(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

// Lets make our records a bit easier to use!
impl Record {
    pub fn new(keyvalue: String) -> Option<Record> {
        let split = keyvalue.split(":").collect::<Vec<&str>>();
        if split.len() == 2 {
            return Some(Record {
                key: split[0].to_string().clone(),
                value: split[1].to_string().clone(),
            });
        }
        return None;
    }

    pub fn get_string(&self) -> String {
        return format!("{}:{}", self.key, self.value);
    }
}
