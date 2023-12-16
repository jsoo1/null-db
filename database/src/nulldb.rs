use crate::index;
use crate::{errors, file_compactor, utils};
use anyhow::anyhow;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::BufRead;
use std::path::{PathBuf, Path};
use std::sync::RwLock;
use std::{fs::File, io::BufReader};
use crate::index::*;
use actix_web::web::Data;
use std::sync::mpsc;
use crate::EasyReader;
use std::convert::TryInto;
use std::time;

pub const TOMBSTONE: &'static str = "~tombstone~";
pub const LOG_SEGMENT_EXT: &'static str = "nullsegment";

pub struct NullDB {
    main_log_mutex: RwLock<PathBuf>,
    main_log_file_mutex: RwLock<bool>,
    main_log_memory_mutex: RwLock<HashMap<String, String>>,
    // Segment, Index 
    log_indexes: RwLock<HashMap<PathBuf, Index>>,
    pub config: RwLock<Config>,
}

#[derive(Clone,Debug)]
pub struct Config {
    path: PathBuf,
    compaction: bool,
}

impl Config {
    pub fn new(path: PathBuf, compaction: bool) -> Config {
        Config { path, compaction }
    }
}

// TODO: pass in PathBuff to define where this database is working
pub fn create_db(config: Config) -> anyhow::Result<Data<NullDB>> {

    let null_db = NullDB::new(config.clone());

    let Ok(null_db) = null_db else {
        panic!("Could not create indexes!!!");
    };

    let db_arc = Data::new(null_db);
    if config.compaction {
        let (_, rx) = mpsc::channel();
        let _file_compactor_thread = file_compactor::start_compaction(rx, db_arc.clone());
    }
    Ok(db_arc)
}

impl NullDB {
    pub fn get_db_path(&self) -> PathBuf {
        let Ok(config) = self.config.read() else {
            println!("could not get readlock on config!");
            panic!("we have poisiod our locks");
        };
        config.path.clone()
    }

    pub fn get_path_for_file(&self, file_name: String) -> PathBuf {
        let mut path = PathBuf::new();

        path.push(self.get_db_path());
        path.push(file_name);
        path
    }

    pub fn new(config: Config) -> anyhow::Result<NullDB,errors::NullDbReadError> {
        let main_log = match Self::create_next_segment_file(config.path.as_path()) {
            Ok(main_log) => main_log,
            Err(e) => {
                panic!("Could not create new main log file! error: {}", e);
            }
        };
        let indexes = RwLock::new(generate_indexes(config.path.as_path(),&main_log)?);
        Ok(NullDB {
            main_log_mutex: RwLock::new(main_log),
            main_log_file_mutex: RwLock::new(false),
            main_log_memory_mutex: RwLock::new(HashMap::new()),
            log_indexes: indexes,
            config: RwLock::new(config),
        })
    }

    fn create_next_segment_file(path: &Path) -> anyhow::Result<PathBuf> {
        let time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let mut seg_file = PathBuf::new();
        seg_file.push(path);
        let file_name = format!("{}-{}.{}", 0, time, LOG_SEGMENT_EXT);
        seg_file.push(file_name.clone());
        let _file = File::create(seg_file.clone())?;
        Ok(seg_file)
    }

    // gets name of main log file "right now" does not hold read lock so value maybe be stale
    pub fn get_main_log(&self) -> anyhow::Result<PathBuf> {
        match self.main_log_mutex.read() {
            Ok(main_log) => Ok(main_log.clone()),
            Err(_) => Err(anyhow!("Could not get main log file!")),
        }
    }

    // Deletes a record from the log
    pub fn delete_record(&self, key: String) -> anyhow::Result<()> {
        self.write_value_to_log(key, TOMBSTONE.into())
    }

    pub fn get_value_for_key(&self, key: String) -> anyhow::Result<String, errors::NullDbReadError> {
        // Aquire read lock on main log in memory
        let Ok(main_log) = self.main_log_memory_mutex.read() else {
            println!("Could not get main log file!");
            panic!("we have poisiod our locks");
        };

        // Check the main log first for key
        if let Some(value) = main_log.get(&key) {
            println!("Returned value from main log! {}", value);
            return Ok(value.clone());
        }

        let Ok(config) = self.config.read() else {
            println!("could not get readlock on config!");
            panic!("we have poisiod our locks");
        };
        // If not in main log, check all the segments
        let mut generation_mapper =
            utils::get_generations_segment_mapper(config.path.as_path(), file_compactor::SEGMENT_FILE_EXT.to_owned())?;

        /*
         * unstable is faster, but could reorder "same" values.
         * We will not have same values as this was from a set.
         */
        let mut gen_vec: Vec<i32> = generation_mapper.generations.into_iter().collect();
        gen_vec.sort_unstable();

        //Umm... I don't know if this is the best way to do this. it's what I did though, help me?
        let mut gen_iter = gen_vec.into_iter();
        let Ok(main_log_filename) = self.main_log_mutex.read() else {
            panic!("we have poisiod our locks... don't do this please");
        };

        while let Some(current_gen) = gen_iter.next() {
            println!("Gen {} in progress!", current_gen);
            /*
             * Power of rust, we KNOW that this is safe because we just built it...
             * but it's better to check anyhow... sometimes annoying but.
             */
            if let Some(file_name_vec) = generation_mapper
                .gen_name_segment_files
                .get_mut(&current_gen)
            {
                file_name_vec.sort_unstable();
                let mut file_name_iter = file_name_vec.into_iter();

                let then = time::Instant::now();
            
                while let Some(file_path) = file_name_iter.next_back() {
                    //file names: [gen]-[time].nullsegment
                    let path = self.get_path_for_file(format!("{}-{}", current_gen, file_path.clone()));

                    // Don't check the main log, we already did that.
                    if path == *main_log_filename {
                        continue;
                    }

                    //Check index for value
                    let Ok(log_index) = self.log_indexes.read() else {
                        panic!("could not optain read log on indexes");
                    };
                        
                    let index = log_index.get(&path);
                    
                    let Some(index) = index else {
                        println!("{:?}", log_index);
                        println!("{:?}", path);
                        panic!("Index not found for log segment");
                    };

                    let Some(line_number) = index.get(&key) else {
                        continue;
                    };

                    println!("record found, file:{:?}, line_number:{}", path.clone(),line_number);
                    let dur: u128 = ((time::Instant::now() - then).as_millis())                
                        .try_into()
                        .unwrap();
                    println!("inner dur: {}", dur);
                    return get_value_from_segment(path, *line_number);
                }
                
            }
            
        }
        Ok("value not found".into())
    }

    // Writes value to log, will create new log if over 64 lines.
    pub fn write_value_to_log(&self, key: String, value: String) -> anyhow::Result<()> {
        let line_count;
        {
            let main_log = self.main_log_mutex.read();
            let Ok(main_log) = main_log else {
                println!("Could not get main log file!");
                return Err(anyhow!("Could not get main log file!"));
            };
            let file = File::open(main_log.clone())?;
            // make new file if over our 64 lines max
            let f = BufReader::new(file);
            line_count = f.lines().count();
        }

        // Check if main log is "full"
        if line_count > 5120 {
            let main_log = self.main_log_mutex.write();
            let Ok(mut main_log) = main_log else {
                return Err(anyhow!("Could not get main log file!"));
            };
            let Some(index) = index::generate_index_for_segment(&main_log) else {
                panic!("could not create index of main log");
            };
            self.add_index(main_log.clone(), index);
            
            let Ok(mut main_memory_log) = self.main_log_memory_mutex.write() else {
                println!("Could not get main log file!");
                panic!("we have poisiod our locks");
            };

            // Check the main log first for key
            main_memory_log.clear();
            let Ok(config) = self.config.read() else {
                println!("could not get readlock on config!");
                panic!("we have poisiod our locks");
            };
            *main_log = Self::create_next_segment_file(config.path.as_path())?;
        }

        // Aquire write lock on main log file
        let Ok(_main_log_disk) = self.main_log_file_mutex.write() else {
            return Err(anyhow!("Could not get main log file!"));
        };

        // Aquire write lock on main log memory
        let Ok(mut main_log_memory) = self.main_log_memory_mutex.write() else {
            return Err(anyhow!("Could not get main log in memory!"));
        };

        // Aquire read lock on main log file name
        let Ok(main_log_name) = self.main_log_mutex.read() else {
            return Err(anyhow!("Could not get main log file name!"));
        };

        // Write to memory
        let old_value = main_log_memory.insert(key.clone(), value.clone());

        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(main_log_name.clone())?;

        let ret = writeln!(file, "{}:{}", key, value);

        if ret.is_err() {
            // If we failed to write to disk, reset the memory to what it was before
            if let Some(old_value) = old_value {
                main_log_memory.insert(key.clone(), old_value);
            } else {
                main_log_memory.remove(&key);
            }
            return Err(anyhow!("Could not write to main log file!"));
        }
        Ok(())
    }

    pub fn add_index(&self, segment: PathBuf, index: Index) -> Option<Index> {
        let Ok(mut main_index) = self.log_indexes.write() else {
            panic!("could not optain write lock to index");
        };

        main_index.insert(segment, index)
    }

    pub fn remove_index(&self, segment: &PathBuf) -> Option<Index> {
        let Ok(mut main_index) = self.log_indexes.write() else {
            panic!("could not optain write lock to index");
        };

        main_index.remove(segment)
    }
}

fn get_value_from_segment(path: PathBuf, line_number: usize ) -> anyhow::Result<String,errors::NullDbReadError> {
    let file = OpenOptions::new()
        .read(true)
        .write(false)
        .open(path.clone())
        .expect("db pack file doesn't exist.");

    let bb = BufReader::new(file);
    let mut buffer_iter = bb.lines();                   
    // .nth -> Option<Result<String,Err>>
    let value = buffer_iter.nth(line_number).expect("index missed");

    let Ok(value) = value else {
        panic!("data corrupted");
    };

    let parsed_value = get_value_from_database(value)?;

    return Ok(parsed_value);
}

pub fn get_value_from_database(value: String) -> anyhow::Result<String, errors::NullDbReadError> {
    
    let split = value.split(":").collect::<Vec<&str>>();
    if split.len() != 2 {
       return Err(errors::NullDbReadError::Corrupted); 
    }

    let val = split[1].to_string().clone();
    if val == TOMBSTONE {
        return Err(errors::NullDbReadError::ValueDeleted);
    }

    Ok(value)
}

pub fn get_key_from_database_line(value: String) -> anyhow::Result<String, errors::NullDbReadError> {
    
    let split = value.split(":").collect::<Vec<&str>>();
    if split.len() != 2 {
       return Err(errors::NullDbReadError::Corrupted); 
    }

    let val = split[1].to_string().clone();
    if val == TOMBSTONE {
        return Err(errors::NullDbReadError::ValueDeleted);
    }

    let key = split[0].to_string().clone();

    Ok(key)
}

pub fn check_file_for_key(key: String, file: File) -> Result<String, errors::NullDbReadError> {
    let mut reader = EasyReader::new(file).unwrap();
    // Generate index (optional)
    if let Err(e) = reader.build_index() {
        return Err(errors::NullDbReadError::IOError(e));
    }
    reader.eof();
    while let Some(line) = reader.prev_line().unwrap() {
        let split = line.split(":").collect::<Vec<&str>>();
        if split.len() != 2 {
            continue;
        }
        if split[0] == key {
            let val = split[1].to_string().clone();
            if val == TOMBSTONE {
                return Err(errors::NullDbReadError::ValueDeleted);
            }
            return Ok(split[1].to_string().clone());
        }
    }
    return Err(errors::NullDbReadError::ValueNotFound);
}

