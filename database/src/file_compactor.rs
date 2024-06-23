use super::file::record;
use super::utils;
use crate::index::generate_index_for_segment;
use crate::nulldb::NullDB;
use actix_web::web::Data;
use anyhow::anyhow;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs::OpenOptions;
use std::fs::{self};
use std::io::BufRead;
use std::io::BufReader;
use std::io::Write;
use std::mem;
use std::path::PathBuf;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::TryRecvError;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{thread, time};

pub const SEGMENT_FILE_EXT: &str = "nullsegment";
const MAX_FILE_SIZE: usize = 1024; //1kb block

pub async fn start_compaction(rx: Receiver<i32>, db: Data<NullDB>) {
    thread::spawn(move || loop {
        match rx.try_recv() {
            Err(TryRecvError::Empty) => println!("compact!"),
            _ => {
                println!("call to quite compaction");
                break;
            }
        }
        let ret = compactor(db.clone());

        if ret.is_err() {
            println!("Error compacting {ret:?}");
        }

        println!("Suspending...");
        thread::sleep(time::Duration::from_secs(300));
    });
}

pub fn compactor(db: Data<NullDB>) -> anyhow::Result<()> {
    let segment_files = utils::get_all_files_by_ext(&db.get_db_path(), SEGMENT_FILE_EXT)?;

    // stores the files for a generation
    let mut gen_name_segment_files: HashMap<i32, Vec<String>> = HashMap::new();
    // easy to iterate over list of generations
    let mut generations: HashSet<i32> = HashSet::new();

    //file names: [gen]-[time].nullsegment
    for file_path in segment_files.into_iter().rev() {
        /*
         * file names look like this:
         * [generation]-[time].nseg
         */
        let file_name_breakdown = file_path.split('-').collect::<Vec<&str>>();

        // TODO: This will panic if the file name is not in the correct format.

        if let Ok(gen_val) = file_name_breakdown[0].parse::<i32>() {
            generations.insert(gen_val);
            if let Some(generation) = gen_name_segment_files.get_mut(&gen_val) {
                generation.push(file_name_breakdown[1].to_string());
            } else {
                //This gen does not have a vec yet! Create it!
                let v = vec![file_name_breakdown[1].to_string()];
                gen_name_segment_files.insert(gen_val, v);
            }
        }
    }

    /*
     * unstable is faster, but could reorder "same" values.
     * We will not have same values as this was from a set.
     */
    let mut gen_vec: Vec<i32> = generations.into_iter().collect();
    gen_vec.sort_unstable();

    /* Setup the variables */
    let data: &mut HashSet<record::Record> = &mut HashSet::new();
    let mut compacted_files: Vec<PathBuf> = Vec::new();

    let main_log_file_name = match db.get_main_log() {
        Ok(f) => f,
        Err(e) => {
            println!("No main log file found when compacting!");
            return Err(anyhow!(
                "No main log file found when compacting! error: {}",
                e
            ));
        }
    };

    for current_gen in gen_vec.into_iter().rev() {
        println!("Gen {current_gen} in progress!");
        /*
         * Power of rust, we KNOW that this is safe because we just built it...
         * but it's better to check anyhow... sometimes annoying but.
         */
        if let Some(file_name_vec) = gen_name_segment_files.get_mut(&current_gen) {
            file_name_vec.sort_unstable();
            // iterate over each file in the generation
            // This is the opisite of the "get value" function as we want to go oldest to newest!
            for file_path in file_name_vec {
                //file names: [gen]-[time].nullsegment
                let path = db.get_path_for_file(format!("{current_gen}-{file_path}"));
                if path == main_log_file_name {
                    println!("Skipping main log file");
                    continue;
                };
                println!("{path:?}");

                let file = OpenOptions::new()
                    .read(true)
                    .write(false)
                    .open(&path)
                    .expect("db pack file doesn't exist.");

                // Read file into buffer reader
                let f = BufReader::new(file);
                // break it into lines and insert each line into our set.
                // TODO: In the event there is a read error, flatten will read in a loop forever.
                // I'm not convinced the code previously did not exhibit this behavior, so I'm leaving flatten
                // in place, where clippy points out the issue.
                for line in f.lines().flatten() {
                    // need to use our hashing object so only the "key" is looked at. pretty cool.
                    // have no idea why i'm so excited about this one single bit.
                    // this is what makes software engineering fun.
                    if let Ok(record) = db.get_file_engine().deserialize(&line) {
                        data.replace(record);
                    }
                }

                compacted_files.push(path);

                // If we are over our max file size, lets flush to disk
                // for now, we will just check at the end of each file.
                if mem::size_of_val(data) > MAX_FILE_SIZE {
                    // Calculate file generation
                    let file_gen = current_gen + 1;
                    println!("===========I DO NOT RUN============");
                    let new_segment_name = generate_segment_file_name(db.get_db_path(), file_gen);
                    // Create new file
                    let mut new_file = OpenOptions::new()
                        .write(true)
                        .truncate(false)
                        .create(true)
                        .open(&new_segment_name)
                        .unwrap();

                    // interesting we don't "care" about the order now
                    // becuase all records are unique
                    for r in data.iter() {
                        let rec = r.serialize();
                        if let Err(e) = new_file.write_all(&rec) {
                            eprintln!("Couldn't write to file: {e}");
                        }
                    }

                    let Some(index) =
                        generate_index_for_segment(&new_segment_name, db.get_file_engine())
                    else {
                        panic!("could not generate index of compacted file");
                    };
                    db.add_index(new_segment_name.clone(), index);
                    println!("saved new segment index: {new_segment_name:?}");

                    eprintln!("files to be deleted: {compacted_files:?}");
                    for f in &compacted_files {
                        db.remove_index(f);
                        if let Err(e) = fs::remove_file(f) {
                            println!("Failed to delete old file:{e:?}")
                        }
                    }

                    // These files have been deleted, clear them!
                    compacted_files.clear();
                    // This data has been writen and saved to files. Clear it up!
                    data.clear();
                }
            }
        }
    }

    /*
     * Same as above, just need to check if there is some data left over at the end
     * We will just flush it to a file. no problem.
     */
    if mem::size_of_val(data) > 0 {
        // Calculate file generation
        let file_gen = 1;

        // current time
        let new_segment_name = generate_segment_file_name(db.get_db_path(), file_gen);
        // Create new file
        let mut new_file = OpenOptions::new()
            .write(true)
            .truncate(false)
            .create(true)
            .open(new_segment_name.clone())
            .unwrap();

        // interesting we don't "care" about the order now
        // becuase all records are unique
        for r in data.iter() {
            let rec = r.serialize();
            if let Err(e) = new_file.write_all(&rec) {
                eprintln!("Couldn't write to file: {e}");
            }
        }

        let Some(index) = generate_index_for_segment(&new_segment_name, db.get_file_engine())
        else {
            panic!("failed to create index for new log segment");
        };
        db.add_index(new_segment_name, index);
    }

    println!("deleting old logs");
    // delete old compacted files now that the new files are saved to disk.
    for f in &compacted_files {
        db.remove_index(f);
        if let Err(e) = fs::remove_file(f) {
            println!("Failed to delete old file:{e:?}")
        }
    }

    Ok(())
}

fn generate_segment_file_name(base_path: PathBuf, file_gen: i32) -> PathBuf {
    let since_the_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Current time is before epoch");

    let name = format!("{file_gen}-{since_the_epoch:?}.nullsegment");

    base_path.join(name)
}
