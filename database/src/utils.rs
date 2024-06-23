use super::errors;
use std::collections::HashMap;
use std::collections::HashSet;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};

pub fn get_all_files_by_ext(path: &Path, ext: &str) -> std::io::Result<Vec<String>> {
    let paths = std::fs::read_dir(path)?;
    let file_paths = paths
        .into_iter()
        .flat_map(|x| {
            match x {
                Ok(y) => {
                    if get_extension_from_filename(y.file_name().to_str()?) == Some(ext) {
                        return Some(y.file_name().into_string().unwrap());
                    }
                }
                Err(_) => return None,
            }
            None
        })
        .collect();

    Ok(file_paths)
}

#[allow(dead_code)]
pub fn get_all_files_in_dir(path: &str) -> std::io::Result<Vec<PathBuf>> {
    let paths = std::fs::read_dir(path)?;

    let file_paths = paths
        .into_iter()
        .flat_map(|x| x.map(|y| y.path()).ok())
        .collect();

    Ok(file_paths)
}

pub fn get_extension_from_filename(filename: &str) -> Option<&str> {
    Path::new(filename).extension().and_then(OsStr::to_str)
}

#[derive(Debug)]
pub struct SegmentGenerationMapper {
    pub gen_name_segment_files: HashMap<i32, Vec<String>>,
    pub generations: HashSet<i32>,
}

pub fn get_generations_segment_mapper(
    path: &Path,
    ext: &str,
) -> Result<SegmentGenerationMapper, errors::NullDbReadError> {
    let segment_files =
        get_all_files_by_ext(path, ext).map_err(errors::NullDbReadError::IOError)?;

    let mut generations = SegmentGenerationMapper {
        gen_name_segment_files: HashMap::new(),
        generations: HashSet::new(),
    };

    //file names: [gen]-[time].nullsegment
    for file_path in segment_files.into_iter().rev() {
        /*
         * file names look like this:
         * [generation]-[time].nseg
         */
        let file_name_breakdown = file_path.split('-').collect::<Vec<&str>>();

        if let Ok(gen_val) = file_name_breakdown[0].parse::<i32>() {
            generations.generations.insert(gen_val);
            if let Some(generation) = generations.gen_name_segment_files.get_mut(&gen_val) {
                generation.push(file_name_breakdown[1].to_string());
            } else {
                //This gen does not have a vec yet! Create it!
                let v = vec![file_name_breakdown[1].to_string()];
                generations.gen_name_segment_files.insert(gen_val, v);
            }
        }
    }

    Ok(generations)
}
