use std::collections::hash_map::DefaultHasher;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{BufReader, BufWriter};
use std::time::Instant;

use bincode::ErrorKind;
use flate2::bufread::ZlibDecoder;
use flate2::Compression;
use flate2::write::ZlibEncoder;

use crate::data::data_loader::DataLoader;

pub struct Cache {}

fn hash_file_suffix(case_resource_file_name: &str, event_resource_file_name: &str) -> String {
    let mut hasher = DefaultHasher::new();
//    ptr::hash(case_resource_file_name, &mut hasher);
    Hash::hash_slice(case_resource_file_name.as_bytes(), &mut hasher);
    let case_file_hash = hasher.finish();

    Hash::hash_slice(event_resource_file_name.as_bytes(), &mut hasher);
    let event_file_hash = hasher.finish();
    return format!("{}_{}", case_file_hash, event_file_hash);
}

impl Cache {
    pub fn restore_data(case_resource_file_name: &str, event_resource_file_name: &str) -> Result<DataLoader, String> {
        println!("Trying to restore cached data...");
        let suffix = hash_file_suffix(case_resource_file_name, event_resource_file_name);
        let file_path = format!("./cache/data_{}.bin", suffix);

        let start = Instant::now();
        let result = File::open(file_path)
            .map_err(|err| err.to_string())
            .and_then(|file| {
                let reader = BufReader::new(file);
                let decoder = ZlibDecoder::new(reader);
                return bincode::deserialize_from::<ZlibDecoder<BufReader<File>>, DataLoader>(decoder)
                    .map_err(|err| err.to_string());
            });

        if result.is_ok() {
            let end = Instant::now();
            println!("Duration for restoring cached data: {:?}", end.duration_since(start));
        }

        return result;
    }

    pub fn store_data(data_loader: &DataLoader, case_resource_file_name: &str, event_resource_file_name: &str) -> Result<(), Box<ErrorKind>> {
        let suffix = hash_file_suffix(case_resource_file_name, event_resource_file_name);
        let file_path = format!("./cache/data_{}.bin", suffix);
        let data = File::create(file_path)
            .expect("Unable to create file");
        let writer = BufWriter::new(data);
        let encoder = ZlibEncoder::new(writer, Compression::best());

        return Ok(bincode::serialize_into(encoder, &data_loader)?);
    }
}