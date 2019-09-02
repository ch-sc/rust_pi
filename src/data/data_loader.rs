use std::cmp::Eq;
use std::collections::HashMap;
use std::fs::File;
use std::hash::Hash;
use std::io::BufReader;
use std::io::prelude::*;
use std::time::Instant;

use bincode::ErrorKind;

use crate::data::cache::Cache;

#[derive(Serialize, Deserialize, PartialEq, RustcEncodable, RustcDecodable)]
pub struct EventHolder {
    pub case_ids: Vec<String>,
    pub event_names: Vec<u16>,
    pub timestamps: Vec<i64>,
}

#[derive(Serialize, Deserialize, PartialEq, RustcEncodable, RustcDecodable)]
pub struct CasesHolder {
    pub case_ids: Vec<String>,
    pub order_amounts: Vec<f64>,
    pub order_statuses: Vec<String>,
    pub types_of_payment: Vec<String>,
    pub types_of_goods: Vec<String>,
    pub custom_types: Vec<String>,
    pub cities: Vec<u16>,
    /// vector of tuples; each tuple indicates start and end events (inclusive)
    pub events: Vec<(usize, usize)>,
}

#[derive(Serialize, Deserialize, PartialEq, RustcEncodable, RustcDecodable)]
pub struct DataLoader {
    pub cities_dictionary: HashMap<String, u16>,
    pub event_name_dictionary: HashMap<String, u16>,
    pub case_holder: CasesHolder,
    pub event_holder: EventHolder,
}


fn add_choice_or_default<K: Hash + Eq, V: Copy>(vector: &mut Vec<V>, map: &mut HashMap<K, V>, key: K, default_value: V) {
    let value = map.entry(key).or_insert(default_value);
    vector.push(*value);
}

fn load_and_cache_data(case_resource_file_name: &str, event_resource_file_name: &str) -> Result<DataLoader, Box<ErrorKind>> {

    let mut start = Instant::now();
    let mut data_loader = DataLoader::new();
    match data_loader.load_data(case_resource_file_name, event_resource_file_name) {
        Err(err) => println!("something went wrong: {:?}", err),
        _ => ()
    };
    let mut end = Instant::now();
    println!("duration for loading data: {:?}", end.duration_since(start));

    start = Instant::now();
    Cache::store_data(&data_loader, case_resource_file_name, event_resource_file_name)?;
    end = Instant::now();
    println!("duration for caching data: {:?}", end.duration_since(start));

    return Ok(data_loader);
}

pub fn fetch_data(case_resource_file_name: &str, event_resource_file_name: &str) -> Option<DataLoader> {
    let data_loader = match Cache::restore_data(case_resource_file_name, event_resource_file_name) {
        Ok(dl) => dl,
        Err(error) => {
            println!("Info: Could not restore cached data: {:?}", error);
            match load_and_cache_data(case_resource_file_name, event_resource_file_name) {
                Ok(dl) => dl,
                Err(error) => {
                    println!("Error: Could not load and cache data: {:?}", error.to_string());
                    return None;
                }
            }
        }
    };
    return Some(data_loader);
}

impl DataLoader {
    #[inline]
    pub fn new() -> DataLoader {
        DataLoader {
            cities_dictionary: HashMap::new(),
            event_name_dictionary: HashMap::new(),
            case_holder: CasesHolder {
                case_ids: vec![],
                events: vec![],
                order_amounts: vec![],
                order_statuses: vec![],
                types_of_payment: vec![],
                types_of_goods: vec![],
                cities: vec![],
                custom_types: vec![],
            },
            event_holder: EventHolder {
                case_ids: vec![],
                event_names: vec![],
                timestamps: vec![],
            },
        }
    }


    /// case csv file excerpt
    /// ```
    /// "CaseId";"EventName";"Timestamp"
    /// "10100";"Receive Customer Order";"2017-08-08T22:52:00"
    /// "10100";"Receive Payment";"2017-08-12T13:04:32"
    /// ```
    pub fn parse_event(&mut self, resource_input: &Vec<&str>) {
        &self.event_holder.case_ids.push(resource_input[0].to_string().trim_matches('"').to_string());

        let default_value = self.event_name_dictionary.len().clone() as u16;
        add_choice_or_default(&mut self.event_holder.event_names,
                              &mut self.event_name_dictionary,
                              resource_input[1].to_string().trim_matches('"').to_string(),
                              default_value)

// todo parse date string into i64
//    events.timestamps.push(resource_input[1]);
    }


    /// case csv file excerpt
    /// ```
    /// "CaseId";"Customer ID (CHOICE)";"Order Amount in EUR (CURRENCY)";"Order Status (CHOICE)";"Payment Type (CHOICE)";"Type of Goods (CHOICE)";"Customer Type (CHOICE)";"City (CHOICE)"
    /// "10100";"10100";"137.66";"Delivered";"Bank Transfer";"T-shirt";"Standard";"Houston"
    /// "10101";"10099";"129.90";"Delivered";"Bank Transfer";"T-shirt";"Standard";"San Francisco"
    /// ```
    pub fn parse_case(&mut self, resource_input: Vec<&str>) {
        &self.case_holder.case_ids.push(resource_input[0].trim_matches('"').to_string());

        match resource_input[2].trim_matches('"').parse::<f64>() {
            Err(err) => print!("Could not parse value '{}': {}", resource_input[2], err),
            Ok(value) => self.case_holder.order_amounts.push(value)
        }

        let default_value = self.cities_dictionary.len().clone() as u16;
        add_choice_or_default(&mut self.case_holder.cities,
                              &mut self.cities_dictionary,
                              resource_input[7].trim_matches('"').to_string(),
                              default_value);
    }

    pub fn load_data(&mut self, case_resource_file: &str,
                     event_resource_file: &str) -> Result<(), std::io::Error> {
        let case_attribute_file = File::open(case_resource_file)?;
        let event_attribute_file = File::open(event_resource_file)?;

        let mut case_reader = BufReader::new(case_attribute_file);
        let mut event_reader = BufReader::new(event_attribute_file);

        let mut case_header = String::new();
        let mut event_header = String::new();

        case_reader.read_line(&mut case_header)?;
        event_reader.read_line(&mut event_header)?;

        println!("case attribute headers: {}", case_header.trim());
        println!("event attribute headers: {}", event_header.trim());

        let mut c_index: usize = 0;
        let mut e_index: usize = 0;

        let csv_separator = ";";

        let mut event_lines = event_reader.lines();

        for result in case_reader.lines() {
            let case_line = result.expect("Unable to read line");

            // omit empty lines
            if case_line.is_empty() { continue; }

            let c_split_arr = case_line.split(csv_separator).collect::<Vec<_>>();

            self.parse_case(c_split_arr);

            let local_case_id = self.case_holder.case_ids[c_index].to_string();
            let local_start_event_index = e_index.clone();
            let mut local_end_event_index = local_start_event_index;

            loop {
                let event_line = match event_lines.next() {
                    None => break,
                    Some(result) => result?
                };

                // omit empty lines
                if event_line.is_empty() { continue; }

                let e_split_arr = event_line.split(csv_separator).collect::<Vec<_>>();
                self.parse_event(&e_split_arr);

                // if still in the same case continue loop, otherwise break out of it
                if self.event_holder.case_ids[e_index] == local_case_id {
                    local_end_event_index = e_index;
                    e_index += 1;
                } else {
                    e_index += 1;
                    break;
                }
            }

            &self.case_holder.events.push((local_start_event_index, local_end_event_index));
            c_index += 1;
        }

        Ok(())
    }
}