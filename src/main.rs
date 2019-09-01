extern crate bincode;
extern crate rustc_serialize;
#[macro_use]
extern crate serde_derive;

use std::time::Instant;

use crate::data::data_loader::{CasesHolder, DataLoader, EventHolder};

mod data;


const EVENT_NAME_SHIRT: &str = "Send T-shirt to Printing";
const EVENT_NAME_ORDER_CANCELLED: &str = "Order Canceled";

/// SELECT
///   SUM(\"Order Amount in EUR\")
///     FILTER (WHERE \"City\" = 'Boston'),
///   SUM(\"Order Amount in EUR\")
///     FILTER (WHERE \"City\" = 'New York'),
///   SUM(\"Order Amount in EUR\")
///     FILTER (WHERE \"City\" = 'Boston')
///   /
///   SUM(\"Order Amount in EUR\")
///     FILTER (WHERE \"City\" = 'New York')
/// FROM CASES
/// WHERE
///   event_name MATCHES ('Send T-shirt to Printing' .* 'Order Canceled')
fn test_query(data_loader: &DataLoader, city_key_boston: &u16, city_key_ny: &u16, event_name_key_1: &u16, event_name_key_2: &u16) -> (f64, f64, f64) {
    let cases: &CasesHolder = &data_loader.case_holder;
    let events: &EventHolder = &data_loader.event_holder;

    let mut sum_order_amount_boston: f64 = 0.0;
    let mut sum_order_amount_ny: f64 = 0.0;

    for x in 0..cases.case_ids.len() {
        if cases.cities[x] == *city_key_boston {
            if directly_follows(cases.events[x].0, cases.events[x].1 + 1, &events.event_names, event_name_key_1, event_name_key_2) {
                sum_order_amount_boston += cases.order_amounts[x];
            }
        } else if cases.cities[x] == *city_key_ny {
            if directly_follows(cases.events[x].0, cases.events[x].1 + 1, &events.event_names, event_name_key_1, event_name_key_2) {
                sum_order_amount_ny += cases.order_amounts[x];
            }
        }
    }

    return (sum_order_amount_boston, sum_order_amount_ny, sum_order_amount_boston as f64 / sum_order_amount_ny.max(1.0));
}

fn directly_follows(start: usize, end: usize, event_names: &Vec<u16>, event_1: &u16, event_2: &u16) -> bool {
    let mut event_name;
    for x in start..end {
        event_name = event_names[x];
        if event_name == *event_1 {
            for j in x..end {
                event_name = event_names[j];
                if event_name == *event_2 {
                    return true;
                }
            }
        }
    }
    return false;
}

fn query_benchmark(data_loader: &DataLoader) {
    let city_key_boston = *data_loader.cities_dictionary.get("Boston").unwrap();
    let city_key_ny = *data_loader.cities_dictionary.get("New York").unwrap();
    let event_name_1 = *data_loader.event_name_dictionary.get(EVENT_NAME_SHIRT).unwrap();
    let event_name_2 = *data_loader.event_name_dictionary.get(EVENT_NAME_ORDER_CANCELLED).unwrap();

    let mut start = Instant::now();
    let result = test_query(&data_loader, &city_key_boston, &city_key_ny, &event_name_1, &event_name_2);
    let mut end = Instant::now();
    println!("result: cases {} | events {} | sum 'Boston': {} | sum 'New York' {} | ratio {}", data_loader.case_holder.case_ids.len(), data_loader.event_holder.case_ids.len(), result.0, result.1, result.2);
    println!("query processing duration for first query execution: {:?}ms", end.duration_since(start).subsec_millis() as f64);

    // warm up phase
    start = Instant::now();
    for _i in 0..100 {
        test_query(&data_loader, &city_key_boston, &city_key_ny, &event_name_1, &event_name_2);
    }
    end = Instant::now();
    println!("avg duration for query execution in warm up phase (100 runs): {:?}ms", end.duration_since(start).subsec_millis() as f64 / 100 as f64);

    // hot phase
    start = Instant::now();
    for _i in 0..1000 {
        test_query(&data_loader, &city_key_boston, &city_key_ny, &event_name_1, &event_name_2);
    }
    end = Instant::now();

    println!("avg duration for query execution in hot phase (10000 runs): {:?}ms", end.duration_since(start).subsec_millis() as f64 / 10000 as f64);
}


fn main() {
//    let case_resource_file_name = "/home/chris/workspace/sig/pex-query-engine/sampledata/PI_Training_Attr_v3_EN.csv";
//    let event_resource_file_name = "/home/chris/workspace/sig/pex-query-engine/sampledata/PI_Training_Eventlog_v3_EN.csv";
    let case_resource_file_name = "/home/chris/workspace/sig/cc_coding_challenge/PI_Training_Attr_10000.csv";
    let event_resource_file_name = "/home/chris/workspace/sig/cc_coding_challenge/PI_Training_Eventlog_10000.csv";
//    let case_resource_file_name = "/home/chris/workspace/sig/pex-query-engine/sampledata/PI_Training_Attr_v3_EN_ordered.csv";
//    let event_resource_file_name = "/home/chris/workspace/sig/pex-query-engine/sampledata/PI_Training_Eventlog_v3_EN_ordered.csv";


    let data_loader = match crate::data::data_loader::fetch_data(case_resource_file_name, event_resource_file_name) {
        Some(thing) => { thing }
        None => return
    };

    query_benchmark(&data_loader);
}
