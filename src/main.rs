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

    let cities = cases.cities.as_slice();
    let event_indices = cases.events.as_slice();
    let order_amounts = cases.order_amounts.as_slice();
    let event_names = events.event_names.as_slice();

    let mut sum_order_amount_boston: f64 = 0.0;
    let mut sum_order_amount_ny: f64 = 0.0;

    let len = cases.case_ids.len();

    for x in 0..len {
        if cities[x] == *city_key_boston {
            if directly_follows(&event_indices[x].0, &event_indices[x].1, &event_names, event_name_key_1, event_name_key_2) {
                sum_order_amount_boston += order_amounts[x];
            }
        } else if cities[x] == *city_key_ny {
            if directly_follows(&event_indices[x].0, &event_indices[x].1, &event_names, event_name_key_1, event_name_key_2) {
                sum_order_amount_ny += order_amounts[x];
            }
        }
    }

    return (sum_order_amount_boston, sum_order_amount_ny, sum_order_amount_boston as f64 / sum_order_amount_ny.max(1.0));
}


fn directly_follows(start: &usize, end: &usize, event_names: &[u16], event_1: &u16, event_2: &u16) -> bool {
    let mut event_name;
    for x in *start..(*end + 1) {
        event_name = event_names[x];
        if event_name == *event_1 {
            for j in x..(*end + 1) {
                event_name = event_names[j];
                if event_name == *event_2 {
                    return true;
                }
            }
        }
    }
    return false;
}

fn do_benchmark(runs: u32,
                data_loader: &DataLoader,
                city_key_boston: &u16,
                city_key_ny: &u16,
                event_name_1: &u16,
                event_name_2: &u16) {
    // warm up phase
    let start = Instant::now();
    for _i in 0..runs {
        test_query(&data_loader, &city_key_boston, &city_key_ny, &event_name_1, &event_name_2);
    }
    let end = Instant::now();
    println!("avg duration for query execution in warm up phase ({} runs): {:?}ms", runs,
             end.duration_since(start).as_millis() as f64 / runs as f64);
}

fn query_benchmark(data_loader: &DataLoader) {
    let city_key_boston = *data_loader.cities_dictionary.get("Boston").unwrap();
    let city_key_ny = *data_loader.cities_dictionary.get("New York").unwrap();
    let event_name_1 = *data_loader.event_name_dictionary.get(EVENT_NAME_SHIRT).unwrap();
    let event_name_2 = *data_loader.event_name_dictionary.get(EVENT_NAME_ORDER_CANCELLED).unwrap();

    let start = Instant::now();
    let result = test_query(&data_loader, &city_key_boston, &city_key_ny, &event_name_1, &event_name_2);
    let end = Instant::now();
    println!("result: cases {} | events {} | sum 'Boston': {} | sum 'New York' {} | ratio {}", data_loader.case_holder.case_ids.len(), data_loader.event_holder.case_ids.len(), result.0, result.1, result.2);
    println!("duration for query execution in first run: {:?}", end.duration_since(start));

    // warm up phase
    do_benchmark(100, &data_loader, &city_key_boston, &city_key_ny, &event_name_1, &event_name_2);

    // hot phase
    do_benchmark(1_000, &data_loader, &city_key_boston, &city_key_ny, &event_name_1, &event_name_2);
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
