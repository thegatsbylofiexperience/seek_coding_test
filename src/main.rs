
use serde::{Deserialize, Serialize};
use chrono::prelude::*;
use csv;
use std::collections::*;

#[derive(Serialize, Deserialize, Clone, PartialEq)]
struct TrafficCount
{
    timestamp   : String,
    count       : u32,
}

fn update_top_half_hours(current: &TrafficCount, top_three : &mut VecDeque<TrafficCount>)
{
    if top_three.len() == 0
    {
        top_three.push_back(current.clone());
    }

    let mut should_insert = false;
    let mut insert_point = 0_usize;

    for (i, traf_cnt) in top_three.iter().enumerate()
    {
        if traf_cnt.count < current.count
        {
            should_insert = true;
            insert_point = i;
            break;
        }
    }

    if should_insert
    {
        top_three.insert(insert_point, current.clone());
    }

    if top_three.len() > 3
    {
        top_three.pop_back();
    }
}

#[test]
fn test_update_top_3_half_hours()
{
    let mut top_three  : VecDeque<TrafficCount> = VecDeque::new();

    for i in 1..10
    {
        let current = TrafficCount { timestamp : "".into(), count : i };
        
        update_top_half_hours(&current, &mut top_three);
    }

    assert!(top_three.get(0) == Some(&TrafficCount { timestamp: "".into(), count: 9 } ));
    assert!(top_three.get(1) == Some(&TrafficCount { timestamp: "".into(), count: 8 } ));
    assert!(top_three.get(2) == Some(&TrafficCount { timestamp: "".into(), count: 7 } ));
}

#[test]
fn test_update_top_three_the_same()
{
    let mut top_three  : VecDeque<TrafficCount> = VecDeque::new();

    for _i in 1..10
    {
        let current = TrafficCount { timestamp : "".into(), count : 10 };
        
        update_top_half_hours(&current, &mut top_three);
    }

    assert!(top_three.get(0) == Some(&TrafficCount { timestamp: "".into(), count: 10 } ));
    assert!(top_three.get(1) == None);
    assert!(top_three.get(2) == None);
}

fn update_minimum_counts(record: &TrafficCount, min: &mut u32, current_counts: &mut VecDeque<TrafficCount>, min_counts : &mut VecDeque<TrafficCount>)
{
    current_counts.push_back(record.clone());

    if current_counts.len() > 3
    {
        current_counts.pop_front();
    }

    if current_counts.len() == 3
    {
        let mut total = 0_u32;
        for cnt in current_counts.iter()
        {
            total += cnt.count;
        }

        if *min > total
        {
            *min = total;
            *min_counts = current_counts.clone();
        }
    }
}

#[test]
fn test_minimum_counts()
{
    let mut min                                     = u32::MAX;
    let mut current_counts : VecDeque<TrafficCount> = VecDeque::new();
    let mut min_counts : VecDeque<TrafficCount>     = VecDeque::new();

    for i in 1..10
    {
        let current = TrafficCount { timestamp : "".into(), count : i };
        
        update_minimum_counts(&current, &mut min, &mut current_counts, &mut min_counts);
    }

    assert!(min_counts.len() == 3);
    assert!(min_counts.get(0) == Some(&TrafficCount { timestamp: "".into(), count: 1 } ));
    assert!(min_counts.get(1) == Some(&TrafficCount { timestamp: "".into(), count: 2 } ));
    assert!(min_counts.get(2) == Some(&TrafficCount { timestamp: "".into(), count: 3 } ));
    
    assert!(current_counts.get(0) == Some(&TrafficCount { timestamp: "".into(), count: 7 } ));
    assert!(current_counts.get(1) == Some(&TrafficCount { timestamp: "".into(), count: 8 } ));
    assert!(current_counts.get(2) == Some(&TrafficCount { timestamp: "".into(), count: 9 } ));
}

fn update_daily_totals(record: &TrafficCount, daily_totals: &mut BTreeMap<NaiveDate, u32>) -> Result<(), Box<dyn std::error::Error>>
{
    let dt = record.timestamp.parse::<NaiveDateTime>()?;
    let date = dt.date();

    if let Some(day) = daily_totals.get_mut(&date)
    {
        *day += record.count;
    }
    else
    {
        daily_totals.insert(date, record.count);
    }

    Ok(())
}

#[test]
fn test_daily_totals()
{
    let a = TrafficCount { timestamp: "2021-12-01T05:00:00".into(), count: 0 };
    let b = TrafficCount { timestamp: "2021-12-02T05:30:00".into(), count: 1 };
    let c = TrafficCount { timestamp: "2021-12-03T06:00:00".into(), count: 2 };

    let mut daily_totals : BTreeMap<NaiveDate, u32> = BTreeMap::new();

    update_daily_totals(&a, &mut daily_totals).unwrap();
    update_daily_totals(&b, &mut daily_totals).unwrap();
    update_daily_totals(&c, &mut daily_totals).unwrap();

    let mut count = 0_u32;
    for (_date, cnt) in daily_totals.iter()
    {
        assert!(cnt == &count);

        count += 1;
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>>
{
    let mut args: Vec<String> = std::env::args().collect();
    if args.len() < 2
    {
        return Err("Please enter a filename".into());
    }

    let mut rdr = csv::ReaderBuilder::new().has_headers(false).delimiter(b' ').from_path(args.remove(1))?;

    let mut total = 0_u32;
    let mut daily_totals : BTreeMap<NaiveDate, u32> = BTreeMap::new();

    let mut top_three  : VecDeque<TrafficCount> = VecDeque::new();

    let mut min                                     = u32::MAX;
    let mut current_counts : VecDeque<TrafficCount> = VecDeque::new();
    let mut min_counts : VecDeque<TrafficCount>     = VecDeque::new();

    for result in rdr.deserialize()
    {
        let hinted_result: Result<TrafficCount, csv::Error> = result;
        
        match hinted_result
        {
            Ok(record) =>
            {
                total += record.count;

                update_daily_totals(&record, &mut daily_totals)?;

                update_top_half_hours(&record, &mut top_three);

                update_minimum_counts(&record, &mut min, &mut current_counts, &mut min_counts);
            },
            Err(e) =>
            {
                return Err(e.into());
            }
        }
    }

    println!("Total: {}", total);

    for (date, count) in daily_totals.iter()
    {
        println!("{} {}", date, count);
    }

    match (top_three.get(0), top_three.get(1), top_three.get(2))
    {
        (Some(f), Some(s), Some(t)) => 
        {
            println!("First: {} {}", f.timestamp, f.count);
            println!("Second: {} {}", s.timestamp, s.count);
            println!("Third: {} {}", t.timestamp, t.count);
        },
        _ =>
        {
            return Err("Not enough data for top 3".into());
        }
    }

    if min_counts.len() == 3
    {
        println!("Min Period: Start: {} End: {} {}", min_counts[0].timestamp, min_counts[2].timestamp, min);
    }
    else
    {
        return Err(format!("Min counts has the wrong amount of entries: {} which is not 3!", min_counts.len()).into());
    }

    Ok(())
}
