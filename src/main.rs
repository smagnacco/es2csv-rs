use clap::{App, Arg, ArgMatches};
use elasticsearch::http::transport::Transport;
use elasticsearch::{Elasticsearch, SearchParts};
use elasticsearch::http::response::Response;
use std::process;


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let arguments = process_arguments();

    let url = get_from("url", &arguments);
    let index = get_from("index", &arguments);
    let query = get_from("query", &arguments);
    //let output = get_from("output", &arguments);

    let transport = Transport::single_node(&url)?;
    let client = Elasticsearch::new(transport);

    //"hotels_metadata_*_2020_05_29_00"
    let response: Response = client
        .search(SearchParts::Index(&[&index]))
        .from(0)
        .size(10)
        .body(json!(query))
        .send()
        .await?;

    let response_body = response.text().await?;

    println!("result: {}", response_body);

    Ok(())
}

//run -u 'http://zoom-hotels-es-c-00:9290' -i 'hotels_metadata_*_$DATE_*_'  -q '{     "_source": [     "domain.country",     "search_time",     "metadata.destination",     "metadata.hotel",     "metadata.distribution",     "metadata.checkin_date",     "metadata.stay",     "metadata.currency",     "metadata.total",     "metadata.meal_plan",     "metadata.promo_percentage",     "metadata.tags"     ],     "query": {       "bool": {         "must": [         {           "term": {             "domain.country": "AR"           }         },         {           "term": {             "domain.channel": "site"           }         },         {          "term": {             "metadata.contexts": "hotel"           }         }       ]       }     } }' -o output.csv

fn process_arguments() -> ArgMatches {
    return App::new("es2csv-rs")
        .version("0.1.0")
        .author("Sergio Magnacco <smagnacco@gmail.com>")
        .about("Argument passing")
        .arg(
            Arg::with_name("url")
                .short('u')
                .long("url")
                .takes_value(true)
                .help_heading(Some("The elastic node e.g. -u 'http:://myelastic:9200'")),
        )
        .arg(
            Arg::with_name("index")
                .short('i')
                .long("index")
                .takes_value(true)
                .help_heading(Some("Index name, like -i 'tweets_2020_05_*' ")),
        )
        .arg(
            Arg::with_name("query")
                .short('q')
                .long("query")
                .takes_value(true)
                .help_heading(Some(
                    "ES Query, like -q '{ \"query\": { \"match_all\": {} } }'",
                )),
        )
        .arg(
            Arg::with_name("output")
                .short('o')
                .long("output")
                .takes_value(true)
                .help_heading(Some("Output filename, e.g -o some.csv")),
        )
        .get_matches();
}

fn get_from(key: &str, arguments: &ArgMatches) -> String {
    match arguments.value_of(key) {
        None => {
            println!("Missing Argument, -u or -url must be provided for ES Node");
            process::exit(1);
        }
        Some(url) => url.to_string(),
    }
}