use polars::prelude::*;
use std::fs::File;

fn load_data() -> Result<DataFrame> {
    let file = File::open("bundesrat.csv").expect("could not open file");
  
    CsvReader::new(file)
            .infer_schema(None)
            .has_header(true)
            //.with_one_thread(true) // set this to false to try multi-threaded parsing
            .with_delimiter(b';')
            .with_ignore_parser_errors(true)
            .finish()
  }

fn main() {
    let df = load_data().unwrap();
    let df_shape = df.shape();
    println!("Dataframe rows/cols: {}/{}", df_shape.0, df_shape.1);
    println!("{:?}", df.select("Name"));

    let gender = {
        (&df).groupby("Geschlecht").unwrap().select("Name").count()
    }.unwrap();
    println!("{:?}", gender);

    let party = {
        (&df).groupby("Partei").unwrap().select("Name").count()
    }.unwrap();
    println!("{:?}", party);

    let mut kanton = {
        (&df).groupby("Kanton").unwrap().select("Name").count()
    }.unwrap();
    for k in kanton.iter_record_batches(10) {
        println!("{:?}", k);
    }
    
}
