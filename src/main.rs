use polars::prelude::*;
use std::fs::File;

fn load_data() -> DataFrame {
    let file = File::open("bundesrat.csv").expect("could not open file");
  
    let elected = Field::new("Gewählt", DataType::Utf8);
    let stepped_down = Field::new("Zurückgetreten", DataType::Utf8);
    
    let schema = Schema::new(vec![elected, stepped_down]);
    let df = CsvReader::new(file)
            .infer_schema(None)
            .has_header(true)
            .with_delimiter(b';')
            .with_ignore_parser_errors(true)
            .with_dtype_overwrite(Some(&schema))
            .finish().unwrap();
    //let fmt = "%d.%m.%Y";//%Y-%m-%d";
    //let s0 = Date32Chunked::parse_from_str_slice("Gewählt", df.column("Gewählt").unwrap().utf8().unwrap().cont_slice(), fmt).into();
    df
  }

fn main() {
    let df = load_data();
    let df_shape = df.shape();
    println!("Dataframe rows/cols: {}/{}", df_shape.0, df_shape.1);
    println!("{:?}", df);

    let gender = {
        (&df).groupby("Geschlecht").unwrap().select("Name").count()
    }.unwrap();
    println!("{:?}", gender);

    let party = {
        (&df).groupby("Partei").unwrap().select("Partei").count()
    }.unwrap();
    let party = party.sort("Partei_count", true);
    println!("{:?}", party);

    let kanton = {
        (&df).groupby("Kanton").unwrap().select("Kanton").count()
    }.unwrap();
    let kanton = kanton.sort("Kanton_count", true);
    println!("{:?}", kanton);
    
}
