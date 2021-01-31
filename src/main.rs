use polars::lazy::dsl::*;
use polars::prelude::*;
use std::fs::File;

use plotly::common::Title;
use plotly::layout::{Axis, Layout};
use plotly::{Bar, Plot};

fn to_date_column(s: &Series) -> ChunkedArray<Date32Type> {
    let elected: Vec<&str> = s.utf8().unwrap().into_iter().map(|s| s.unwrap()).collect();
    let fmt = "%d.%m.%Y";
    let s: ChunkedArray<Date32Type> = Date32Chunked::parse_from_str_slice("Elected", &elected, fmt);
    s
}

fn load_data() -> DataFrame {
    let file = File::open("bundesrat.csv").expect("could not open file");

    let elected = Field::new("Elected", DataType::Utf8);
    let stepped_down = Field::new("Retired", DataType::Utf8);

    let schema = Schema::new(vec![elected, stepped_down]);
    let mut df = CsvReader::new(file)
        .infer_schema(None)
        .has_header(true)
        .with_delimiter(b';')
        .with_ignore_parser_errors(true)
        .with_dtype_overwrite(Some(&schema))
        .finish()
        .unwrap();
    let elected_series = df.column("Elected").unwrap();
    let elected_series = to_date_column(elected_series);
    df.replace("Elected", elected_series).unwrap();

    let elected_series = df.column("Retired").unwrap();
    let elected_series = to_date_column(elected_series);
    df.replace("Retired", elected_series).unwrap();
    df
}

fn create_plot(kanton: &DataFrame) {
    let labels_series = kanton.column("Kanton").unwrap();
    let labels: Vec<String> = labels_series
        .utf8()
        .unwrap()
        .into_iter()
        .map(|s| s.unwrap().to_string())
        .collect();
    let values_series = kanton.column("Kanton_count").unwrap();
    let values = values_series
        .u32()
        .unwrap()
        .into_iter()
        .map(Option::unwrap)
        .collect();

    let trace: Box<Bar<String, u32>> = Bar::new(labels, values);

    let layout = Layout::new()
        .title(Title::new("Members of the federal council per canton"))
        .x_axis(Axis::new().title(Title::new("Canton")))
        .y_axis(Axis::new().title(Title::new("#members")));
    let mut plot = Plot::new();
    plot.set_layout(layout);
    plot.add_trace(trace);
    //plot.show();
}

fn main() {
    let df = load_data();
    let df = df
        .lazy()
        .with_column(
            (cast(col("Retired"), DataType::Int32)
                - cast(col("Elected"), DataType::Int32))
            .alias("Days_in_Office"),
        )
        .collect()
        .unwrap();

    let df_shape = df.shape();
    println!("Dataframe rows/cols: {}/{}", df_shape.0, df_shape.1);
    println!("{:?}", df);

    let gender = { (&df).groupby("Sex").unwrap().select("Name").count() }.unwrap();
    println!("{:?}", gender);

    let party = { (&df).groupby("Party").unwrap().select("Party").count() }.unwrap();
    let party = party.sort("Party_count", true);
    println!("{:?}", party);

    let kanton = { (&df).groupby("Kanton").unwrap().select("Kanton").count() }.unwrap();
    let kanton = kanton.sort("Kanton_count", true).unwrap();
    println!("{:?}", &kanton);
    create_plot(&kanton);
}
