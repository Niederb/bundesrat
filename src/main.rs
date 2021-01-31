use polars::lazy::dsl;
use polars::lazy::dsl::Expr;
use polars::prelude::*;
use std::fs::File;

use plotly::common::Title;
use plotly::layout::{Axis, Layout};
use plotly::{Bar, Plot};

fn to_date_column(s: &Series) -> ChunkedArray<Date32Type> {
    let elected: Vec<&str> = s.utf8().unwrap().into_iter().map(|s| s.unwrap()).collect();
    let fmt = "%d.%m.%Y";
    let s: ChunkedArray<Date32Type> = Date32Chunked::parse_from_str_slice("Gewählt", &elected, fmt);
    s
}

fn load_data() -> DataFrame {
    let file = File::open("bundesrat.csv").expect("could not open file");

    let elected = Field::new("Gewählt", DataType::Utf8);
    let stepped_down = Field::new("Zurückgetreten", DataType::Utf8);

    let schema = Schema::new(vec![elected, stepped_down]);
    let mut df = CsvReader::new(file)
        .infer_schema(None)
        .has_header(true)
        .with_delimiter(b';')
        .with_ignore_parser_errors(true)
        .with_dtype_overwrite(Some(&schema))
        .finish()
        .unwrap();
    let elected_series = df.column("Gewählt").unwrap();
    let elected_series = to_date_column(elected_series);
    df.replace("Gewählt", elected_series).unwrap();

    let elected_series = df.column("Zurückgetreten").unwrap();
    let elected_series = to_date_column(elected_series);
    df.replace("Zurückgetreten", elected_series).unwrap();
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
            (dsl::cast(dsl::col("Zurückgetreten"), DataType::Int32)
                - dsl::cast(dsl::col("Gewählt"), DataType::Int32))
            .alias("Amtszeit"),
        )
        .collect()
        .unwrap();

    let df_shape = df.shape();
    println!("Dataframe rows/cols: {}/{}", df_shape.0, df_shape.1);
    println!("{:?}", df);

    let gender = { (&df).groupby("Geschlecht").unwrap().select("Name").count() }.unwrap();
    println!("{:?}", gender);

    let party = { (&df).groupby("Partei").unwrap().select("Partei").count() }.unwrap();
    let party = party.sort("Partei_count", true);
    println!("{:?}", party);

    let kanton = { (&df).groupby("Kanton").unwrap().select("Kanton").count() }.unwrap();
    let kanton = kanton.sort("Kanton_count", true).unwrap();
    println!("{:?}", &kanton);
    create_plot(&kanton);
}
