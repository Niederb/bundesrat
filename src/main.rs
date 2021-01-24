use polars::prelude::*;
use std::fs::File;

use plotly::common::{
    ColorScale, ColorScalePalette, DashType, Fill, Font, Line, LineShape, Marker, Mode, Title,
};
use plotly::layout::{Axis, BarMode, Layout, Legend, TicksDirection};
use plotly::{Bar, NamedColor, Plot, Rgb, Rgba, Scatter};

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

fn create_plot(kanton: &DataFrame) {
    let labels_series = kanton.column("Kanton").unwrap();
    let labels: Vec<String> = labels_series.utf8().unwrap().into_iter().map(Option::unwrap).map(|s| s.to_string()).collect();
    let values_series = kanton.column("Kanton_count").unwrap();
    let values = values_series.u32().unwrap().into_iter().map(Option::unwrap).collect();

    //values = values.collect().to_owned();
    let trace: Box<Bar<String, u32>> = Bar::new(labels, values);
    // https://www.snb.ch/de/iabout/pub/id/pub_annrep
    let layout = Layout::new().title(Title::new("Swiss National Bank Annual Report Length"))
        .x_axis(Axis::new().title(Title::new("Kanton")))
        .y_axis(Axis::new().title(Title::new("Anzahl Bundesräte")));
    let mut plot = Plot::new();
    plot.set_layout(layout);
    plot.add_trace(trace);
    plot.show();
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
    let kanton = kanton.sort("Kanton_count", true).unwrap();
    println!("{:?}", &kanton);
    create_plot(&kanton);
    
}
