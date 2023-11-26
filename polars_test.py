from datetime import date
import polars as pl
import plotly.express as px

print(pl.__version__)

def read_data():
    admin_ch = pl.read_csv("bundesrat.csv", separator=";")
    # Convert the names from the format with "lastname, firstname" (with commas) to "firstname lastname"
    # This is done to allow the join with wikipedia
    admin_ch = admin_ch.with_columns(pl.col("Name").str.split(", ").list.reverse().list.join(" "))

    wikipedia_de = pl.read_csv("bundesrat-wikipedia-de.csv", separator="\t")
    data = admin_ch.join(wikipedia_de, on="Name", validate="1:1")
    assert data.height == admin_ch.height
    assert data.height == wikipedia_de.height

    wikipedia_es= pl.read_csv("bundesrat-wikipedia-es.csv", separator="\t")
    #with pl.Config(tbl_cols=wikipedia_es.width, tbl_rows=wikipedia_es.height):
    #    print(wikipedia_es)
    wikipedia_es = wikipedia_es.with_columns(pl.col("Nacido el").str.to_date("%d.%m.%Y"))
    wikipedia_es = wikipedia_es.rename({ "N°": "Nummer", "Nacido el": "Geburtsdatum"})
    data = data.join(wikipedia_es, on="Nummer", validate="1:1")
    assert data.height == wikipedia_de.height
    assert data.height == wikipedia_es.height
    return data

def extract_amtsjahre(data):
    data = data.with_columns(pl.col("Amtsjahre").str.split("–"))
    data = data.with_columns(pl.col("Amtsjahre").list.first().str.to_integer().alias("ErstesAmtsjahr"))
    data = data.with_columns(pl.col("Amtsjahre").list.last().str.replace("^$", date.today().year).str.to_integer().alias("LetztesAmtsjahr"))
    data = data.drop("Amtsjahre")
    data = data.with_columns(pl.int_ranges("ErstesAmtsjahr", pl.col("LetztesAmtsjahr") + 1).alias("AktiveJahre"))
    return data

data = read_data()
data = data.with_columns(pl.col("Elected").str.to_date("%d.%m.%Y"))
data = data.with_columns(pl.col("Elected").dt.month().alias("MonthElected"))
data = data.with_columns(pl.col("Retired").str.to_date("%d.%m.%Y"))

# Make sure that exactly seven members are currently active (have not resigned)
assert data.select(pl.col("Retired")).null_count().item() == 7
today = pl.lit(date.today())
data = data.with_columns(pl.col("Retired").fill_null(today))
assert data.select(pl.col("Retired")).null_count().item() == 0

data = data.with_columns((pl.col("Retired") - pl.col("Elected")).alias("Term"))

print(data.filter(pl.col("Term").is_not_null()).sort("Term", descending=True))
print(f"Total members in the council: {data.height}")
print(data.select(pl.col("Term")).describe())

print(data.group_by(by="Party").count().sort("count", descending=True))

print(data.group_by(by="Sex").count().sort("count", descending=True))

by_canton = data.group_by(by="Kanton").count().sort("count", descending=True)
print(by_canton)
print(f"Cantons without federal council: {26 - by_canton.height}")

print(data.group_by(by="MonthElected").count().sort("count", descending=True))

data = extract_amtsjahre(data)


# Fill in the missing data for the years alive (use current year for people that are still alive)
data = data.with_columns(pl.col("Lebensdaten").str.split("–"))
data = data.with_columns(pl.col("Lebensdaten").list.first().alias("Geburtsjahr").str.to_integer())
data = data.with_columns(pl.col("Lebensdaten").list.last().str.replace("^$", date.today().year).str.to_integer().alias("LetztesLebensjahr"))
data = data.drop("Lebensdaten")
with pl.Config(tbl_cols=data.width, tbl_rows=data.height):
    print(data)

by_year = data.with_columns(pl.col("AktiveJahre")).explode("AktiveJahre")
by_year = by_year.with_columns(pl.concat_str([pl.lit("1.1.").alias("FirstDay"), pl.col("AktiveJahre").cast(pl.Utf8)]).alias("AktiveJahre"))
by_year = by_year.with_columns(pl.col("AktiveJahre").str.to_date("%d.%m.%Y"))

# TODO: Find way to calculate age properly with considering leap years
by_year = by_year.with_columns(pl.col("AktiveJahre").sub(pl.col("Geburtsdatum")).dt.total_days().truediv(365).alias("Alter"))
#with pl.Config(tbl_cols=by_year.width, tbl_rows=by_year.height):
#    print(by_year)
print(by_year["Alter"].describe())
group_by_years = by_year.group_by("AktiveJahre").agg(
    pl.col("Alter").mean().alias("DurchschnittsAlter"), 
    pl.col("Alter").max().alias("MaxAlter"),
    pl.col("Alter").min().alias("MinAlter"))
group_by_years = group_by_years.rename({ "AktiveJahre": "JahrDatum"})
group_by_years = group_by_years.with_columns(pl.col("JahrDatum").dt.year().alias("Jahr"))
print(group_by_years.sort("DurchschnittsAlter"))

with pl.Config(tbl_cols=group_by_years.width, tbl_rows=group_by_years.height):
    print(group_by_years)
fig = px.bar(group_by_years, x="Jahr", y=["MaxAlter", "DurchschnittsAlter", "MinAlter"], title="Durchschnittsalter pro Jahr", barmode='overlay', opacity=1.0)
#fig.show()
fig.write_image("plots/Durchschnittsalter.png", width=1000)


with pl.Config(tbl_cols=data.width, tbl_rows=data.height):
    print(data)