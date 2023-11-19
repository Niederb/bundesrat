from datetime import date
import polars as pl
import plotly.express as px

print(pl.__version__)

admin_ch = pl.read_csv("bundesrat.csv", separator=";")
# Convert the names from the format with "lastname, firstname" (with commas) to "firstname lastname"
# This is done to allow the join with wikipedia
admin_ch = admin_ch.with_columns(pl.col("Name").str.split(", ").list.reverse().list.join(" "))

wikipedia = pl.read_csv("bundesrat-wikipedia-de.csv", separator="\t")
data = admin_ch.join(wikipedia, on="Name", validate="1:1")
assert data.height == admin_ch.height
assert data.height == wikipedia.height

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

data = data.with_columns(pl.col("Amtsjahre").str.split("–"))
data = data.with_columns(pl.col("Amtsjahre").list.first().str.to_integer().alias("ErstesAmtsjahr"))
data = data.with_columns(pl.col("Amtsjahre").list.last().str.replace("^$", date.today().year).str.to_integer().alias("LetztesAmtsjahr"))
data = data.drop("Amtsjahre")
data = data.with_columns(pl.int_ranges("ErstesAmtsjahr", pl.col("LetztesAmtsjahr") + 1).alias("AktiveJahre"))

# Fill in the missing data for the years alive (use current year for people that are still alive)
data = data.with_columns(pl.col("Lebensdaten").str.split("–"))
data = data.with_columns(pl.col("Lebensdaten").list.first().alias("Geburtsjahr").str.to_integer())
data = data.with_columns(pl.col("Lebensdaten").list.last().str.replace("^$", date.today().year).str.to_integer().alias("LetztesLebensjahr"))
data = data.drop("Lebensdaten")
print(data)

by_year = data.with_columns(pl.col("AktiveJahre")).explode("AktiveJahre")
with pl.Config(tbl_cols=by_year.width):
    print(by_year)
by_year = by_year.with_columns(pl.col("AktiveJahre").sub(pl.col("Geburtsjahr")).alias("Alter"))
print(by_year["Alter"].describe())
group_by_years = by_year.group_by("AktiveJahre").agg(pl.col("Alter").mean().alias("DurchschnittsAlter"))
group_by_years = group_by_years.rename({ "AktiveJahre": "Jahr"})
print(group_by_years.sort("DurchschnittsAlter"))

fig = px.bar(group_by_years, x="Jahr", y="DurchschnittsAlter", title="Wide-Form Input")
fig.show()