from datetime import date
import polars as pl

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
data = data.with_columns(
    pl.col("Retired").fill_null(today),
)
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


only_dead = data.filter(
    pl.col("Lebensdaten").str.contains("\*") == False,
)

data = data.with_columns(pl.col("Amtsjahre").str.split("–"))
data = data.with_columns(pl.col("Amtsjahre").list.first().str.to_integer().alias("ErstesAmtsjahr"))
data = data.with_columns(pl.col("Amtsjahre").list.last().str.replace("^$", date.today().year).str.to_integer().alias("LetztesAmtsjahr"))
data = data.drop("Amtsjahre")
data = data.with_columns(pl.int_ranges("ErstesAmtsjahr", "LetztesAmtsjahr").alias("AktiveJahre"))
data = data.with_columns(pl.col("AktiveJahre")).explode("AktiveJahre")
years = data.group_by("AktiveJahre").count().sort("count", descending=True)
print(data)
print(years)

#only_dead = only_dead.with_columns(pl.col("Lebensdaten").str.split("–").alias("Lebensjahre"))
#only_dead = only_dead.with_columns(pl.col("Lebensjahre").list.first().alias("Geburtsjahr").str.to_integer())
#only_dead = only_dead.with_columns(pl.col("Lebensjahre").list.first().alias("Todesjahr").str.to_integer())
#only_dead = only_dead.drop("Lebensjahre")
#only_dead = only_dead.drop("Lebensdaten")
#print(only_dead)
