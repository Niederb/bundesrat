import polars as pl

data = pl.read_csv("bundesrat.csv", separator=";", try_parse_dates=True)

data = data.with_columns(pl.col("Elected").str.to_date("%d.%m.%Y"))
data = data.with_columns(pl.col("Elected").dt.month().alias("MonthElected"))
data = data.with_columns(pl.col("Retired").str.to_date("%d.%m.%Y"))

data = data.with_columns((pl.col("Retired") - pl.col("Elected")).alias("Term"))

# Make sure that exactly seven members are currently active (have not resigned)
assert data.filter(pl.col("Term").is_null()).height == 7
print(data.filter(pl.col("Term").is_not_null()).sort("Term", descending=True))
print(f"Total members in the council: {data.height}")
print(data.select(pl.col("Term")).describe())

print(data.group_by(by="Party").count().sort("count", descending=True))

print(data.group_by(by="Sex").count().sort("count", descending=True))

by_canton = data.group_by(by="Kanton").count().sort("count", descending=True)
print(by_canton)
print(f"Cantons without federal council: {26 - by_canton.height}")

print(data.group_by(by="MonthElected").count().sort("count", descending=True))