import polars as pl

s = pl.Series("a", [1, 2, 3, 4, 5])
print(s)

data = pl.read_csv("bundesrat.csv", separator=";")

print(f"Total members in the council: {data.height}")

group_by_party = data.group_by(by="Party")
print(group_by_party.count())

group_by_sex = data.group_by(by="Sex")
print(group_by_sex.count())