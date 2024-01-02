from datetime import date
import polars as pl
import plotly.express as px

print(pl.__version__)


def markdown_export(data, file):
    print(file)
    print(data)
    with open('export/' + file + '.md', "w") as markdown_export:
        # Export the Polars DataFrame to a Markdown table
        markdown = data.to_pandas().to_markdown(index=False)
        markdown_export.write(markdown)
    with open('export/' + file + '.typ', "w") as typst_export:
        typst_export.write('#table(\n')
        typst_export.write('columns: (')
        for _ in data.columns:
            typst_export.write('auto, ')
        typst_export.write('), \n')
        typst_export.write('inset: 10pt,\n')
        typst_export.write('align: horizon,\n')
        for c in data.columns:
            typst_export.write(f'[*{c}*], ')
        for row in data.iter_rows():
            for cell in row:
                typst_export.write(f'[{cell}], ')
            typst_export.write(f'\n')
        typst_export.write(')\n')

def read_data():
    admin_ch = pl.read_csv("bundesrat.csv", separator=";")
    # Convert the names from the format with "lastname, firstname" (with commas) to "firstname lastname"
    # This is done to allow the join with wikipedia
    admin_ch = admin_ch.with_columns(pl.col("Name").str.split(", ").list.reverse().list.join(" "))
    admin_ch = admin_ch.with_columns(pl.col("FirstDayInOffice").str.to_date("%d.%m.%Y"))

    wikipedia_de = pl.read_csv("bundesrat-wikipedia-de.csv", separator="\t")
    data = admin_ch.join(wikipedia_de, on="Name", validate="1:1")
    assert data.height == admin_ch.height
    assert data.height == wikipedia_de.height

    wikipedia_es= pl.read_csv("bundesrat-wikipedia-es.csv", separator="\t")
    #with pl.Config(tbl_cols=wikipedia_es.width, tbl_rows=wikipedia_es.height):
    #    print(wikipedia_es)
    wikipedia_es = wikipedia_es.with_columns(pl.col("Nacido el").str.to_date("%d.%m.%Y"))
    wikipedia_es = wikipedia_es.with_columns(pl.col("Fallecido el").str.to_date("%d.%m.%Y"))
    wikipedia_es = wikipedia_es.rename({ "N°": "Nummer", "Nacido el": "DateOfBirth", "Fallecido el": "DateOfDeath"})
    wikipedia_es = wikipedia_es.select(["Nummer", "DateOfBirth", "DateOfDeath"])
    data = data.join(wikipedia_es, on="Nummer", validate="1:1")
    assert data.height == wikipedia_de.height
    assert data.height == wikipedia_es.height

    # Convert dates
    data = data.with_columns(pl.col("Elected").str.to_date("%d.%m.%Y"))
    data = data.with_columns(pl.col("Retired").str.to_date("%d.%m.%Y"))
    return data

def extract_amtsjahre(data):
    data = data.with_columns(pl.col("Amtsjahre").str.split("–"))
    data = data.with_columns(pl.col("Amtsjahre").list.first().str.to_integer().alias("ErstesAmtsjahr"))
    data = data.with_columns(pl.col("Amtsjahre").list.last().str.replace("^$", date.today().year).str.to_integer().alias("LetztesAmtsjahr"))
    data = data.drop("Amtsjahre")
    data = data.with_columns(pl.int_ranges("ErstesAmtsjahr", pl.col("LetztesAmtsjahr") + 1).alias("AktiveJahre"))
    return data

def extract_living_years(data):
    # Fill in the missing data for the years alive (use current year for people that are still alive)
    data = data.with_columns(pl.col("Lebensdaten").str.split("–"))
    data = data.with_columns(pl.col("Lebensdaten").list.first().alias("Geburtsjahr").str.to_integer())
    data = data.with_columns(pl.col("Lebensdaten").list.last().str.replace("^$", date.today().year).str.to_integer().alias("LetztesLebensjahr"))
    data = data.drop("Lebensdaten")
    data = data.drop("Geburtsjahr")
    return data

def sanity_checks(data):
    # All should be "retired". Active ones retired today
    assert data.select(pl.col("Retired")).null_count().item() == 0
    # Sanity check: No one should retire before being elected
    assert data.filter(pl.col("Retired") < pl.col("Elected")).height == 0
    # Sanity check: No one should be elected before being born
    assert data.filter(pl.col("Elected") < pl.col("DateOfBirth")).height == 0

data = read_data()

# Make sure that exactly seven members are currently active (have not resigned)
assert data.select(pl.col("Retired")).null_count().item() == 7
today = pl.lit(date.today())
# For currently active members set their Retired value to today for the purpose of this analysis
data = data.with_columns(pl.col("Retired").fill_null(today))

data = extract_amtsjahre(data)
data = extract_living_years(data)
print(f"Total members in the council: {data.height}")

sanity_checks(data)

def analysis_days_in_office(data):
    data = data.with_columns((pl.col("Retired") - pl.col("FirstDayInOffice")).alias("DaysInOffice"))
    markdown_export(data.sort("DaysInOffice", descending=True), "days_in_office")
    print(data.select(pl.col("DaysInOffice")).describe())

def analysis_party(data):
    markdown_export(data.group_by(by="Party").count().sort("count", descending=True), "party")

def analysis_sex(data):
    markdown_export(data.group_by(by="Sex").count().sort("count", descending=True), "sex")

def analysis_cantons(data):
    by_canton = data.group_by(by="Kanton").count().sort("count", descending=True)
    markdown_export(by_canton, "cantons")
    print(f"Cantons without federal council: {26 - by_canton.height}")

def analysis_month_elected(data):
    data = data.with_columns(pl.col("Elected").dt.month().alias("MonthElected"))
    markdown_export(data.group_by(by="MonthElected").count().sort("count", descending=True), "month_elected")

def analysis_month_born(data):
    data = data.with_columns(pl.col("DateOfBirth").dt.month().alias("MonthBorn"))
    print(data.group_by(by="MonthBorn").count().sort("count", descending=True))

def analysis_election_day(data):
    by_election_date = data.group_by(by="Elected").count()
    markdown_export(by_election_date.sort("count", descending=True), "most_elected_single_day")

    print("Most Elections in a year")
    most_elections_year = by_election_date.group_by([pl.col("Elected").dt.year()]).count().sort("count", descending=True)
    markdown_export(most_elections_year, "most_elections_in_a_year")

def analysis_average_age(data):
    by_year = data.with_columns(pl.col("AktiveJahre")).explode("AktiveJahre")
    print(by_year.group_by(by="AktiveJahre").count().sort("count", descending=True))
    by_year = by_year.with_columns(pl.concat_str([pl.lit("1.1.").alias("FirstDay"), pl.col("AktiveJahre").cast(pl.Utf8)]).alias("AktiveJahre"))
    by_year = by_year.with_columns(pl.col("AktiveJahre").str.to_date("%d.%m.%Y"))

    # TODO: Find way to calculate age properly with considering leap years
    by_year = by_year.with_columns(pl.col("AktiveJahre").sub(pl.col("DateOfBirth")).dt.total_days().truediv(365).alias("Alter"))
    #with pl.Config(tbl_cols=by_year.width, tbl_rows=by_year.height):
    #    print(by_year)
    print(by_year["Alter"].describe())
    group_by_years = by_year.group_by("AktiveJahre").agg(
        pl.col("Alter").mean().alias("DurchschnittsAlter"), 
        pl.col("Alter").max().alias("MaxAlter"),
        pl.col("Alter").min().alias("MinAlter"))
    group_by_years = group_by_years.rename({ "AktiveJahre": "JahrDatum"})
    group_by_years = group_by_years.with_columns(pl.col("JahrDatum").dt.year().alias("Jahr"))
    #print(group_by_years.sort("DurchschnittsAlter"))

    #with pl.Config(tbl_cols=group_by_years.width, tbl_rows=group_by_years.height):
    #    print(group_by_years)
    fig = px.bar(group_by_years, x="Jahr", y=["MaxAlter", "DurchschnittsAlter", "MinAlter"], title="Durchschnittsalter pro Jahr", barmode='overlay', opacity=1.0)
    #fig.show()
    fig.write_image("plots/Durchschnittsalter.png", width=1000)

def analysis_list_of_women(data):
    data = data.filter(pl.col("Sex") == 'W').select(['Name'])
    markdown_export(data, 'list_of_women')

def analysis_death_in_office(data):
    markdown_export(data.filter(pl.col("DateOfDeath").is_null()), "still_alive")
    markdown_export(data.filter(pl.col("DateOfDeath") == pl.col("Retired")).select(['Name']), "died_in_office")

analysis_days_in_office(data)
analysis_party(data)
analysis_sex(data)
analysis_cantons(data)
analysis_month_elected(data)
analysis_month_born(data)
analysis_average_age(data)
analysis_election_day(data)
analysis_list_of_women(data)
analysis_death_in_office(data)

markdown_export(data, 'complete_data')

with pl.Config(tbl_cols=data.width, tbl_rows=data.height):
    print(data)
