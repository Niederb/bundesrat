# Bundesrat

## Data sources

<https://en.wikipedia.org/wiki/List_of_members_of_the_Swiss_Federal_Council>
<https://de.wikipedia.org/wiki/Liste_der_Mitglieder_des_Schweizerischen_Bundesrates>
<https://www.admin.ch/gov/de/start/bundesrat/geschichte-des-bundesrats/bundesraete-und-ihre-wahl/alle-bundesraete-liste.html>

## Preprocessing

I originally took the data from [admin.ch](https://www.admin.ch/gov/de/start/bundesrat/geschichte-des-bundesrats/bundesraete-und-ihre-wahl/alle-bundesraete-liste.html). To this I added manually a column for the gender.

## Data analysis

For this I wanted to play around with a data frame crate. After searching around I decided to give [Polars](https://crates.io/crates/polars) a try. Polars is using Apache Arrow in the background.

### Loading the data

My data file is a csv and luckily Polars provides a easy method to load a csv file. 

After loading the data we can do some simple analysis and for example count how many federal 

### Plotting

### Ideas

- In which year were the most members active?
- In which month are their birthdays?