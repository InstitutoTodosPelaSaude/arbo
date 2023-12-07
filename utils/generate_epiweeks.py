from epiweeks import Week, Year
import pandas as pd


years = [2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026, 2027]
epiweeks = []

for week in Year(2019).iterweeks():
    print(week.enddate(), week.startdate())

for year in years:
    for _, week in enumerate(Year(year).iterweeks()):
        week_num = week.isoformat()[-2:]
        epiweeks.append( (year, week_num, week.enddate(), week.startdate()) ) 
        print(week_num, week.enddate(), week.startdate())


df_epiweeks = pd.DataFrame(epiweeks, columns=['year', 'week_num', 'end_date', 'start_date'])

df_epiweeks.to_csv('epiweeks.csv', index=False)