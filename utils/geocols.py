import geopandas as gpd
import re

# data from 
# https://www.ibge.gov.br/geociencias/organizacao-do-territorio/malhas-territoriais.html
gdf = gpd.read_file('BR_Municipios_2022.zip')

gdf = gdf[[
    'CD_MUN',
    'NM_MUN',
    'SIGLA_UF',
    'geometry'
]]


# transform geometry in the centroid of the polygon

gdf['geometry'] = gdf['geometry'].centroid

# To CRS
gdf = gdf.to_crs(epsg=4291)

# transform into lat and long
gdf['lat'] = gdf['geometry'].y
gdf['long'] = gdf['geometry'].x

# Drop geometry column
gdf = gdf.drop('geometry', axis=1)

# normalize NM_MUN
# UPPERCASE
# remove accents
# remove special characters

gdf['NM_MUN_NORM'] = gdf['NM_MUN'].str.upper()
gdf['NM_MUN_NORM'] = gdf['NM_MUN_NORM'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
gdf['NM_MUN_NORM'] = gdf['NM_MUN_NORM'].str.replace(re.compile(r'[^a-zA-Z0-9 ]'), '', regex=True)

gdf = gdf[
    [
        'CD_MUN',
        'SIGLA_UF',
        'NM_MUN',
        'NM_MUN_NORM',
        'lat',
        'long'
    ]
]

# save to csv
gdf.to_csv('municipios.csv', index=False)