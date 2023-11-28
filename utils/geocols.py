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

sigla_to_uf_name = {
    'AC': 'Acre',
    'AL': 'Alagoas',
    'AP': 'Amapá',
    'AM': 'Amazonas',
    'BA': 'Bahia',
    'CE': 'Ceará',
    'DF': 'Distrito Federal',
    'ES': 'Espírito Santo',
    'GO': 'Goiás',
    'MA': 'Maranhão',
    'MT': 'Mato Grosso',
    'MS': 'Mato Grosso do Sul',
    'MG': 'Minas Gerais',
    'PA': 'Pará',
    'PB': 'Paraíba',
    'PR': 'Paraná',
    'PE': 'Pernambuco',
    'PI': 'Piauí',
    'RJ': 'Rio de Janeiro',
    'RN': 'Rio Grande do Norte',
    'RS': 'Rio Grande do Sul',
    'RO': 'Rondônia',
    'RR': 'Roraima',
    'SC': 'Santa Catarina',
    'SP': 'São Paulo',
    'SE': 'Sergipe',
    'TO': 'Tocantins'
}

sigla_uf_to_region = {
    'AC': 'NORTE',
    'AL': 'NORDESTE',
    'AP': 'NORTE',
    'AM': 'NORTE',
    'BA': 'NORDESTE',
    'CE': 'NORDESTE',
    'DF': 'CENTRO-OESTE',
    'ES': 'SUDESTE',
    'GO': 'CENTRO-OESTE',
    'MA': 'NORDESTE',
    'MT': 'CENTRO-OESTE',
    'MS': 'CENTRO-OESTE',
    'MG': 'SUDESTE',
    'PA': 'NORTE',
    'PB': 'NORDESTE',
    'PR': 'SUL',
    'PE': 'NORDESTE',
    'PI': 'NORDESTE',
    'RJ': 'SUDESTE',
    'RN': 'NORDESTE',
    'RS': 'SUL',
    'RO': 'NORTE',
    'RR': 'NORTE',
    'SC': 'SUL',
    'SP': 'SUDESTE',
    'SE': 'NORDESTE',
    'TO': 'NORTE'
}

# https://www.ibge.gov.br/explica/codigos-dos-municipios.php
sigla_uf_to_uf_code = {
    'AC': 12,
    'AL': 27,
    'AP': 16,
    'AM': 13,
    'BA': 29,
    'CE': 23,
    'DF': 53,
    'ES': 32,
    'GO': 52,
    'MA': 21,
    'MT': 51,
    'MS': 50,
    'MG': 31,
    'PA': 15,
    'PB': 25,
    'PR': 41,
    'PE': 26,
    'PI': 22,
    'RJ': 33,
    'RN': 24,
    'RS': 43,
    'RO': 11,
    'RR': 14,
    'SC': 42,
    'SP': 35,
    'SE': 28,
    'TO': 17
}

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
gdf['NM_MUN_NORM'] = gdf['NM_MUN_NORM'].str.replace('-', ' ', regex=False)
gdf['NM_MUN_NORM'] = gdf['NM_MUN_NORM'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
gdf['NM_MUN_NORM'] = gdf['NM_MUN_NORM'].str.replace(re.compile(r'[^a-zA-Z0-9 ]'), '', regex=True)


gdf['NM_UF'] = gdf['SIGLA_UF'].map(sigla_to_uf_name)
gdf['NM_UF_NORM'] = gdf['NM_UF'].str.upper()
gdf['NM_UF_NORM'] = gdf['NM_UF_NORM'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
gdf['NM_UF_NORM'] = gdf['NM_UF_NORM'].str.replace(re.compile(r'[^a-zA-Z0-9 ]'), '', regex=True)

gdf['REGIAO'] = gdf['SIGLA_UF'].map(sigla_uf_to_region)

gdf['CD_UF'] = gdf['SIGLA_UF'].map(sigla_uf_to_uf_code)

gdf = gdf[
    [
        'CD_MUN',
        'CD_UF',
        'SIGLA_UF',
        'NM_UF',
        'NM_UF_NORM',
        'NM_MUN',
        'NM_MUN_NORM',
        'REGIAO',
        'lat',
        'long'
    ]
]

# save to csv
gdf.to_csv('municipios.csv', index=False)