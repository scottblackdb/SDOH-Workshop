# Databricks notebook source
spark.conf.set("fs.azure.account.key","konAi2KC83VSXKwUT9OCC4Dt/FEfsRkTlRkyweAunZ0UM8S3kxKNUMdPNzQLVZNKeH/Z3WHpLJe7+ASthBztjg==")

# COMMAND ----------

# MAGIC %pip install ipyleaflet

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sdoh.usa_model_county_vaccine_shap

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC 	select fips, abs(population_density) population_density,	
# MAGIC abs(Minoirity_Population_Pct) Minoirity_Population_Pct,	
# MAGIC abs(income)income,	
# MAGIC abs(All_Ages_in_Poverty_Percent)All_Ages_in_Poverty_Percent	,	
# MAGIC abs(25PlusHSPct)25PlusHSPct	,	
# MAGIC abs(25PlusAssociatePct)25PlusAssociatePct	,	
# MAGIC abs(SmokingPct)	SmokingPct,	
# MAGIC abs(ObesityPct)ObesityPct	,	
# MAGIC abs(HeartDiseasePct)HeartDiseasePct	,	
# MAGIC abs(CancerPct)CancerPct	,	
# MAGIC abs(NoHealthInsPct)NoHealthInsPct	,	
# MAGIC abs(AsthmaPct) AsthmaPct
# MAGIC     from sdoh.usa_model_county_vaccine_shap

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create or replace temp view shap_values as
# MAGIC 	select fips, abs(population_density) population_density,	
# MAGIC abs(Minoirity_Population_Pct) Minoirity_Population_Pct,	
# MAGIC abs(income)income	,	
# MAGIC abs(All_Ages_in_Poverty_Percent)All_Ages_in_Poverty_Percent	,	
# MAGIC abs(25PlusHSPct)25PlusHSPct	,	
# MAGIC abs(25PlusAssociatePct)25PlusAssociatePct	,	
# MAGIC abs(SmokingPct)	SmokingPct,	
# MAGIC abs(ObesityPct)ObesityPct	,	
# MAGIC abs(HeartDiseasePct)HeartDiseasePct	,	
# MAGIC abs(CancerPct)CancerPct	,	
# MAGIC abs(NoHealthInsPct)NoHealthInsPct	,	
# MAGIC abs(AsthmaPct) AsthmaPct
# MAGIC     from sdoh.state_model_county_vaccine_shap

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create or replace temp view county_abs_deter as
# MAGIC select fips, stack(12,'Minority_Population_Pct',Minoirity_Population_Pct,'income', income, '25PlusHSPct', 25PlusHSPct,
# MAGIC 'All_Ages_in_Poverty_Percent',All_Ages_in_Poverty_Percent,'population_density', population_density, '25PlusAssociatePct', 25PlusAssociatePct, 
# MAGIC 'SmokingPct',SmokingPct, 
# MAGIC 'ObesityPct', ObesityPct, 
# MAGIC 'HeartDiseasePct', HeartDiseasePct, 
# MAGIC 'CancerPct', CancerPct, 
# MAGIC 'NoHealthInsPct', NoHealthInsPct,
# MAGIC 'AsthmaPct', AsthmaPct
# MAGIC )  
# MAGIC as (factor, value)
# MAGIC from shap_values

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from sdoh.gold_fips

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create or replace temp view state_agg_det_values as
# MAGIC select f.fips, state, factor, sum(abs_value) det_total 
# MAGIC from sdoh.gold_fips f join sdoh.usa_model_county_vaccine_shap u on (f.fips = u.fips)
# MAGIC group by f.fips, state, factor

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create or replace temp view state_top_det as
# MAGIC select state, max(det_total) top_det
# MAGIC from (
# MAGIC select f.fips, state, factor, sum(abs_value) det_total 
# MAGIC from sdoh.gold_fips f join sdoh.usa_model_county_vaccine_shap u on (f.fips = u.fips)
# MAGIC group by f.fips, state, factor)
# MAGIC group by state

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select s.state, round(top_det) value, factor, case factor when 'AsthmaPct' then 1 when 'NoHealthInsPct' then 2 
# MAGIC when 'population_density' then 3 when 'income' then 4 when 'B25010_001E' then 5 when 'ObesityPct' then 6 when '25PlusAssociatePct' then 7 when 'SmokingPct' then 8 end vvalue
# MAGIC from state_top_det s join state_agg_det_values d on (s.state = d.state) and (s.top_det = d.det_total)

# COMMAND ----------

import pandas as pd

df = spark.sql("""select s.state, round(top_det) value, factor, case factor when 'AsthmaPct' then 1 when 'NoHealthInsPct' then 2 
when 'population_density' then 3 when 'income' then 4 when 'B25010_001E' then 5 when 'ObesityPct' then 6 when '25PlusAssociatePct' then 7 when 'SmokingPct' then 8 end vvalue
from state_top_det s join state_agg_det_values d on (s.state = d.state) and (s.top_det = d.det_total)""").toPandas()

df

# COMMAND ----------

import ipyleaflet
import ipywidgets as ipyw
import json
import pandas as pd
import os
import requests
from ipywidgets import link, FloatSlider
from branca.colormap import linear

def load_data(url, filename, file_type):
    r = requests.get(url)
    with open(filename, 'w') as f:
        f.write(r.content.decode("utf-8"))
    with open(filename, 'r') as f:
        return file_type(f)

geo_json_data = load_data(
    'https://raw.githubusercontent.com/jupyter-widgets/ipyleaflet/master/examples/us-states.json',
    'us-states.json',
     json.load)

unemployment = load_data(
    'https://raw.githubusercontent.com/jupyter-widgets/ipyleaflet/master/examples/US_Unemployment_Oct2012.csv',
    'US_Unemployment_Oct2012.csv',
     pd.read_csv)

unemployment =  dict(zip(unemployment['State'].tolist(), unemployment['Unemployment'].tolist()))

label = ipyw.Label(layout=ipyw.Layout(width="100%"))

layer = ipyleaflet.Choropleth(
    geo_data=geo_json_data,
    choro_data=b,
    colormap=linear.YlOrRd_04,
    border_color='black',
    style={'fillOpacity': 0.8, 'dashArray': '5, 5'},
    key_on="id",
    hover_style={
                            'dashArray': '1', 'fillOpacity': 0.1
                        },
)

m = ipyleaflet.Map(center = (43,-100), zoom = 4)

def hover_handler(event=None, feature=None, id=None, properties=None):
    label.value = b[id]


layer.on_hover(hover_handler)

m.add_layer(layer)
#m
ipyw.VBox([m, label])

# COMMAND ----------

df = df.drop(['value'], axis=1)



# COMMAND ----------

d = df.to_dict('split')
x = d['data']
x

# COMMAND ----------

b = {}
for a in x:
  b[a[0]] = a[1]

b['AK'] = 0
b

# COMMAND ----------

# MAGIC %pip install folium

# COMMAND ----------

import folium
import pandas as pd

url = (
      "https://raw.githubusercontent.com/python-visualization/folium/master/examples/data"
  )

state_geo = f"{url}/us-states.json"
state_unemployment = f"{url}/US_Unemployment_Oct2012.csv"
state_data = pd.read_csv(state_unemployment)

m = folium.Map(location=[48, -102], zoom_start=3)

# capturing the return of folium.Choropleth()
cp = folium.Choropleth(
    geo_data=state_geo,
    name="choropleth",
    data=state_data,
    columns=["State", "Unemployment"],
    key_on="feature.id",
    fill_color="YlGn",
    fill_opacity=0.7,
    line_opacity=0.2,
    legend_name="Unemployment Rate (%)",
).add_to(m)

# creating a state indexed version of the dataframe so we can lookup values
state_data_indexed = state_data.set_index('State')

# looping thru the geojson object and adding a new property(unemployment)
# and assigning a value from our dataframe
for s in cp.geojson.data['features']:
    s['properties']['unemployment'] = state_data_indexed.loc[s['id'], 'Unemployment']

# and finally adding a tooltip/hover to the choropleth's geojson
folium.GeoJsonTooltip(['name', 'unemployment']).add_to(cp.geojson)

folium.LayerControl().add_to(m)

m

# COMMAND ----------

import folium
import pandas as pd

url = (
      "https://raw.githubusercontent.com/python-visualization/folium/master/examples/data"
  )
    
state_geo = f"{url}/us-states.json"
#state_unemployment = f"{url}/US_Unemployment_Oct2012.csv"
#state_data = pd.read_csv(state_unemployment)

m = folium.Map(location=[48, -102], zoom_start=3)

# capturing the return of folium.Choropleth()
cp = folium.Choropleth(
    geo_data=state_geo,
    name="choropleth",
    data=df,
    columns=["state","vvalue","factor"],
    key_on="feature.id",
    fill_color="YlGn",
    fill_opacity=0.7,
    line_opacity=0.2,
    legend_name="Unemployment Rate (%)",
).add_to(m)

# creating a state indexed version of the dataframe so we can lookup values
state_data_indexed = df.set_index('state')

# looping thru the geojson object and adding a new property(unemployment)
# and assigning a value from our dataframe
for s in cp.geojson.data['features']:
  if s['id'] != 'AK':
    s['properties']['factor'] = state_data_indexed.loc[s['id'], 'factor']

# and finally adding a tooltip/hover to the choropleth's geojson
folium.GeoJsonTooltip(['name','factor']).add_to(cp.geojson)

folium.LayerControl().add_to(m)

m

# COMMAND ----------

df

# COMMAND ----------

import os
import json
import random
import requests

from ipyleaflet import Map, GeoJSON

if not os.path.exists('europe_110.geo.json'):
    url = 'https://github.com/jupyter-widgets/ipyleaflet/raw/master/examples/europe_110.geo.json'
    r = requests.get(url)
    with open('europe_110.geo.json', 'w') as f:
        f.write(r.content.decode("utf-8"))

with open('europe_110.geo.json', 'r') as f:
    data = json.load(f)

def random_color(feature):
    return {
        'color': 'black',
        'fillColor': random.choice(['red', 'yellow', 'green', 'orange']),
    }

m = Map(center=(50.6252978589571, 0.34580993652344), zoom=3)

geo_json = GeoJSON(
    data=data,
    style={
        'opacity': 1, 'dashArray': '9', 'fillOpacity': 0.1, 'weight': 1
    },
    hover_style={
        'color': 'white', 'dashArray': '0', 'fillOpacity': 0.5
    },
    style_callback=random_color
)
m.add_layer(geo_json)

m

# COMMAND ----------

state_data

# COMMAND ----------

df

# COMMAND ----------


