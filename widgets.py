import pandas as pd
import plotly.graph_objects as go
from dash_bootstrap_templates import ThemeSwitchAIO
from dash_bootstrap_templates import load_figure_template
import dash_bootstrap_components as dbc

import initial_values
import functions
import json

# select the Bootstrap stylesheet2 and figure template2 for the theme toggle here:
# template_theme1 = "sketchy"
template_theme1 = "flatly"
template_theme2 = "darkly"
# url_theme1 = dbc.themes.SKETCHY
url_theme1 = dbc.themes.FLATLY
url_theme2 = dbc.themes.DARKLY

templates = [
    "bootstrap",
    "minty",
    "pulse",
    "flatly",
    "quartz",
    "cyborg",
    "darkly",
    "vapor",
    "sandstone"
]

load_figure_template(templates)

dbc_css = (
    "https://cdn.jsdelivr.net/gh/AnnMarieW/dash-bootstrap-templates@V1.0.1/dbc.min.css"
)
 
################# График простоев по месяцам за три года ###############################
def widgets_data(theme_selector):
  """Данные для графиков"""

  downtime_graph_data = pd.read_csv('widget_data/downtime_graph_data.csv')
  x_month_year = downtime_graph_data['Период']
  y_downtime = downtime_graph_data['Запланированный простой, час']
  text_list_downtime_month_year = downtime_graph_data['Запланированный простой, час']
 
  if theme_selector:
      graph_template = 'seaborn'
  # bootstrap

  else:
      graph_template = 'plotly_dark'

  fig_downtime = go.Figure()
  fig_downtime.add_trace(go.Bar(
    name="Простои",
    x=x_month_year, 
    y=y_downtime,
    # xperiod="M1",
    # xperiodalignment="middle",
    #textposition='auto'
    ))
 

  fig_downtime.update_xaxes(
    showgrid=False, 
    # ticklabelmode="period"
  )
  fig_downtime.update_traces(
    text = text_list_downtime_month_year,
    textposition='auto'
  )
  fig_downtime.update_layout(
    title_text='Запланированный простой по месяцам за 3 года, час',
    template=graph_template,
    )
  
  ########################### ГРАФИК КТГ ###############################
  ktg_graph_data = pd.read_csv('widget_data/ktg_graph_data.csv')
  x_month_year_ktg = ktg_graph_data['Период']
  y_ktg = ktg_graph_data['КТГ']
  text_list_ktg_month_year = ktg_graph_data['КТГ']
  fig_ktg = go.Figure()
  fig_ktg.add_trace(go.Bar(
    name="КТГ",
    x=x_month_year_ktg, 
    y=y_ktg,
    # xperiod="M1",
    # xperiodalignment="middle",
    #textposition='auto'
    ))
  

  fig_ktg.update_xaxes(
    showgrid=False, 
    # ticklabelmode="period"
  )
  fig_ktg.update_traces(
    text = text_list_ktg_month_year,
    textposition='auto'
  )
  fig_ktg.update_layout(
    title_text='Запланированный КТГ по месяцам за 3 года',
    template=graph_template,
    )

  
  return fig_downtime, fig_ktg


  
  

# widgets_data(True, ['first11'])