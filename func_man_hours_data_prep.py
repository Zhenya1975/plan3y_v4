import pandas as pd
import initial_values
import plotly.graph_objects as go

def man_hours_data_pre(theme_selector, be_list_for_dataframes_filtering, model_eo_filter_list_for_dataframes_filtering):
  man_hours_raw_data_df = pd.read_csv('data/man_hours_raw_data_df.csv')
  man_hours_raw_data_df = man_hours_raw_data_df.loc[man_hours_raw_data_df['level_1'].isin(be_list_for_dataframes_filtering) &
  man_hours_raw_data_df['eo_model_id'].isin(model_eo_filter_list_for_dataframes_filtering)
  ]
  man_hours_raw_data_df.drop(labels='level_1', axis=1)


  period_dict = initial_values.period_dict
  period_sort_index = initial_values.period_sort_index
  man_hours_raw_data_df['period'] = man_hours_raw_data_df['month_year'].map(period_dict).astype(str)
  man_hours_raw_data_df['period_sort_index'] = man_hours_raw_data_df['month_year'].map(period_sort_index)
  
  man_hours_raw_data = man_hours_raw_data_df.groupby(['month_year', 'period', 'period_sort_index'], as_index = False)['man_hours'].sum()
  man_hours_raw_data['man_hours'] = man_hours_raw_data['man_hours'].apply(lambda x: round(x, 1))
  man_hours_raw_data['period_sort_index'] = man_hours_raw_data['period_sort_index'].astype(int)
  man_hours_raw_data.sort_values(by='period_sort_index', inplace = True)
  
  
  man_hours_raw_data.rename(columns={'period': 'Период', 'man_hours': "Трудозатраты, чел-час"}, inplace=True)
  man_hours_raw_data.to_csv('data/man_hours_raw_data_2.csv')
  man_hours_graph_data = man_hours_raw_data.loc[:, ['Период', "Трудозатраты, чел-час"]]
  # print(man_hours_graph_data)

  x_month_year = man_hours_graph_data['Период']
  y_man_hours = man_hours_graph_data['Трудозатраты, чел-час']
  text_list_man_hours_month_year = man_hours_graph_data['Трудозатраты, чел-час']
 
  if theme_selector:
      graph_template = 'seaborn'
  # bootstrap

  else:
      graph_template = 'plotly_dark'

  fig_man_hours = go.Figure()
  fig_man_hours.add_trace(go.Bar(
    name="Трудозатраты, чел-час",
    x=x_month_year, 
    y=y_man_hours,
    # xperiod="M1",
    # xperiodalignment="middle",
    #textposition='auto'
    ))
  # new_year_2022_2023 = pd.to_datetime('01.01.2024', format='%d.%m.%Y')
  # new_year_2023_2024 = pd.to_datetime('01.01.2025', format='%d.%m.%Y')
  # fig_downtime.add_vline(x=new_year_2022_2023, line_width=3, line_color="green")
  # fig_downtime.add_vline(x=new_year_2023_2024, line_width=3, line_color="green")

  fig_man_hours.update_xaxes(
    showgrid=False, 
    # ticklabelmode="period"
  )
  fig_man_hours.update_traces(
    text = text_list_man_hours_month_year,
    textposition='auto'
  )
  fig_man_hours.update_layout(
    title_text='Запланированные трудозатраты, чел-час',
    template=graph_template,
    )
  return fig_man_hours

  
# man_hours_data_pre(True, ["first11"])  