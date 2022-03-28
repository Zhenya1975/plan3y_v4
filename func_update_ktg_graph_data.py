import pandas as pd
import initial_values

def update_downtime_graph_data(be_list_for_dataframes_filtering):
  ktg_by_month_data = pd.read_csv('widget_data/ktg_by_month_data_df.csv', decimal = ",")
    # режем по фильтру
  
  ktg_by_month_data_filtered = ktg_by_month_data.loc[ktg_by_month_data['level_1'].isin(be_list_for_dataframes_filtering)]
  ktg_by_month_data_filtered = ktg_by_month_data_filtered.copy()
  ktg_by_month_data_filtered['month_year'] = ktg_by_month_data_filtered['month'].astype(str) + "_" + ktg_by_month_data_filtered['year'].astype(str)
  
  # ktg_by_month_data_filtered.loc[:, ['month_year']] = ktg_by_month_data_filtered.loc[:, ['month']].astype(str) + "_" + ktg_by_month_data_filtered.loc[:, ['year']].astype(str)
  
  ########### ГРУППИРОВКА  #####################
  downtime_ktg_graph_data = ktg_by_month_data_filtered.groupby(['month_year'], as_index=False)[['calendar_fond','downtime']].sum()
  
  
  downtime_ktg_graph_data['ktg'] = (downtime_ktg_graph_data['calendar_fond'] - downtime_ktg_graph_data['downtime']) / downtime_ktg_graph_data['calendar_fond']
  downtime_ktg_graph_data['downtime'] = downtime_ktg_graph_data['downtime'].apply(lambda x: round(x, 0))
  downtime_ktg_graph_data['ktg'] = downtime_ktg_graph_data['ktg'].apply(lambda x: round(x, 2))
  period_dict = initial_values.period_dict
    
  period_sort_index = initial_values.period_sort_index
  
  downtime_ktg_graph_data['period'] = downtime_ktg_graph_data['month_year'].map(period_dict).astype(str)
  
  downtime_ktg_graph_data['period_sort_index'] = downtime_ktg_graph_data['month_year'].map(period_sort_index)
  downtime_ktg_graph_data.sort_values(by='period_sort_index', inplace = True)
  
  # downtime_graph_data['downtime'] = downtime_graph_data['downtime'].astype(int)
  # ktg_graph_data['downtime'] = ktg_graph_data['downtime'].apply(lambda x: round(x, 0))
  
  downtime_ktg_graph_data.rename(columns={'period': 'Период', 'downtime': "Запланированный простой, час", "ktg": "КТГ"}, inplace=True)
  
  downtime_graph_data = downtime_ktg_graph_data.loc[:, ['Период', 'Запланированный простой, час']]
  downtime_graph_data.to_csv('widget_data/downtime_graph_data.csv')


# update_downtime_graph_data(['first11', 'first06'])