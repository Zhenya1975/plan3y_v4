import pandas as pd
import initial_values

def update_downtime_graph_data(be_list_for_dataframes_filtering,model_eo_filter_list_for_dataframes_filtering):
  ktg_by_month_data = pd.read_csv('widget_data/ktg_by_month_data_df.csv', decimal = ",")
    # режем по фильтру
  

  ktg_by_month_data_filtered = ktg_by_month_data.loc[ktg_by_month_data['level_1'].isin(be_list_for_dataframes_filtering) &
  ktg_by_month_data['eo_model_id'].isin(model_eo_filter_list_for_dataframes_filtering)
  ]
  
  ktg_by_month_data_filtered = ktg_by_month_data_filtered.copy()
  ktg_by_month_data_filtered['month_year'] = ktg_by_month_data_filtered['month'].astype(str) + "_" + ktg_by_month_data_filtered['year'].astype(str)
  
  # ktg_by_month_data_filtered.loc[:, ['month_year']] = ktg_by_month_data_filtered.loc[:, ['month']].astype(str) + "_" + ktg_by_month_data_filtered.loc[:, ['year']].astype(str)
  
  ########### ГРУППИРОВКА  #####################
  downtime_ktg_graph_data_raw = ktg_by_month_data_filtered.groupby(['month_year', 'eo_model_name'], as_index=False)[['calendar_fond','downtime']].sum()

  downtime_ktg_graph_data_raw['ktg'] = (downtime_ktg_graph_data_raw['calendar_fond'] - downtime_ktg_graph_data_raw['downtime']) / downtime_ktg_graph_data_raw['calendar_fond']
  downtime_ktg_graph_data_raw['downtime'] = downtime_ktg_graph_data_raw['downtime'].apply(lambda x: round(x, 0))
  downtime_ktg_graph_data_raw['ktg'] = downtime_ktg_graph_data_raw['ktg'].apply(lambda x: round(x, 2))


  

  downtime_ktg_graph_data = downtime_ktg_graph_data_raw.groupby(['month_year'], as_index=False)[['calendar_fond','downtime']].sum()
  
  downtime_ktg_graph_data['ktg'] = (downtime_ktg_graph_data['calendar_fond'] - downtime_ktg_graph_data['downtime']) / downtime_ktg_graph_data['calendar_fond']
  downtime_ktg_graph_data['downtime'] = downtime_ktg_graph_data['downtime'].apply(lambda x: round(x, 0))
  downtime_ktg_graph_data['ktg'] = downtime_ktg_graph_data['ktg'].apply(lambda x: round(x, 2))
  period_dict = initial_values.period_dict
    
  period_sort_index = initial_values.period_sort_index
  
  downtime_ktg_graph_data['period'] = downtime_ktg_graph_data['month_year'].map(period_dict).astype(str)

  downtime_ktg_graph_data_raw['period'] = downtime_ktg_graph_data_raw['month_year'].map(period_dict).astype(str)
  
  
  downtime_ktg_graph_data['period_sort_index'] = downtime_ktg_graph_data['month_year'].map(period_sort_index)
  downtime_ktg_graph_data.sort_values(by='period_sort_index', inplace = True)

  downtime_ktg_graph_data_raw['period_sort_index'] = downtime_ktg_graph_data_raw['month_year'].map(period_sort_index)
  downtime_ktg_graph_data_raw.sort_values(by='period_sort_index', inplace = True)
  
  # downtime_graph_data['downtime'] = downtime_graph_data['downtime'].astype(int)
  # ktg_graph_data['downtime'] = ktg_graph_data['downtime'].apply(lambda x: round(x, 0))
  # print(downtime_ktg_graph_data_raw)

  
  downtime_ktg_graph_data.rename(columns={'period': 'Период', 'downtime': "Запланированный простой, час", "ktg": "КТГ"}, inplace=True)
  downtime_ktg_graph_data_raw.rename(columns={'period': 'Период', 'downtime': "Запланированный простой, час", "ktg": "КТГ"}, inplace=True)
  
  
  downtime_graph_data = downtime_ktg_graph_data.loc[:, ['Период', 'Запланированный простой, час']]
  ktg_graph_data = downtime_ktg_graph_data.loc[:, ['Период', 'КТГ']]
  downtime_graph_data.to_csv('widget_data/downtime_graph_data.csv')
  ktg_graph_data.to_csv('widget_data/ktg_graph_data.csv')


  ############ ТАБЛИЦА КТГ по группам моделей ##############
  # downtime_ktg_graph_data_raw.to_csv('widget_data/downtime_ktg_graph_data_raw.csv')
  
  eo_model_list = list(set(downtime_ktg_graph_data_raw['eo_model_name']))
  columns_list = initial_values.months_list
  columns_list = ['Модель ЕО'] + columns_list
  index_list = eo_model_list
  ktg_table_df = pd.DataFrame(columns=columns_list, index=index_list)
  # Сначала внешним циклом итерируемся по строкам таблицы - то есть по списку моделей ео
  for eo_model in eo_model_list:
    temp_dict = {}
    # делаем срез  - все записи текущей модели ео
    ktg_graph_data_selected = downtime_ktg_graph_data_raw.loc[downtime_ktg_graph_data_raw['eo_model_name'] == eo_model]
    
    temp_dict['Модель ЕО'] = eo_model
    # итерируемся по полученном временном срезу по модели
    for row in ktg_graph_data_selected.itertuples():
      month_year = getattr(row, 'month_year')
      ktg = getattr(row, 'КТГ')
      temp_dict[month_year] = ktg
    ktg_table_df.loc[eo_model] = pd.Series(temp_dict)
    
  ktg_table_df = ktg_table_df.rename(columns = initial_values.period_dict)
  ktg_table_df.index.name = 'Наименование модели ЕО'
  ktg_table_df.fillna(0, inplace = True)
  # ktg_table_df['Наименование модели'] = ktg_table_df.index
  # print(ktg_table_df)
  ktg_table_df.to_csv('widget_data/ktg_table_data.csv', index = False)


  
  ########## ГОДОВОЙ КТГ В КАРТОЧКУ #################
  ktg_year_data = ktg_by_month_data_filtered.groupby(['year'], as_index=False)[['calendar_fond','downtime']].sum()

  ktg_year_data['ktg'] = (ktg_year_data['calendar_fond'] - ktg_year_data['downtime']) / ktg_year_data['calendar_fond']
  ktg_year_data['ktg'] = ktg_year_data['ktg'].apply(lambda x: round(x, 2))

  ktg_2023 = ktg_year_data.loc[ktg_year_data['year']==2023].iloc[0]['ktg']
  ktg_2024 = ktg_year_data.loc[ktg_year_data['year']==2024].iloc[0]['ktg']
  ktg_2025 = ktg_year_data.loc[ktg_year_data['year']==2025].iloc[0]['ktg']

  ############## РАЗВЕРТКА Р11 ###############################
  ktg_by_month_data_df_columns = ktg_by_month_data_filtered.columns
  job_list = list(pd.read_csv('data/job_list.csv')['maintanance_category_id'])
  actual_job_list = list(set(ktg_by_month_data_df_columns).intersection(job_list))
  
  actual_job_list_df = pd.DataFrame(actual_job_list, columns = ['maintanance_category_sort_index'])
  
  job_codes_df = pd.read_csv('data/job_codes.csv')
  
  column_list_groupping = ['downtime'] + actual_job_list

  # группируем в eo_main_class_description
  p11_raw_data = ktg_by_month_data_filtered.groupby(['eo_main_class_description', 'eo_model_name', 'year', 'month'], as_index = False)[column_list_groupping].sum()
  p11_raw_data['downtime'] = p11_raw_data['downtime'].apply(lambda x: round(x, 1))

  p11_raw_data.rename(columns=initial_values.rename_columns_dict, inplace=True)
 
  
  p11data_transposed = p11_raw_data.transpose()
  p11data_transposed.reset_index(inplace = True)

  p11data_transposed.to_csv('widget_data/p11data.csv', index = False)

  return ktg_2023, ktg_2024, ktg_2025

# update_downtime_graph_data(['first11', 'first06'])