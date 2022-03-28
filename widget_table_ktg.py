import pandas as pd
import initial_values
import functions

def ktg_table_prep(be_list_for_dataframes_filtering):
  """Подготовка таблицы ктг. В строках - модели ЕО, в столбцах месяцы"""
  ktg_by_month_data_df = functions.ktg_data_reading()
  ktg_by_month_data_df = ktg_by_month_data_df.loc[ktg_by_month_data_df['level_1'].isin(be_list_for_dataframes_filtering)]
  ktg_graph_data = ktg_by_month_data_df.groupby(['eo_model_name', 'month_year'], as_index=False)[['calendar_fond', 'downtime']].sum()
  ktg_graph_data.to_csv('data/ktg_graph_data_delete.csv')
  
  period_dict = initial_values.period_dict
    
  period_sort_index = initial_values.period_sort_index
  
  ktg_graph_data['period'] = ktg_graph_data['month_year'].map(period_dict).astype(str)
  
  ktg_graph_data['period_sort_index'] = ktg_graph_data['month_year'].map(period_sort_index)
  ktg_graph_data.sort_values(by='period_sort_index', inplace = True)
  
  # Список моделей из выборки
  eo_model_list = list(set(ktg_graph_data['eo_model_name']))
  columns_list = initial_values.months_list
  columns_list = ['Модель ЕО'] + columns_list
  index_list = eo_model_list
  ktg_table_df = pd.DataFrame(columns=columns_list, index=index_list)
  
  # Сначала внешним циклом итерируемся по строкам таблицы - то есть по списку моделей ео
  for eo_model in eo_model_list:
    temp_dict = {}
    # делаем срез  - все записи текущей модели ео
    ktg_graph_data_selected = ktg_graph_data.loc[ktg_graph_data['eo_model_name'] == eo_model]
    
    ktg_graph_data_selected_groupped = ktg_graph_data_selected.groupby(['period_sort_index','month_year'], as_index = False)[['calendar_fond', 'downtime']].sum()
    
    ktg_graph_data_selected_groupped['ktg'] = (ktg_graph_data_selected_groupped['calendar_fond'] - ktg_graph_data_selected_groupped['downtime']) / ktg_graph_data_selected_groupped['calendar_fond']

    ktg_graph_data_selected_groupped['ktg'] = ktg_graph_data_selected_groupped['ktg'].apply(lambda x: round(x, 2))
    
    temp_dict['Модель ЕО'] = eo_model
    # итерируемся по полученном временном срезу по модели
    for row in ktg_graph_data_selected_groupped.itertuples():
      month_year = getattr(row, 'month_year')
      ktg = getattr(row, 'ktg')
      temp_dict[month_year] = ktg


    ktg_table_df.loc[eo_model] = pd.Series(temp_dict)
    
  ktg_table_df = ktg_table_df.rename(columns = initial_values.period_dict)
  ktg_table_df.index.name = 'Наименование модели ЕО'
  # ktg_table_df['Наименование модели'] = ktg_table_df.index

  ktg_table_df.to_csv('widget_data/ktg_table_data.csv', index = False)
  return ktg_table_df

  
  
# ktg_table_prep()