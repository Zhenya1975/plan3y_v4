import pandas as pd
import functions
import initial_values

def eo_list_download_preparation(be_list_for_dataframes_filtering):
  """подготовка csv файла для выгрузки в эксель данных о машинах в выборке"""
  
  eo_download_data_raw = pd.read_csv('data/eo_download_data_raw.csv', low_memory=False)
  eo_download_data = eo_download_data_raw.loc[eo_download_data_raw['level_1'].isin(be_list_for_dataframes_filtering)]
  eo_download_data.drop(labels='level_1', axis=1)
  # # Читаем maintanance_jobs_df()
  # maintanance_jobs_dataframe = functions.maintanance_jobs_df()
  # maintanance_jobs_dataframe = maintanance_jobs_dataframe.loc[maintanance_jobs_dataframe['level_1'].isin(be_list_for_dataframes_filtering)]
  # # извлекаем список ЕО
  # eo_list = pd.DataFrame(list(set(maintanance_jobs_dataframe['eo_code'])), columns=['eo_code'])
  # # джойним с full_eo_list 
  # full_eo_list = functions.full_eo_list_func()
  # eo_list_data = pd.merge(eo_list, full_eo_list, on = 'eo_code', how ='left')

  # # выбираем колонки
  # eo_list_data = eo_list_data.loc[:, ['level_1_description','eo_class_description','constr_type','teh_mesto',	'mvz','eo_model_name', 'eo_code',	 'eo_description',  'operation_start_date', 'operation_finish_date', 'avearage_day_operation_hours_updated', 'Наработка 1.03.2022']]
  # # eo_list_data['eo_code'] = eo_list_data['eo_code'].astype(str)
  # # переименовываем колонки
  # eo_download_data = eo_list_data.rename(columns=initial_values.rename_columns_dict)
  eo_download_data.to_csv('widget_data/eo_download_data.csv', index = False)
  
  return eo_download_data
