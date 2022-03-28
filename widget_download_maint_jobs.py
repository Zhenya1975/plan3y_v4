import pandas as pd
import functions
import initial_values

def maint_jobs_download_preparation(be_list_for_dataframes_filtering):
  """подготовка csv файла для выгрузки в эксель данных о работах, которые вошли в отчет"""
  # Читаем maintanance_jobs_df()
  maint_job_data = pd.read_csv('data/maint_jobs_download_data_raw.csv', low_memory=False)
  print(be_list_for_dataframes_filtering)
  # maintanance_jobs_dataframe = maint_job_data.loc[maint_job_data['level_1']].isin(be_list_for_dataframes_filtering)
  # maintanance_jobs_dataframe.drop(labels='level_1', axis=1)
  # извлекаем список ЕО
  full_eo_list = functions.full_eo_list_func()
  full_eo_list = full_eo_list.loc[:, ['eo_code','level_1_description', 'eo_class_description', 'constr_type', 'teh_mesto', 'mvz', 'eo_description', 'operation_start_date', 'operation_finish_date', 'avearage_day_operation_hours_updated', 'Наработка 1.03.2022']]
  # джойним с full_eo_list
  
  maint_jobs_data = pd.merge(maint_job_data, full_eo_list, on = 'eo_code', how ='left')
  maint_jobs_data['downtime_plan'] = maint_jobs_data['downtime_plan'].astype(str)
  maint_jobs_data['downtime_plan'] = maint_jobs_data['downtime_plan'].str.replace('.', ',', regex=False)
 
   # выбираем колонки
  maint_jobs_data = maint_jobs_data.loc[:, ['level_1_description','eo_class_description','constr_type','teh_mesto',	'mvz','eo_model_name', 'eo_code',	 'eo_description',  'operation_start_date', 'operation_finish_date', 'avearage_day_operation_hours_updated', 'Наработка 1.03.2022', 'maintanance_start_datetime','maintanance_finish_datetime','year', 'month', 'maintanance_name', 'downtime_plan']]
  # переименовываем колонки
  maint_jobs_data_for_excel = maint_jobs_data.rename(columns= initial_values.rename_columns_dict)
  
  maint_jobs_data_for_excel.to_csv('widget_data/maint_jobs_download_data.csv', index = False)

  return maint_jobs_data_for_excel