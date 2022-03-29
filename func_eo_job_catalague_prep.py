import pandas as pd
import functions
def eo_job_catologue():
  '''создание файла eo_job_catologue: список оборудование - работа на оборудовании'''
  # Джойним список машин из full_eo_list c планом ТО из maintanance_job_list_general
  maintanance_job_list_general_df = functions.maintanance_job_list_general_func()
  strategy_list = list(set( maintanance_job_list_general_df['strategy_id']))
  maintanance_job_list_general_df.rename(columns={'upper_level_tehmesto_code': 'level_upper'}, inplace=True)
  full_eo_list = functions.full_eo_list_func()
  full_eo_list['strategy_id'] = full_eo_list['strategy_id'].astype(int)
  maintanance_job_list_general_df['strategy_id'] = maintanance_job_list_general_df['strategy_id'].astype(int)

  full_eo_list = full_eo_list.loc[full_eo_list['strategy_id'].isin(strategy_list)]
  eo_maintanance_plan_df = pd.merge(full_eo_list, maintanance_job_list_general_df, on='strategy_id', how='inner')


  # удаляем строки, в которых нет данных в колонке eo_main_class_code
  eo_maintanance_plan_df = eo_maintanance_plan_df.loc[eo_maintanance_plan_df['eo_main_class_code'] != 'no_data']

  # получаем первую букву в поле eo_class_code
  eo_maintanance_plan_df['check_S_eo_class_code'] = eo_maintanance_plan_df['eo_class_code'].astype(str).str[0]
  eo_maintanance_plan_df = eo_maintanance_plan_df.loc[eo_maintanance_plan_df['check_S_eo_class_code'] != 'S']

  eo_maintanance_plan_df['eo_maintanance_job_code'] = eo_maintanance_plan_df['eo_code'].astype(str) + '_' + \
                                                      eo_maintanance_plan_df['maintanance_code_id'].astype(str)

  eo_maintanance_plan_df = eo_maintanance_plan_df.loc[:,
                           ['eo_maintanance_job_code', 'strategy_id', 'eo_model_id', 'maintanance_code', 'eo_code',
                            'eo_main_class_code', 'eo_description', 'maintanance_category_id', 'maintanance_name',
                            'tr_category', 'tr_man_hours_start_value', 'tr_man_hours_finish_value',	'tr_downtime_start_value',	'tr_downtime_finish_value',	'tr_start_motohour',	'tr_finish_motohour',
                            'interval_type',
                            'interval_motohours', 'downtime_planned', 'man_hours', 'pass_interval', 'go_interval',
                            'operation_start_date', 'operation_finish_date']].reset_index(drop=True)
  # убираем строки у которых в поле tr_category есть текст tr 
  
  eo_maintanance_plan_df_no_tr = eo_maintanance_plan_df.loc[eo_maintanance_plan_df['tr_category'] != 'tr']
  """eo_maintanance_plan_df_no_tr - таблица с данными без ТР"""
  # eo_maintanance_plan_df_no_tr.to_csv('data/eo_maintanance_plan_df_no_tr_delete.csv')
  
  eo_maintanance_plan_df_tr = eo_maintanance_plan_df.loc[eo_maintanance_plan_df['tr_category'] == 'tr']
  """eo_maintanance_plan_df_tr - таблица с данными c ТР"""
  result_list = []

  for row in eo_maintanance_plan_df_tr.itertuples():
    maintanance_code = getattr(row, "maintanance_code")
    eo_maintanance_job_code = getattr(row, "eo_maintanance_job_code")
    strategy_id = getattr(row, "strategy_id")
    eo_model_id = getattr(row, "eo_model_id")
    eo_code = getattr(row, "eo_code")
    eo_main_class_code = getattr(row, "eo_main_class_code")
    eo_description = getattr(row, "eo_description")
    maintanance_name = getattr(row, "maintanance_name")
    
    maintanance_category_id = getattr(row, "maintanance_category_id")
    tr_category = getattr(row, "tr_category")
    tr_man_hours_start_value = getattr(row, "tr_man_hours_start_value")
    tr_man_hours_finish_value = getattr(row, "tr_man_hours_finish_value")

    tr_downtime_start_value = getattr(row, "tr_downtime_start_value")
    tr_downtime_finish_value = getattr(row, "tr_downtime_finish_value")

    interval_type = getattr(row, "interval_type")
    tr_start_motohour = getattr(row, "tr_start_motohour")
    tr_finish_motohour = getattr(row, "tr_finish_motohour")

    interval_motohours = getattr(row, "interval_motohours")
    
    total_qty_of_tr = (tr_finish_motohour - tr_start_motohour) / interval_motohours
    if total_qty_of_tr >0:

      tr_downtime_delta = (tr_downtime_finish_value -tr_downtime_start_value) / total_qty_of_tr
      """tr_downtime_delta -пророст значения простоя на следующем ТР"""
      
      tr_service_interval = 0
      """tr_service_interval - текущее значение межсервисного инетервала в цикле"""
      
      tr_man_hours_delta = (tr_man_hours_finish_value - tr_man_hours_start_value) / total_qty_of_tr
      """tr_man_hours_delta -пророст значения трудозатрат на следующем ТР"""
      
      downtime_current_value = tr_downtime_start_value
      man_hours_current_value = tr_man_hours_start_value
      
      while tr_service_interval < tr_finish_motohour:
        temp_dict = {}
        temp_dict["eo_maintanance_job_code"] = eo_maintanance_job_code
        temp_dict["eo_model_id"] = eo_model_id
        temp_dict["eo_code"] = eo_code
        temp_dict["eo_main_class_code"] = eo_main_class_code
        temp_dict["eo_description"] = eo_description
        temp_dict["maintanance_name"] = maintanance_name
        
        temp_dict["maintanance_category_id"] = maintanance_category_id
        temp_dict["tr_category"] = tr_category
        temp_dict["tr_man_hours_start_value"] = tr_man_hours_start_value
        temp_dict["tr_man_hours_finish_value"] = tr_man_hours_finish_value
        temp_dict["maintanance_code"] = maintanance_code
        temp_dict["strategy_id"] = strategy_id
        tr_service_interval = tr_service_interval + interval_motohours
        temp_dict["maintanance_code"] = maintanance_code
        temp_dict["tr_downtime_start_value"] = tr_downtime_start_value
        temp_dict["tr_downtime_finish_value"] = tr_downtime_finish_value
        temp_dict["tr_start_motohour"] = tr_start_motohour
        temp_dict["tr_finish_motohour"] = tr_finish_motohour
        temp_dict["interval_type"] = interval_type
        temp_dict["interval_motohours"] = interval_motohours
        
        temp_dict["tr_service_interval"] = tr_service_interval
        downtime_current_value = downtime_current_value + tr_downtime_delta
        man_hours_current_value = man_hours_current_value + tr_man_hours_delta
        temp_dict['downtime_planned'] = downtime_current_value
        temp_dict['man_hours'] = man_hours_current_value

        result_list.append(temp_dict)
  
  tr_data_df = pd.DataFrame(result_list)
  # собираем две таблицы 
  tr_data_complete = pd.concat([eo_maintanance_plan_df_no_tr, tr_data_df])
  tr_data_complete.to_csv('data/eo_job_catologue.csv')

  
  # return tr_data_df
  # eo_maintanance_plan_df_tr.to_csv('data/eo_maintanance_plan_df_delete.csv')
  print("работает файл func_eo_job_catalague_prep.py")
  
# eo_job_catologue()