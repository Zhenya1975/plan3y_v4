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

  # eo_maintanance_plan_df.to_csv('data/eo_maintanance_plan_df_delete.csv')

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
  
  eo_maintanance_plan_df_tr = eo_maintanance_plan_df.loc[eo_maintanance_plan_df['tr_category'] == 'tr']

  result_list = []
  # print(eo_maintanance_plan_df_tr.info())
  i = 0
  for row in eo_maintanance_plan_df_tr.itertuples():
    i = i+1
    print(i)
    maintanance_code = getattr(row, "maintanance_code")
    eo_maintanance_job_code = getattr(row, "eo_maintanance_job_code")
    strategy_id = getattr(row, "strategy_id")
    maintanance_category_id = getattr(row, "maintanance_category_id")
    tr_category = getattr(row, "tr_category")
    tr_man_hours_start_value = getattr(row, "tr_man_hours_start_value")
    tr_man_hours_finish_value = getattr(row, "tr_man_hours_finish_value")
    tr_downtime_start_value = getattr(row, "tr_downtime_start_value")
    tr_downtime_finish_value = getattr(row, "tr_downtime_finish_value")
    tr_start_motohour = getattr(row, "tr_start_motohour")
    tr_finish_motohour = getattr(row, "tr_finish_motohour")

    interval_motohours = getattr(row, "interval_motohours")
    
    total_qty_of_tr = (tr_finish_motohour - tr_start_motohour) / interval_motohours
    if total_qty_of_tr >0:
      tr_downtime_delta = (tr_downtime_finish_value -tr_downtime_start_value) / total_qty_of_tr
      tr_service_interval = 0
      downtime = tr_downtime_start_value

      # print("tr_downtime_finish_value", tr_downtime_finish_value)
      # print("tr_downtime_start_value", tr_downtime_start_value)
      # print("tr_downtime_delta", tr_downtime_delta)
      # print('total_qty_of_tr', total_qty_of_tr)
      
      while tr_service_interval < tr_finish_motohour:
        temp_dict = {}
        temp_dict["eo_maintanance_job_code"] = eo_maintanance_job_code
        
        temp_dict["maintanance_category_id"] = maintanance_category_id
        temp_dict["tr_category"] = tr_category
        temp_dict["tr_man_hours_start_value"] = tr_man_hours_start_value
        temp_dict["tr_man_hours_finish_value"] = tr_man_hours_finish_value
        temp_dict["maintanance_code"] = maintanance_code
        temp_dict["strategy_id"] = strategy_id
        tr_service_interval = tr_service_interval + interval_motohours
        temp_dict["maintanance_code"] = maintanance_code
        temp_dict["tr_service_interval"] = tr_service_interval
        downtime = downtime + tr_downtime_delta
        temp_dict['downtime'] = downtime
        # print(temp_dict)
        result_list.append(temp_dict)
  
  result_df = pd.DataFrame(result_list)   
  result_df.to_csv('data/result_df_delete.csv')
  
  # eo_maintanance_plan_df_tr.to_csv('data/eo_maintanance_plan_df_delete.csv')

  
eo_job_catologue()