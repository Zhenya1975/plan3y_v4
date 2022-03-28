import pandas as pd
import functions

def func_tr_maint_data_prep():
  """Подготовка данных для прогрессивных значений ТР"""
  # Читаем maintanance_jobs_df
  maintanance_job_list_general = functions.maintanance_job_list_general_func()
  result_list = []
  for row in maintanance_job_list_general.itertuples():
    maintanance_code = getattr(row, "maintanance_code")
    strategy_id = getattr(row, "strategy_id")
    maintanance_category_id = getattr(row, "maintanance_category_id")
    downtime_start = getattr(row, "downtime_start")
    downtime_finish = getattr(row, "downtime_finish")
    downtime_motohours_limit = getattr(row, "downtime_motohours_limit")
    interval_motohours = getattr(row, "interval_motohours")
    
    if maintanance_category_id == "tr":
      
      # print("downtime_motohours_limit", downtime_motohours_limit)
      # print("interval_motohours", interval_motohours)
      
      total_qty_of_tr = downtime_motohours_limit / interval_motohours
      # print("total_qty_of_tr", total_qty_of_tr)
      # дельта прироста простоя от ожной тр к другой
      if total_qty_of_tr >0:
        tr_downtime_delta = downtime_finish / total_qty_of_tr
        tr_service_interval = 0
        downtime = downtime_start
        while tr_service_interval < downtime_motohours_limit:
          temp_dict = {}
          temp_dict["maintanance_code"] = maintanance_code
          temp_dict["strategy_id"] = strategy_id
          tr_service_interval = tr_service_interval + interval_motohours
          temp_dict["maintanance_code"] = maintanance_code
          temp_dict["tr_service_interval"] = tr_service_interval
          downtime = downtime + tr_downtime_delta
          temp_dict['downtime'] = downtime
          result_list.append(temp_dict)

      
  result_df = pd.DataFrame(result_list)   
  print(result_df)
     

func_tr_maint_data_prep()