import pandas as pd
import functions
import initial_values
from datetime import timedelta
import json
first_day_of_selection = initial_values.first_day_of_selection
last_day_of_selection = initial_values.last_day_of_selection


def maintanance_jobs_df_prepare(calculation_start_mode):
  '''подготовка файла со списком работ - основной файл для построения графика простоев'''
  # print('расчет maintanance_jobs_df начат')
  eo_job_catologue_df = functions.eo_job_catologue_df_func()
  full_eo_list = functions.full_eo_list_func()
  with open('saved_filters.json', 'r') as openfile:
      # Reading from json file
      saved_filters_dict = json.load(openfile)
  calculation_start_mode = saved_filters_dict["calculation_start_status_value"]


  # full_eo_list = full_eo_list.loc[full_eo_list['eo_code'].isin(['100000062398', '100000008673', 'sl_730_1'])]
  full_eo_list = full_eo_list.loc[full_eo_list['eo_code'].isin(['sl_730_1', 'sl_730_2'])]
  # выдергиваем из full_eo_list 'eo_code', 'avearage_day_operation_hours'
  full_eo_list_selected = full_eo_list.loc[:, ['eo_code', 'avearage_day_operation_hours']]
   # джойним с full_eo_list

  eo_maint_plan_with_dates_with_full_eo_list = pd.merge(full_eo_list_selected, eo_job_catologue_df, on='eo_code', how='left')

  # eo_maint_plan_with_dates_with_full_eo_list.to_csv('data/eo_maint_plan_with_dates_with_full_eo_list_delete.csv')
  
  # джйоним с файлом last_maint_date - датами проведения последней формы.

  last_maint_date = functions.last_maint_date_func()
  eo_maint_plan = pd.merge(eo_maint_plan_with_dates_with_full_eo_list, last_maint_date, on='eo_maintanance_job_code',
                             how='left')
  # eo_maint_plan.to_csv('data/eo_maint_plan_delete.csv')
  # Итерируемся по eo
  # Список ео в выборке
  eo_list = list(set(eo_maint_plan['eo_code']))
  maintanance_jobs_complete_df = pd.DataFrame()
  for eo in eo_list:
    # print("eo: ", eo)
    eo_maint_plan_by_eo = eo_maint_plan.loc[eo_maint_plan['eo_code'] == eo]
    # print(eo_maint_plan_by_eo.info())
    # Сначала делаем выборку записей eto
    eo_maint_plan_eto = eo_maint_plan_by_eo.loc[eo_maint_plan['maintanance_category_id'] == 'eto']
    
    # Итериеруемся по этой выборке
    maintanance_jobs_result_list = []
    
    
    for row in eo_maint_plan_eto.itertuples():
      maintanance_job_code = getattr(row, "eo_maintanance_job_code")
      eo_code = getattr(row, "eo_code")
      standard_interval_motohours = float(getattr(row, "interval_motohours"))
      plan_downtime = getattr(row, "downtime_planned")
      man_hours = getattr(row, "man_hours")
      
      operation_start_date = getattr(row, "operation_start_date")
      #if initial_values.initial_start_status == "operation_start_date":
      if calculation_start_mode == 'operation_start_date':
        start_point = operation_start_date
      else:
        start_point =initial_values.start_point
    
      operation_finish_date = getattr(row, "operation_finish_date")
      avearage_day_operation_hours = float(getattr(row, "avearage_day_operation_hours"))
      maintanance_category_id = getattr(row, "maintanance_category_id")
      maintanance_name = getattr(row, "maintanance_name")
      interval_type = getattr(row, "interval_type")
      pass_interval = getattr(row, "pass_interval")
      go_interval = getattr(row, "go_interval")
      # если у нас ежедневное ТО, то это особый случай. переписываем точку старта
      start_point = initial_values.eto_start_point
      maintanance_start_datetime = start_point
      while maintanance_start_datetime < last_day_of_selection:
        temp_dict = {}
        temp_dict['maintanance_job_code'] = maintanance_job_code
        temp_dict['eo_code'] = eo_code
        temp_dict['interval_motohours'] = standard_interval_motohours
        temp_dict['interval_type'] = interval_type
        temp_dict['maint_interval'] = 24
        temp_dict['downtime_plan'] = plan_downtime
        temp_dict['man_hours'] = man_hours
        temp_dict['maintanance_start_datetime'] = maintanance_start_datetime
  
        maintanance_finish_datetime = maintanance_start_datetime + timedelta(hours=plan_downtime)
        temp_dict['maintanance_finish_datetime'] = maintanance_finish_datetime
        temp_dict['maintanance_date'] = maintanance_start_datetime.date()
        temp_dict['maintanance_category_id'] = maintanance_category_id
        temp_dict['maintanance_name'] = maintanance_name
        temp_dict['avearage_day_operation_hours'] = avearage_day_operation_hours
  
        maintanance_start_datetime = maintanance_start_datetime + timedelta(hours=24)
  
        if maintanance_start_datetime >= operation_start_date and maintanance_start_datetime <= operation_finish_date:
            maintanance_jobs_result_list.append(temp_dict)
          
    maintanance_jobs_eto_df = pd.DataFrame(maintanance_jobs_result_list)
    # maintanance_jobs_eto_df.to_csv('data/maintanance_jobs_eto_df_delete.csv')
  
    # если у формы нет поглащений другими формами, то расставляем через каждый интервал между формами
    # режем выборку!= 'eto' and pass_interval == 'not' и это не тр
    eo_maint_plan_no_ierarhy = eo_maint_plan_by_eo.loc[eo_maint_plan['maintanance_category_id'] != 'eto']
    eo_maint_plan_no_ierarhy = eo_maint_plan_no_ierarhy.loc[eo_maint_plan['go_interval'] == 'not']
    eo_maint_plan_no_ierarhy = eo_maint_plan_no_ierarhy.loc[eo_maint_plan['tr_category'] == 'not']
    # eo_maint_plan_no_ierarhy.to_csv('data/eo_maint_plan_no_ierarhy_delete.csv')
    # Итериеруемся по этой выборке
    maintanance_jobs_result_list = []
    for row in eo_maint_plan_no_ierarhy.itertuples():
      maintanance_job_code = getattr(row, "eo_maintanance_job_code")
      eo_code = getattr(row, "eo_code")
      standard_interval_motohours = float(getattr(row, "interval_motohours"))
      plan_downtime = getattr(row, "downtime_planned")
      man_hours = getattr(row, "man_hours")
      operation_start_date = getattr(row, "operation_start_date")
      operation_finish_date = getattr(row, "operation_finish_date")
      if initial_values.initial_start_status == "operation_start_date":
        start_point = operation_start_date
      else:
        start_point =initial_values.start_point
      
      avearage_day_operation_hours = float(getattr(row, "avearage_day_operation_hours"))
      maintanance_category_id = getattr(row, "maintanance_category_id")
      maintanance_name = getattr(row, "maintanance_name")
      interval_type = getattr(row, "interval_type")
      pass_interval = getattr(row, "pass_interval")
      go_interval = getattr(row, "go_interval")
  
      maintanance_start_datetime = start_point
      while maintanance_start_datetime < last_day_of_selection:
        temp_dict = {}
        temp_dict['maintanance_job_code'] = maintanance_job_code
        temp_dict['eo_code'] = eo_code
        temp_dict['interval_motohours'] = standard_interval_motohours
        temp_dict['interval_type'] = interval_type
        temp_dict['maint_interval'] = standard_interval_motohours
        temp_dict['downtime_plan'] = plan_downtime
        temp_dict['man_hours'] = man_hours
        
        temp_dict['maintanance_category_id'] = maintanance_category_id
        temp_dict['maintanance_name'] = maintanance_name
        # количество суток, которые требуются для того, чтобы выработать интервал до следующей формы
        number_of_days_to_next_maint = standard_interval_motohours // avearage_day_operation_hours
        # остаток часов в следующие сутки для выработки интервала до следующей формы
        remaining_hours = standard_interval_motohours - number_of_days_to_next_maint * avearage_day_operation_hours
        # календарный интервал между формами = кол-во суток х 24 + остаток
        calendar_interval_between_maint = number_of_days_to_next_maint * 24 + remaining_hours
  
        ############## В зависимости от типа межсервисного интервала определяем момент следующего ТО ##################
        next_maintanance_datetime = maintanance_start_datetime + timedelta(hours=calendar_interval_between_maint) + timedelta(hours=plan_downtime)
        maintanance_start_datetime = next_maintanance_datetime
        temp_dict['maintanance_start_datetime'] = maintanance_start_datetime
        maintanance_finish_datetime = maintanance_start_datetime + timedelta(hours=plan_downtime)
        temp_dict['maintanance_finish_datetime'] = maintanance_finish_datetime
        temp_dict['maintanance_start_date'] = maintanance_start_datetime.date()
        
        days_between_maintanance = next_maintanance_datetime - maintanance_start_datetime
        # Если интервал задан в часах, то переписываем значения
        if interval_type == 'hrs':
          next_maintanance_datetime = maintanance_start_datetime + timedelta(
            hours=standard_interval_motohours) + timedelta(hours=plan_downtime)
          days_between_maintanance = next_maintanance_datetime - maintanance_start_datetime
  
        temp_dict['days_between_maintanance'] = days_between_maintanance
        temp_dict['next_maintanance_datetime'] = next_maintanance_datetime
  
        if maintanance_start_datetime >= operation_start_date and maintanance_start_datetime <= operation_finish_date:
          maintanance_jobs_result_list.append(temp_dict)
  
        
    
    maintanance_jobs_no_ierarhy_df = pd.DataFrame(maintanance_jobs_result_list) 
    # maintanance_jobs_no_ierarhy_df.to_csv('data/maintanance_jobs_no_ierarhy_df_full_list_delete.csv')
  
    # остаются записи, которые не ЕТО, и у которых есть поглащения форм.
    # для таких записей итерируемся по списку 'go interval' и это не тр
    eo_maint_plan_ierarhy = eo_maint_plan_by_eo.loc[eo_maint_plan['maintanance_category_id'] != 'eto']
    eo_maint_plan_ierarhy = eo_maint_plan_ierarhy.loc[eo_maint_plan['go_interval'] != 'not']
    eo_maint_plan_ierarhy = eo_maint_plan_ierarhy.loc[eo_maint_plan['tr_category'] == 'not']
    # eo_maint_plan_ierarhy.to_csv('data/eo_maint_plan_ierarhy_delete.csv')
    
    maintanance_jobs_result_list = []
    for row in eo_maint_plan_ierarhy.itertuples():
      maintanance_job_code = getattr(row, "eo_maintanance_job_code")
      eo_code = getattr(row, "eo_code")
      standard_interval_motohours = float(getattr(row, "interval_motohours"))
      plan_downtime = getattr(row, "downtime_planned")
      man_hours = getattr(row, "man_hours")
      operation_start_date = getattr(row, "operation_start_date")
      operation_finish_date = getattr(row, "operation_finish_date")
      if initial_values.initial_start_status == "operation_start_date":
        start_point = operation_start_date
      else:
        start_point =initial_values.start_point
      
      
      avearage_day_operation_hours = float(getattr(row, "avearage_day_operation_hours"))
      maintanance_category_id = getattr(row, "maintanance_category_id")
      maintanance_name = getattr(row, "maintanance_name")
      interval_type = getattr(row, "interval_type")
      pass_interval = getattr(row, "pass_interval")
      go_interval = getattr(row, "go_interval")
  
      go_interval_list = go_interval.split(';')
      go_interval_list = [int(i) for i in go_interval_list]
  
      # base_start_maintanance_datetime - это дата к которой будем прибавлять все интервалы из цикла периодов
      base_start_maintanance_datetime = start_point
  
      # итерируемся по списку go_interval
      for maintanance_interval_temp in go_interval_list:
        # количество суток, которые требуются для того, чтобы выработать интервал до следующей формы
        number_of_days_to_next_maint = maintanance_interval_temp // avearage_day_operation_hours
        # остаток часов в следующие сутки для выработки интервала до следующей формы
        remaining_hours = maintanance_interval_temp - number_of_days_to_next_maint * avearage_day_operation_hours
        # календарный интервал между формами = кол-во суток х 24 + остаток
        calendar_interval_between_maint = number_of_days_to_next_maint * 24 + remaining_hours
        maintanance_start_datetime = base_start_maintanance_datetime + timedelta(
            hours=calendar_interval_between_maint) + timedelta(hours=plan_downtime)
        
        # print("maintanance_name", maintanance_name)
        # print("maintanance_start_datetime", maintanance_start_datetime)
        maintanance_finish_datetime = maintanance_start_datetime + timedelta(hours=plan_downtime)
        temp_dict = {}
        temp_dict['maintanance_job_code'] = maintanance_job_code
        temp_dict['eo_code'] = eo_code
        temp_dict['interval_motohours'] = standard_interval_motohours
        temp_dict['interval_type'] = interval_type
        temp_dict['downtime_plan'] = plan_downtime
        temp_dict['man_hours'] = man_hours
        temp_dict['maintanance_start_datetime'] = maintanance_start_datetime
        temp_dict['maintanance_finish_datetime'] = maintanance_finish_datetime
        temp_dict['maintanance_start_date'] = maintanance_start_datetime.date()
        temp_dict['maintanance_category_id'] = maintanance_category_id
        temp_dict['maintanance_name'] = maintanance_name
  
        temp_dict['maint_interval'] = maintanance_interval_temp
        temp_dict['pass_interval_list'] = pass_interval
        temp_dict['go_interval_list'] = go_interval
        next_maintanance_datetime = maintanance_start_datetime + timedelta(
            hours=calendar_interval_between_maint) + timedelta(hours=plan_downtime)
        days_between_maintanance = next_maintanance_datetime - maintanance_start_datetime
        temp_dict['days_between_maintanance'] = days_between_maintanance
        temp_dict['next_maintanance_datetime'] = next_maintanance_datetime
        if maintanance_start_datetime >= operation_start_date and maintanance_start_datetime <= operation_finish_date:
          maintanance_jobs_result_list.append(temp_dict)
    maintanance_jobs_ierarhy_df = pd.DataFrame(maintanance_jobs_result_list)
    # maintanance_jobs_ierarhy_df.to_csv('data/maintanance_jobs_ierarhy_df_full_list_delete.csv')
    
    maintanance_jobs_df = pd.concat([maintanance_jobs_eto_df, maintanance_jobs_no_ierarhy_df, maintanance_jobs_ierarhy_df], ignore_index=True)
    # maintanance_jobs_df.sort_values(by=['maintanance_start_datetime'], inplace = True)
    # maintanance_jobs_df.to_csv('data/maintanance_jobs_df_before_cut.csv')
    
    ################# ОБРАБОТКА ЗАПИСЕЙ С ТР ########################
    eo_maint_plan_tr = eo_maint_plan_by_eo.loc[eo_maint_plan['tr_category'] == 'tr']
    # eo_maint_plan_tr.to_csv('data/eo_maint_plan_tr_delete.csv')
    # для данной ео внутри цикла которой мы находимся надо получить дату начала эксплуатации 
    eo_start_operation_datetime_df = full_eo_list.loc[full_eo_list['eo_code'] == eo, ['operation_start_date']]
    eo_start_operation_datetime = eo_start_operation_datetime_df.iloc[0]['operation_start_date']
    
    eo_finish_operation_datetime_df = full_eo_list.loc[full_eo_list['eo_code'] == eo, ['operation_finish_date']]
    eo_finish_operation_datetime = eo_finish_operation_datetime_df.iloc[0]['operation_finish_date']
    

    avearage_day_operation_hours_df = full_eo_list.loc[full_eo_list['eo_code'] == eo, ['avearage_day_operation_hours']]
    avearage_day_operation_hours = avearage_day_operation_hours_df.iloc[0]['avearage_day_operation_hours']
    # print(avearage_day_operation_hours)
    # получаем выборку для расчета моментов проведения работ
    eo_maint_plan_tr_data = eo_maint_plan_tr.loc[:, ['interval_motohours', 'tr_service_interval', 'downtime_planned', 'man_hours']]

    maintanance_start_datetime = eo_start_operation_datetime
    maintanance_jobs_tr_result_list = []
    
    for row in eo_maint_plan_tr_data.itertuples():
      # maintanance_job_code = getattr(row, "maintanance_job_code")
      temp_dict = {}
      interval_motohours = float(getattr(row, "interval_motohours"))
      tr_service_interval = getattr(row, "tr_service_interval")
      downtime_planned = getattr(row, "downtime_planned")
      man_hours = getattr(row, "man_hours")
      eo_code = eo
      
      temp_dict['eo_code'] = eo_code
      temp_dict['interval_motohours'] = interval_motohours
      temp_dict['maint_interval'] = interval_motohours
      temp_dict['tr_service_interval'] = tr_service_interval
      
      temp_dict['downtime_plan'] = downtime_planned
      temp_dict['man_hours'] = man_hours

      # количество суток, которые требуются для того, чтобы выработать интервал до следующей формы
      number_of_days_to_next_maint = interval_motohours // avearage_day_operation_hours
      # остаток часов в следующие сутки для выработки интервала до следующей формы
      remaining_hours = interval_motohours - number_of_days_to_next_maint * avearage_day_operation_hours
      # календарный интервал между формами = кол-во суток х 24 + остаток
      calendar_interval_between_maint = number_of_days_to_next_maint * 24 + remaining_hours

      
      
      maintanance_start_datetime = maintanance_start_datetime + timedelta(hours=calendar_interval_between_maint) + timedelta(hours=downtime_planned)
      temp_dict['maintanance_start_datetime'] = maintanance_start_datetime
      maintanance_finish_datetime = maintanance_start_datetime + timedelta(hours=downtime_planned)
      temp_dict['maintanance_finish_datetime'] = maintanance_finish_datetime
      temp_dict['maintanance_start_date'] = maintanance_start_datetime.date()
      
      # print("maintanance_start_datetime", maintanance_start_datetime)
      
      
      next_maintanance_datetime = maintanance_start_datetime + timedelta(hours=calendar_interval_between_maint) + timedelta(hours=downtime_planned)
      temp_dict['next_maintanance_datetime'] = next_maintanance_datetime
      days_between_maintanance = next_maintanance_datetime - maintanance_start_datetime
      # Если интервал задан в часах, то переписываем значения
      if interval_type == 'hrs':
        next_maintanance_datetime = maintanance_start_datetime + timedelta(
          hours=interval_motohours) + timedelta(hours=downtime_planned)
        days_between_maintanance = next_maintanance_datetime - maintanance_start_datetime

      temp_dict['days_between_maintanance'] = days_between_maintanance


      if maintanance_start_datetime >= eo_start_operation_datetime and maintanance_start_datetime <= eo_finish_operation_datetime:
        maintanance_jobs_tr_result_list.append(temp_dict)
    
    maintanance_jobs_tr_df = pd.DataFrame(maintanance_jobs_tr_result_list)
    maintanance_jobs_tr_df.to_csv('data/maintanance_jobs_tr_df_delete.csv')
    ########################## Конец расчета ТР #########################
  
    
    
    # режем то, что получилось в период три (десять) года
    maintanance_jobs_df = maintanance_jobs_df.loc[
        maintanance_jobs_df['maintanance_start_datetime'] >= first_day_of_selection]
    maintanance_jobs_df = maintanance_jobs_df.loc[
        maintanance_jobs_df['maintanance_start_datetime'] <= last_day_of_selection]
    
    ############# прицепляем eo_model_id #############################
    eo_model_id_eo_list = full_eo_list.loc[:, ['eo_code', 'eo_model_id', 'eo_model_name', 'level_upper', 'level_1']]
    maintanance_jobs_df = pd.merge(maintanance_jobs_df, eo_model_id_eo_list, on='eo_code', how='left')
  
    maintanance_jobs_df['maintanance_date'] = maintanance_jobs_df['maintanance_start_datetime'].astype(str)
    maintanance_jobs_df['year'] = maintanance_jobs_df['maintanance_start_datetime'].dt.year
    maintanance_jobs_df['month'] = maintanance_jobs_df['maintanance_start_datetime'].dt.month
    maintanance_jobs_df['day'] = maintanance_jobs_df['maintanance_start_datetime'].dt.day
    maintanance_jobs_df['hour'] = maintanance_jobs_df['maintanance_start_datetime'].dt.hour
    maintanance_jobs_df['month_year'] = maintanance_jobs_df['month'].astype('str') + "_" + maintanance_jobs_df[
        'year'].astype('str')
    sort_index_month_year = initial_values.period_sort_index
  
    maintanance_jobs_df['month_year_sort_index'] = maintanance_jobs_df['month_year'].map(sort_index_month_year)
  
    level_upper = pd.read_csv('data/level_upper.csv')
  
    # джойним с level_upper
    maintanance_jobs_df = pd.merge(maintanance_jobs_df, level_upper, on='level_upper', how='left')
    # создаем поле-ключ teh-mesto-month-year
  
    maintanance_jobs_df['teh_mesto_month_year'] = maintanance_jobs_df['level_upper'] + '_' + maintanance_jobs_df[
        'month_year']
  
    maintanance_jobs_df['maintanance_jobs_id'] = maintanance_jobs_df['eo_code'].astype(str) + "_" + maintanance_jobs_df[
        'maintanance_category_id'].astype(str) + "_" + maintanance_jobs_df['maintanance_start_datetime'].astype(str)
    maintanance_jobs_df.sort_values(by=['maintanance_start_datetime'], ignore_index = True, inplace=True)
    
    maintanance_jobs_complete_df = pd.concat([maintanance_jobs_complete_df, maintanance_jobs_df])
  maintanance_jobs_complete_df.sort_values(by=['maintanance_start_datetime'], ignore_index = True, inplace=True)
  maintanance_jobs_df.to_csv('data/maintanance_jobs_df.csv', index=False)

  # print("расчет maintanance_jobs_df завершен")

  job_list = ['eto'] + list(set(maintanance_jobs_df['maintanance_category_id']))
  job_list_df = pd.DataFrame(job_list, columns = ['maintanance_category_id'])
  job_list_df.to_csv('data/job_list.csv', index = False)
  
  # заготовка для подсчета количества машин в выборке
  eo_calculation_table = maintanance_jobs_df.groupby(['eo_code', 'level_1', 'eo_model_id', 'year'], as_index = False)['eo_code'].size()
  eo_calculation_table = eo_calculation_table.loc[:, ['eo_code', 'level_1', 'eo_model_id', 'year']]
  eo_calculation_table.to_csv('widget_data/eo_calculation_table.csv', index = False)


  """подготовка csv файла для выгрузки в эксель данных о машинах в выборке"""
  # Читаем maintanance_jobs_df()

  maintanance_jobs_dataframe = maintanance_jobs_df
  # извлекаем список ЕО
  eo_list = pd.DataFrame(list(set(maintanance_jobs_dataframe['eo_code'])), columns=['eo_code'])
  # джойним с full_eo_list 
  full_eo_list = functions.full_eo_list_func()
  eo_list_data = pd.merge(eo_list, full_eo_list, on = 'eo_code', how ='left')

  # выбираем колонки
  eo_list_data = eo_list_data.loc[:, ['level_1_description','eo_class_description','constr_type','teh_mesto',	'mvz','eo_model_name', 'eo_code',	 'eo_description',  'operation_start_date', 'operation_finish_date', 'avearage_day_operation_hours_updated', 'Наработка 1.03.2022', 'level_1']]
  # eo_list_data['eo_code'] = eo_list_data['eo_code'].astype(str)
  # переименовываем колонки
  eo_download_data = eo_list_data.rename(columns=initial_values.rename_columns_dict)
  eo_download_data.to_csv('data/eo_download_data_raw.csv', index = False)

  ############### ДАННЫЕ ДЛЯ ПОСТРОЕНИЯ ГРАФИКА ПО ТРУДОЗАТРАТАМ ##############
  maintanance_jobs_df['man_hours'] = maintanance_jobs_df['man_hours'].astype(float)
  man_hours_raw_data_df = maintanance_jobs_df.groupby(['level_1', 'eo_code', 'month_year', 'eo_model_id'], as_index = False)['man_hours'].sum()
  man_hours_raw_data_df.to_csv('data/man_hours_raw_data_df.csv', index = False)


  """подготовка csv файла для выгрузки в эксель данных о работах, которые вошли в отчет"""
  # Читаем maintanance_jobs_df()
  # maintanance_jobs_dataframe = maintanance_jobs_df
   
  # # извлекаем список ЕО
  # full_eo_list = functions.full_eo_list_func()
  # full_eo_list = full_eo_list.loc[:, ['eo_code','level_1_description', 'eo_class_description', 'constr_type', 'teh_mesto', 'mvz', 'eo_description', 'operation_start_date', 'operation_finish_date', 'avearage_day_operation_hours_updated', 'Наработка 1.03.2022']]
  # # джойним с full_eo_list
  
  # maint_jobs_data = pd.merge(maintanance_jobs_dataframe, full_eo_list, on = 'eo_code', how ='left')
  # maint_jobs_data['downtime_plan'] = maint_jobs_data['downtime_plan'].astype(str)
  # maint_jobs_data['downtime_plan'] = maint_jobs_data['downtime_plan'].str.replace('.', ',', regex=False)
 
  #  # выбираем колонки
  # maint_jobs_data = maint_jobs_data.loc[:, ['level_1_description','eo_class_description','constr_type','teh_mesto',	'mvz','eo_model_name', 'eo_code',	 'eo_description',  'operation_start_date', 'operation_finish_date', 'avearage_day_operation_hours_updated', 'Наработка 1.03.2022', 'maintanance_start_datetime','maintanance_finish_datetime','year', 'month', 'maintanance_name', 'downtime_plan', 'level_1']]
  # # переименовываем колонки
  # maint_jobs_data_for_excel = maint_jobs_data.rename(columns= initial_values.rename_columns_dict)
  
  # maint_jobs_data_for_excel.to_csv('data/maint_jobs_download_data_raw.csv', index = False)
  print("отработал maintanance_jobs_df_prepare()")
  

maintanance_jobs_df_prepare("operation_start_date")
