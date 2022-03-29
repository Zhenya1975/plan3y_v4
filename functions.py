import pandas as pd
import initial_values
from datetime import timedelta
import json

first_day_of_selection = initial_values.first_day_of_selection
last_day_of_selection = initial_values.last_day_of_selection




def full_eo_list_actual_func():
  """чтение full_eo_list_actual"""
  # full_eo_list_actual = pd.read_csv('data/full_eo_list_actual.csv', dtype=str)
  full_eo_list_actual = pd.read_csv("https://drive.google.com/uc?export=download&id=1GDB2rVwdquDQlI7qrVAlwwiaK2L86nzw", dtype=str)
  # print(full_eo_list_actual.info())
  
  # level_1_df = pd.read_csv("data/level_1.csv", dtype=str)
  # full_eo_list_actual = pd.merge(full_eo_list_actual, level_1_df, on = 'level_1', how = 'left')
  full_eo_list_actual["operation_start_date"] = pd.to_datetime(full_eo_list_actual["operation_start_date"])
  full_eo_list_actual["operation_finish_date"] = pd.to_datetime(full_eo_list_actual["operation_finish_date"])
  full_eo_list_actual = full_eo_list_actual.astype({'strategy_id': int, 'avearage_day_operation_hours': float})

  return full_eo_list_actual
# full_eo_list_actual_func()

def full_eo_list_func():
    """чтение full_eo_list"""
    full_eo_list = pd.read_csv('data/full_eo_list.csv', dtype=str)
    full_eo_list["operation_start_date"] = pd.to_datetime(full_eo_list["operation_start_date"])
    full_eo_list["operation_finish_date"] = pd.to_datetime(full_eo_list["operation_finish_date"])
    full_eo_list = full_eo_list.astype({'strategy_id': int, 'avearage_day_operation_hours': float})

    return full_eo_list


def last_maint_date_func():
    last_maint_date = pd.read_csv('data/last_maint_date.csv')
    last_maint_date["last_maintanance_date"] = pd.to_datetime(last_maint_date["last_maintanance_date"],
                                                              format='%d.%m.%Y')

    return last_maint_date

def ktg_data_reading():
  """чтение данных ктг"""
  ktg_by_month_data_df = pd.read_csv('data/ktg_by_month_data_df.csv', decimal = ",")
  ktg_by_month_data_df['month_year'] = ktg_by_month_data_df['month'].astype(str) + "_" + ktg_by_month_data_df['year'].astype(str)
  
  return ktg_by_month_data_df

def maintanance_jobs_df():
    """чтение maintanance_jobs_df"""
    maintanance_jobs_df = pd.read_csv('data/maintanance_jobs_df.csv', dtype=str)
    with open('saved_filters.json', 'r') as openfile:
      # Reading from json file
      saved_filters_dict = json.load(openfile)
    be_filter = saved_filters_dict['filter_be']
    full_be_list = list(set(maintanance_jobs_df['level_1']))
    if be_filter == []:
      be_filter = full_be_list
    maintanance_jobs_df = maintanance_jobs_df.loc[maintanance_jobs_df["level_1"].isin(be_filter)]
    maintanance_jobs_df = maintanance_jobs_df.astype({'downtime_plan': float, "month_year_sort_index": int, "year":int, "month":int, "day": int, "hour":int})

    return maintanance_jobs_df


def maintanance_job_list_general_func():
    """чтение maintanance_job_list_general"""
    maintanance_job_list_general = pd.read_csv('data/maintanance_job_list_general.csv', decimal=",")
    maintanance_job_list_general = maintanance_job_list_general.astype({'downtime_planned': float, 'strategy_id': int})
    return maintanance_job_list_general


def eo_job_catologue_df_func():
    """чтение eo_job_catologue_df"""
    eo_job_catologue_df = pd.read_csv('data/eo_job_catologue.csv', dtype=str)
    eo_job_catologue_df["downtime_planned"] = eo_job_catologue_df["downtime_planned"].astype('float')
    eo_job_catologue_df["operation_start_date"] = pd.to_datetime(eo_job_catologue_df["operation_start_date"])
    eo_job_catologue_df["operation_finish_date"] = pd.to_datetime(eo_job_catologue_df["operation_finish_date"])

    return eo_job_catologue_df


def pass_interval_fill():
    '''создание списка pass interval в maintanance_job_list_general'''

    maintanance_job_list_general = maintanance_job_list_general_func()

    for index, row in maintanance_job_list_general.iterrows():
        pass_interval_temp = row['pass_interval']
        interval_motohours = int(row['interval_motohours'])

        if pass_interval_temp != 'not':
            pass_interval_list = pass_interval_temp.split(';')
            pass_interval_list = [int(i) for i in pass_interval_list]

            # в temp_list складываем значения, которые соответствуют original pass_interval_list
            temp_list = []
            for pass_interval_value in pass_interval_list:
                if pass_interval_value not in temp_list:
                    temp_list.append(pass_interval_value)

                temp_value = pass_interval_value
                # temp_list = []
                while temp_value < 135000:

                    if temp_value not in temp_list:
                        temp_list.append(temp_value)
                        temp_list.sort()
                    temp_value = temp_value + pass_interval_value
            #####################  Создаем список maintanance_interval #####################
            # next_go_interval - значение интервала проведения формы, которое будем итеративно считать
            next_go_interval = interval_motohours
            # go_interval_list  - список, в который будем складывать значения интервалов для проведения форм
            go_interval_list = []
            while next_go_interval < 135000:
                # если текущее значение next_go_interval не находится в temp_list (списке пропусков форм)
                # то добавляем значение в белый список
                if next_go_interval not in temp_list:
                    go_interval_list.append(next_go_interval)
                # прибавляем к текущему значению next_go_interval значение периодичности interval_motohours
                next_go_interval = next_go_interval + interval_motohours

            temp_list = [str(i) for i in temp_list]
            temp_string = ";".join(temp_list)
            maintanance_job_list_general.loc[index, ['pass_interval']] = temp_string

            go_interval_list = [str(i) for i in go_interval_list]
            go_interval_list_string = ";".join(go_interval_list)
            maintanance_job_list_general.loc[index, ['go_interval']] = go_interval_list_string
        else:
            maintanance_job_list_general.loc[index, ['go_interval']] = 'not'

    maintanance_job_list_general.to_csv('data/maintanance_job_list_general.csv', index=False)


###################################################################################################
def maintanance_category_prep():
    """Создание файла со списком категорий работ ТОИР"""
    df = maintanance_job_list_general_func()

    maintanance_category_id_list = []
    maintanance_category_id_df_list = []
    for index, row in df.iterrows():
        temp_dict = {}
        maintanance_category_id = row['maintanance_category_id']
        maintanance_name = row['maintanance_name']
        temp_dict['maintanance_category_id'] = maintanance_category_id
        temp_dict['maintanance_name'] = maintanance_name
        if maintanance_category_id not in maintanance_category_id_list:
            maintanance_category_id_list.append(maintanance_category_id)
            maintanance_category_id_df_list.append(temp_dict)

    df_result = pd.DataFrame(maintanance_category_id_df_list)
    df_result.to_csv('data/maintanance_category.csv', index=False)


#############################################################################################
def select_eo_for_calculation():
  """Выборка ео из полного списка full_eo_list_actual в full_eo_list"""
  maintanance_job_list_general_df = maintanance_job_list_general_func()
  
  strategy_list = list(set(maintanance_job_list_general_df['strategy_id']))

  full_eo_list = full_eo_list_actual_func()
  full_eo_list['strategy_id'] = full_eo_list['strategy_id'].astype(int)
  maintanance_job_list_general_df['strategy_id'] = maintanance_job_list_general_df['strategy_id'].astype(int)
    
  full_eo_list = full_eo_list.loc[full_eo_list['strategy_id'].isin(strategy_list)]
  full_eo_list.to_csv('data/full_eo_list.csv', index=False)
  return full_eo_list
  #################
## select_eo_for_calculation()
    


#####################################################################################################
def eo_job_catologue():
    '''создание файла eo_job_catologue: список оборудование - работа на оборудовании'''
    # Джойним список машин из full_eo_list c планом ТО из maintanance_job_list_general
    maintanance_job_list_general_df = maintanance_job_list_general_func()
    strategy_list = list(set( maintanance_job_list_general_df['strategy_id']))
    maintanance_job_list_general_df.rename(columns={'upper_level_tehmesto_code': 'level_upper'}, inplace=True)
    full_eo_list = full_eo_list_func()
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
                              'interval_type',
                              'interval_motohours', 'downtime_planned', 'man_hours', 'pass_interval', 'go_interval',
                              'operation_start_date', 'operation_finish_date']].reset_index(drop=True)
    eo_job_catologue = eo_maintanance_plan_df

    eo_job_catologue.to_csv('data/eo_job_catologue.csv', index=False)
  

    ###################### ОБНОВЛЕНИЕ ДАННЫХ ФАЙЛА С ДАТОЙ СТАРТА РАБОТ. ####################################
    # если в файле last_maint_date нет строки с кодом eo_код формы, то добавляем строку в файл. Указываем дату по умолчанию 31.12.2022
    # список кодов в файле last_maint_date
    last_maint_date = pd.read_csv('data/last_maint_date.csv')
    last_maint_date_eo_maintanance_job_code_list = last_maint_date['eo_maintanance_job_code'].unique()
    # список кодов в файле eo_job_catologue
    eo_job_catologue_eo_maintanance_job_code_list = eo_job_catologue['eo_maintanance_job_code'].unique()

    # итерируемся по eo_job_catologue_eo_maintanance_job_code_list
    # если значение нет в списке last_maint_date_eo_maintanance_job_code_list то добавляем строку
    last_maint_date_eo_maintanance_job_code_update = []
    for eo_maintanance_job_code in eo_job_catologue_eo_maintanance_job_code_list:
        temp_dict = {}
        temp_dict['eo_maintanance_job_code'] = eo_maintanance_job_code
        temp_dict['last_maintanance_date'] = '31.12.2022'
        if eo_maintanance_job_code not in last_maint_date_eo_maintanance_job_code_list:
            last_maint_date_eo_maintanance_job_code_update.append(temp_dict)

    last_maint_date_eo_maintanance_job_code_update_df = pd.DataFrame(last_maint_date_eo_maintanance_job_code_update)
    last_maint_date_updated_df = pd.concat([last_maint_date, last_maint_date_eo_maintanance_job_code_update_df])
    last_maint_date_updated_df.to_csv('data/last_maint_date.csv', index=False)

    return eo_job_catologue

# eo_job_catologue()


# заполняем календарный фонд по оборудованию
# берем машины, кооторые участвуют в файле eo_job_catologue.csv
def fill_calendar_fond():
    eo_list_under_maintanance_program = pd.read_csv('data/eo_job_catologue.csv', dtype=str)
    # new data frame with split value columns
    # new = eo_list_under_maintanance_program['eo_code'].str.split(".", n = 1, expand = True)
    # making separate first name column from new data frame
    # eo_list_under_maintanance_program["eo_code"]= new[0]

    eo_list = eo_list_under_maintanance_program['eo_code'].unique()

    result_list = []
    # итерируемся по списку еo
    for eo in eo_list:
        maint_date = first_day_of_selection
        while maint_date < last_day_of_selection:
            temp_dict = {}
            temp_dict['eo_code'] = eo
            temp_dict['datetime'] = maint_date
            temp_dict['calendar_fond'] = 24
            result_list.append(temp_dict)
            maint_date = maint_date + timedelta(hours=24)

    eo_calendar_fond = pd.DataFrame(result_list)


###################### РАСЧЕТ КАЛЕНДАРНОГО ФОНДА И ПРОСТОЕВ ПО ЧАСАМ НА ТРИ ГОДА ######################
# def hour_calculation():
#   """РАСЧЕТ КАЛЕНДАРНОГО ФОНДА И ПРОСТОЕВ ПО ЧАСАМ НА ТРИ ГОДА"""
#   # необходимо получить дефолтную таблицу с нулями
#   # Для этого берем начальную точку в нуле часов 1 января 2023 года, прибавляем час и записываем новую строку
#   start_point = pd.to_datetime('01.01.2023', format='%d.%m.%Y')

#   # Список ЕО для почасовой модели
#   full_eo_list = full_eo_list_func()
#   # full_eo_list.to_csv('data/full_eo_list_delete.csv')
#   full_eo_list = full_eo_list.loc[:, ['level_1_description', 'eo_model_name', 'eo_code', 'operation_start_date', 'operation_finish_date']]

#   ###################  ОГРАНИЧИМСЯ ДЛЯ ТЕСТИРОВАНИЯ ОДНОЙ МАШИНОЙ #################
#   # eo_list = full_eo_list.loc[full_eo_list['eo_code'] == '100000084492']
#   # eo_list.to_csv('data/eo_list_delete.csv')
#   # operation_finish_date = pd.to_datetime('31.12.2023', format='%d.%m.%Y')
#   eo_list = full_eo_list

#   # eo_list = full_eo_list.loc[:, ['eo_code', 'operation_start_date', 'operation_finish_date']]

#   result_df_list = []
#   for row in eo_list.itertuples():
#       temp_dict = {}
#       be = getattr(row, "level_1_description")
#       eo_model_name = getattr(row, "eo_model_name")
#       eo_code = getattr(row, "eo_code")
#       operation_start_date = getattr(row, "operation_start_date")
#       operation_finish_date = getattr(row, "operation_finish_date")
#       current_hour = start_point
      
#       while current_hour < last_day_of_selection:
#           temp_dict['be'] = be
#           temp_dict['eo_model_name'] = eo_model_name
#           temp_dict['eo_code'] = eo_code
#           temp_dict['model_hour'] = current_hour
#           temp_dict['year'] = current_hour.year
#           temp_dict['month'] = current_hour.month
#           temp_dict['month_year'] = str(current_hour.month) + "_" + str(current_hour.year)
          

#           ################ если текущее значение часа внутри времени эксплуатации машины, то ставим единичку в календарный фонд времени #################
#           if current_hour > operation_start_date and current_hour < operation_finish_date:
#               temp_dict['calendar_fond_status'] = 1
#           else:
#               temp_dict['calendar_fond_status'] = 0

#           temp_dict['downtime_status'] = 0

#           result_df_list.append(temp_dict)
#           current_hour = current_hour + timedelta(hours=1)
#           temp_dict = {}

#   model_hours_df = pd.DataFrame(result_df_list)


#   # ПРОСТОИ. Нужно итерировать по таблице с простоями. Получать из нее ЕО момент начала ремонта и величину простоя
#   # определить момент окончания. Затем отрезать мастер таблицу по этому периоду и поместить в поле простоя единички
#   maintanance_jobs_df_ = maintanance_jobs_df()

#   maintanance_jobs_df_selected = maintanance_jobs_df_.loc[:,
#                                  ['maintanance_jobs_id', 'eo_code','maintanance_category_id', 'maintanance_name', 'maintanance_start_datetime', 'maintanance_finish_datetime', 'downtime_plan']]
#   # maintanance_jobs_df_selected.to_csv('data/maintanance_jobs_df_selected_delete.csv', index = False)

#   # Первым проходом заполняем ячейки значениями ето.
#   maintanance_jobs_df_selected_eto = maintanance_jobs_df_selected.loc[maintanance_jobs_df_selected['maintanance_category_id'] =='eto']

#   for row in maintanance_jobs_df_selected_eto.itertuples():
#     maintanance_jobs_id = getattr(row, "maintanance_jobs_id")
#     eo_code = getattr(row, "eo_code")
#     maintanance_name = getattr(row, "maintanance_name")
#     maintanance_category_id = getattr(row, "maintanance_category_id")
#     maintanance_start_datetime = getattr(row, "maintanance_start_datetime")
#     maintanance_finish_datetime = getattr(row, "maintanance_finish_datetime")
#     downtime_plan = getattr(row, "downtime_plan")
#     maintanance_name = getattr(row, "maintanance_name")

#     # Режем таблицу с почасовыми строками по моменту старта и завершения работы
#     model_hours_df_cut_by_maint_job = model_hours_df.loc[
#           (model_hours_df['model_hour'] >= maintanance_start_datetime) &
#           (model_hours_df['model_hour'] <= maintanance_finish_datetime)]
#     # indexes - список индексов записей в которых надо поставить единичку, то есть в этих записях есть простой
#     indexes = model_hours_df_cut_by_maint_job.index.values

#     model_hours_df_cut_by_maint_job = model_hours_df_cut_by_maint_job.copy()
#     model_hours_df.loc[indexes, ['maintanance_jobs_id']] = maintanance_jobs_id
#     model_hours_df.loc[indexes, ['maintanance_name']] = maintanance_name
#     ##### Записываем значение простоя на ето в основную таблицу
#     model_hours_df.loc[indexes, ['downtime_status']] = downtime_plan

#   # model_hours_df.to_csv('data/model_hours_df_check_eto_delete.csv')

#   #  После этого следующим проходом заполним нормальными ТО-шками
#   maintanance_jobs_df_selected = maintanance_jobs_df_selected.loc[maintanance_jobs_df_selected['maintanance_category_id'] !='eto']
#   for row in maintanance_jobs_df_selected.itertuples():
#     maintanance_jobs_id = getattr(row, "maintanance_jobs_id")

#     eo_code = getattr(row, "eo_code")
#     maintanance_name = getattr(row, "maintanance_name")
#     maintanance_category_id = getattr(row, "maintanance_category_id")
    
#     maintanance_start_datetime = getattr(row, "maintanance_start_datetime")
#     maintanance_finish_datetime = getattr(row, "maintanance_finish_datetime")
#     downtime_plan = getattr(row, "downtime_plan")
#     maintanance_name = getattr(row, "maintanance_name")


#     # Режем таблицу с почасовыми строками по моменту старта и завершения работы
#     model_hours_df_cut_by_maint_job = model_hours_df.loc[
#           (model_hours_df['model_hour'] >= maintanance_start_datetime) &
#           (model_hours_df['model_hour'] <= maintanance_finish_datetime)]


#     # indexes - список индексов записей в которых надо поставить единичку, то есть в этих записях есть простой
#     indexes = model_hours_df_cut_by_maint_job.index.values

#     model_hours_df_cut_by_maint_job = model_hours_df_cut_by_maint_job.copy()
#     model_hours_df.loc[indexes, ['maintanance_jobs_id']] = maintanance_jobs_id
#     model_hours_df.loc[indexes, ['maintanance_name']] = maintanance_name
#     ##### Записываем единичку в основную таблицу
#     model_hours_df.loc[indexes, ['downtime_status']] = 1
  
#   period_dict = initial_values.period_dict
  
#   period_sort_index = initial_values.period_sort_index

#   model_hours_df['period'] = model_hours_df['month_year'].map(period_dict).astype(str)

#   model_hours_df['period_sort_index'] = model_hours_df['month_year'].map(period_sort_index)
#   model_hours_df.sort_values(by='period_sort_index', inplace = True)
#   model_hours_df.to_csv('data/model_3y_hours_df.csv', index=False)
  

#   downtime_graph_data = model_hours_df.loc[:, ['period','downtime_status', 'period_sort_index']]
#   downtime_graph_data.sort_values(by='period_sort_index', inplace = True)

#   downtime_graph_data_groupped = downtime_graph_data.groupby(['period_sort_index', 'period'], as_index = False)[['downtime_status']].sum()
  
  
#   downtime_graph_data = downtime_graph_data_groupped.loc[:, ['period', 'downtime_status']]
#   downtime_graph_data = downtime_graph_data.rename(columns={'period': 'Период', 'downtime_status': 'Запланированный простой, час'})

#   downtime_graph_data.to_csv('widget_data/downtime_graph_data.csv', index = False)
  
#   return model_hours_df

# hour_calculation()


def total_qty_EO(be_list_for_dataframes_filtering, model_eo_filter_list_for_dataframes_filtering):
  """расчет количества машин в выборке для отображения в карточке 2023 года"""
  # Читаем maintanance_jobs_df()
  eo_qty_data = pd.read_csv('widget_data/eo_calculation_table.csv')
  eo_qty_data = eo_qty_data.loc[eo_qty_data['level_1'].isin(be_list_for_dataframes_filtering) &
  eo_qty_data['eo_model_id'].isin(model_eo_filter_list_for_dataframes_filtering)
  ]
  eo_qty_data_2023 = eo_qty_data.loc[eo_qty_data['year'] == 2023]
  eo_qty_data_2024 = eo_qty_data.loc[eo_qty_data['year'] == 2024]
  eo_qty_data_2025 = eo_qty_data.loc[eo_qty_data['year'] == 2025]
  # Считаем кол-во записей в колонке eo_code
  eo_qty_2023 = len(eo_qty_data_2023['eo_code'].unique())
  eo_qty_2024 = len(eo_qty_data_2024['eo_code'].unique())
  eo_qty_2025 = len(eo_qty_data_2025['eo_code'].unique())
  return eo_qty_2023, eo_qty_2024, eo_qty_2025




def downtime_by_categiries_data():
  """подготовка csv файла для построения пайчарта простоев по категориям в 2023, 2024, 2025 году"""
  # Читаем model_hours_ktg_data()
  model_hours_df = hour_calculation()
  ############# 2023 год ###################
  model_hours_df_2023 = model_hours_df.loc[model_hours_df['year'] == 2023] 
  model_hours_df_2023 = model_hours_df.loc[model_hours_df['downtime_status'] == 1] 

  model_hours_df_2023_groupped = model_hours_df_2023.groupby(['maintanance_name'], as_index=False)[['downtime_status']].sum()
  downtime_by_catagories_2023_data = model_hours_df_2023_groupped.rename(columns={'maintanance_name': 'Вид ТОРО', 'downtime_status': 'Простой, час'})
  downtime_by_catagories_2023_data.to_csv('widget_data/downtime_by_categiries_2023_data.csv', index = False)

  ############# 2024 год ###################
  model_hours_df_2024 = model_hours_df.loc[model_hours_df['year'] == 2024] 
  model_hours_df_2024 = model_hours_df.loc[model_hours_df['downtime_status'] == 1] 

  model_hours_df_2024_groupped = model_hours_df_2024.groupby(['maintanance_name'], as_index=False)[['downtime_status']].sum()
  downtime_by_catagories_2024_data = model_hours_df_2024_groupped.rename(columns={'maintanance_name': 'Вид ТОРО', 'downtime_status': 'Простой, час'})
  downtime_by_catagories_2024_data.to_csv('widget_data/downtime_by_categiries_2024_data.csv', index = False)

  ############# 2025 год ###################
  model_hours_df_2025 = model_hours_df.loc[model_hours_df['year'] == 2025] 
  model_hours_df_2025 = model_hours_df.loc[model_hours_df['downtime_status'] == 1] 

  model_hours_df_2025_groupped = model_hours_df_2025.groupby(['maintanance_name'], as_index=False)[['downtime_status']].sum()
  downtime_by_catagories_2025_data = model_hours_df_2025_groupped.rename(columns={'maintanance_name': 'Вид ТОРО', 'downtime_status': 'Простой, час'})
  downtime_by_catagories_2025_data.to_csv('widget_data/downtime_by_categiries_2025_data.csv', index = False)

  return downtime_by_catagories_2023_data, downtime_by_catagories_2024_data, downtime_by_catagories_2025_data
  
def job_codes_prep():
  """обработка кодов работ из maintanance_job_list_general"""
  maintanance_job_list_general = maintanance_job_list_general_func()
  
  job_codes_df = maintanance_job_list_general.groupby(['maintanance_category_id',	'maintanance_name',	'maintanance_category_sort_index'], as_index = False)['strategy_id'].sum()
  job_codes_df = job_codes_df.loc[:, ['maintanance_category_id',	'maintanance_name',	'maintanance_category_sort_index']]
  job_codes_df.to_csv('data/job_codes.csv', index = False)



################# ЗАПУСК ФУНКЦИЙ #############################



# full_eo_list_actual_func()
# select_eo_for_calculation()
# full_eo_list_func()
# last_maint_date_func()
# functions.pass_interval_fill() '''создание списка pass interval в maintanance_job_list_general'''
# pass_interval_fill()
  

# functions.maintanance_category_prep() """Создание файла со списком категорий работ ТОИР"""
# maintanance_category_prep()


# functions.eo_job_catologue():'''создание файла eo_job_catologue: список оборудование - работа на оборудовании'''
# eo_job_catologue()


# func_maintanance_jobs_df_prepare.maintanance_jobs_df_prepare()
# maintanance_jobs_df()
# fill_calendar_fond()

# hour_calculation()