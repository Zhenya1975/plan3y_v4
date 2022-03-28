import pandas as pd
import initial_values
import json

def model_eo_select_data_prep(be_filter):
  with open('saved_filters.json', 'r') as openfile:
    # Reading from json file
    saved_filters_dict = json.load(openfile)
  # print("saved_filters_dict", saved_filters_dict)
  
  filter_model_eo_data_df = pd.read_csv('widget_data/filter_model_eo.csv')
  filter_model_eo_data_df = filter_model_eo_data_df.loc[filter_model_eo_data_df['level_1'].isin(be_filter)]
  filter_model_eo_data_df = filter_model_eo_data_df.groupby(['eo_model_id','eo_model_name'], as_index = False)['eo_model_name'].size()
  filter_model_eo_data_df = filter_model_eo_data_df.loc[:, ['eo_model_id','eo_model_name']]
  
  model_eo_filter_checklist_data = []
  model_eo_filter_values = []
  for index, row in filter_model_eo_data_df.iterrows():
    dict_temp = {}
    eo_model_id = row['eo_model_id']
    eo_model_name = row['eo_model_name']
    dict_temp['label'] = eo_model_name
    dict_temp['value'] = eo_model_id
    model_eo_filter_checklist_data.append(dict_temp)
    model_eo_filter_values.append(eo_model_id)
    
  # print("model_eo_filter_values", model_eo_filter_values)
  return model_eo_filter_checklist_data, model_eo_filter_values

# model_eo_select_data_prep(['first11'])
