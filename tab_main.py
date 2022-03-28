from dash import dcc, html
import dash_bootstrap_components as dbc
loading_style = {
    # 'position': 'absolute',
    # 'align-self': 'center'
                 }
def maintanance_chart_tab():
    maintanance_chart_tab_block = dcc.Tab(
        label='КТГ',
        value='ktg',
        children=[
           
          dcc.Loading(id='loading', parent_style=loading_style),
          dcc.Loading(id='loading1', parent_style=loading_style),
            html.Div(
              html.P(),
            ),
          
            html.Div(
            dbc.Row([
                # колонка с фильтрами
                dbc.Col(width=2,
                  children=[
                    html.P(id="empty"), 
                    # dbc.Button("Обновить", id="btn_update", size="sm",
                    #                              style={'marginBottom': '3px',
                    #                                     'marginTop': '3px',
                    #                                     'backgroundColor': '#232632'},),
                    html.Hr(),
                    
                    html.P(),
                    html.Div([
                      "Бизнес единица",
                      dcc.Dropdown(id="checklist_level_1", multi=True, optionHeight = 50),
                  ]),
                    html.P(),
                    html.Div([
                      "Модель ЕО",
                      dcc.Dropdown(id="model_eo_filter", multi=True, optionHeight = 50),
                  ]),
                    


                    
                  
                  # html.P(),
                  #   html.Div([
                  #     "ЕО",
                  #     dcc.Dropdown(id="checklist_eo", multi=True, optionHeight = 50),
                  # ]),
                    # html.Hr(),
                    # html.P('Категории работ'),
                    # html.Div(style={'marginLeft': '3px'},
                    #                  children=[
                    #                      dbc.Button("Выбрать все", size="sm",
                    #                                 id="select_all_maintanance_category_checklist",
                    #                                 style={'marginBottom': '3px',
                    #                                        'marginTop': '3px',
                    #                                        'backgroundColor': '#232632'}
                    #                                 ),
                    #                      dbc.Button("Снять выбор", color="secondary",
                    #                                 size="sm",
                    #                                 style={'marginBottom': '3px',
                    #                                        'marginTop': '3px',
                    #                                        'backgroundColor': '#232632'},
                    #                                 id="release_all_maintanance_category_checklist"),

                    #                      html.P(),
                    #                      dcc.Checklist(
                    #                          id='maintanance_category_checklist',
                    #                          # options=regions,
                    #                          # value=regions_list,
                    #                          labelStyle=dict(display='block')),
                    #                      html.Hr(className="hr"),
                    #                  ]
                    #            ),
                  ]
                ),
                dbc.Col(width=10,
                    children=[
                            dbc.Row([
                              dbc.Col(
                                children = [
                                  html.H5(id = 'be_title_id'),
                                  # html.P(),
                                  html.H5(id = 'level_upper_title_id'),
                                  html.H5(id = 'number_of_eo_title_id')

                                ]
                                
                          )
                        ]),
                        dbc.Row(justify="start",
                          children = [
                              dbc.Col(width = 4,
                                children = [
                                  dbc.Card(
                                    dbc.CardBody(
                                        [
                                            html.H4("2023 год", className="card-title"),
                                            #html.H6("Card subtitle", className="card-subtitle"),
                                            html.P(id = 'eo_qty_2023'),
                                            html.P(id = 'ktg_2023'),

                                           
                                        ]
                                    ),
                                    style={"width": "18rem"},

                                  )

                                ],
                 
                              ),
                              dbc.Col(width = 4,
                                      children = [
                                  dbc.Card(
                                    dbc.CardBody(
                                        [
                                            html.H4("2024 год", className="card-title"),
                                            #html.H6("Card subtitle", className="card-subtitle"),
                                            html.P(id = 'eo_qty_2024'),
                                            html.P(id = 'ktg_2024'),

                                           
                                        ]
                                    ),
                                    style={"width": "18rem"},

                                  )

                                ],
                              
                              ),
                              dbc.Col(width = 4,
                                      children = [
                                  dbc.Card(
                                    dbc.CardBody(
                                        [
                                            html.H4("2025 год", className="card-title"),
                                            #html.H6("Card subtitle", className="card-subtitle"),
                                            html.P(id = 'eo_qty_2025'),
                                            html.P(id = 'ktg_2025'),

                                           
                                        ]
                                    ),
                                    style={"width": "18rem"},

                                  )

                                ],
                              
                              )
                        ],
                          
                               ),
                      html.Hr(),
                      ###################### ряд с кнопками выгрузки таблиц в эксель ####################
                      html.Div(
                        dbc.Row([
                          dbc.Col(width=2,
                                  children=[
                                    html.Div([
                                      dbc.Button("Выгрузить ЕО в выборке xlsx", id="btn_download_eo_table", size="sm",
                                                 style={'marginBottom': '3px',
                                                        'marginTop': '3px',
                                                        'backgroundColor': '#232632'},),
                                      dcc.Download(id="download_excel_eo_table")
                                    ])
                                  ]
                                 ),
                      
                          # dbc.Col(width=2,
                          #         children=[
                          #           html.Div([
                          #             dbc.Button("Выгрузить ТОИР воздействия, xlsx", id="btn_download_maint_jobs_table", size="sm",
                          #                        style={'marginBottom': '3px',
                          #                               'marginTop': '3px',
                          #                               'backgroundColor': '#232632'},),
                          #             dcc.Download(id="download_excel_maint_jobs_table")
                          #           ])
                          #         ]
                          #        ),
                          dbc.Col(width=2,
                                  children=[
                                    html.Div([
                                      dbc.Button("Выгрузить КТГ, xlsx", id="btn_download_ktg_table", size="sm",
                                                 style={'marginBottom': '3px',
                                                        'marginTop': '3px',
                                                        'backgroundColor': '#232632'},),
                                      dcc.Download(id="download_excel_ktg_table")
                                    ])
                                  ]
                                 ),
                          dbc.Col(width=2,
                                  children=[
                                    html.Div([
                                      dbc.Button("Выгрузить простои Виды воздейств./ месяцы года, xlsx", id="btn_download_p11_table", size="sm",
                                                 style={'marginBottom': '3px',
                                                        'marginTop': '3px',
                                                        'backgroundColor': '#232632'},),
                                      dcc.Download(id="download_excel_p11_table")
                                    ])
                                  ]
                                 ),
                        ])
                      ),

                      #######################################################
                      html.Div(
                        dbc.Row([
                      
                          dbc.Col(width=12,
                            children=[
                              html.P(),
                              dcc.Graph(id='planned_downtime', config={'displayModeBar': False}),
                            ]),
                        
                       ]),
                      # style={"background-color": "#ABBAEA"},
                      ), 
                      #############################################################
                      html.Div(
                        dbc.Row([
                      
                          dbc.Col(width=12,
                            children=[
                              html.P(),
                              dcc.Graph(id='fig_ktg_3y_by_months_id', config={'displayModeBar': False}),
                            ]),
                         
                       ]),
                      # style={"background-color": "#ABBAEA"},
                      ), 

                      #############################################################
                      # html.Div(
                      #   dbc.Row([
                      #      dbc.Col(width=4,
                      #       children=[
                      #         html.P(),
                      #         dcc.Graph(id='planned_downtime_piechart_2023', config={'displayModeBar': False}),
                      #       ]),
                      #     dbc.Col(width=4,
                      #       children=[
                      #         html.P(),
                      #         dcc.Graph(id='planned_downtime_piechart_2024', config={'displayModeBar': False}),
                      #       ]),
                      #     dbc.Col(width=4,
                      #       children=[
                      #         html.P(),
                      #         dcc.Graph(id='planned_downtime_piechart_2025', config={'displayModeBar': False}),
                      #       ]),
                      #   ])
                      # ),
                       
                       html.Hr(),
                       html.P(),
                      
                
                      html.Div(id='ktg_by_month_table'),
                      
                      html.P(),
                      html.Div(id='p11_table'),
                      
                      html.P(),
                      html.Div(
                        dbc.Row([
                      
                          dbc.Col(width=12,
                            children=[
                              html.P(),
                              dcc.Graph(id='fig_man_hours', config={'displayModeBar': False}),
                            ]),
                         
                       ]),
                      # style={"background-color": "#ABBAEA"},
                      ),




                      
                      # html.P(),
                       # dcc.Graph(id='ktg_by_month', config={'displayModeBar': False}),
                       # html.Hr(),
                       # html.P(),
                       # dcc.Graph(id='ktg_by_weeks', config={'displayModeBar': False}),
                    ]),
                
            ]),
            )
            #dbc.Row([
                
            #]),
        
       
          ]
    )
    return maintanance_chart_tab_block