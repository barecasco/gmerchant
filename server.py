import dash
from dash import dcc, html, dash_table, Input, Output, State
import pandas as pd
import sqlite3
import database as db
import io
import os
import sys
import utils
from dash import dcc, html
import plotly.express as px
import plotly.graph_objects as go
from dash.dependencies import Input, Output
import pandas as pd
import numpy as np
import invoice_layout_generator as inlay
import datetime
from fill_tables import replenish_table

strptime  = datetime.datetime.strptime
date      = datetime.date
timedelta = datetime.timedelta


# Initialize database if not exist
if os.path.exists(db.database_file):
    print("database exist")
else:
    print("database is missing, creating database...")
    replenish_table()


# Define dark theme colors
colors = {
    'background'    : '#111111',
    'text'          : '#FFFFFF',
    'grid'          : '#333333',
    'plot_bgcolor'  : '#222222',
    'paper_bgcolor' : '#222222',
    'accent'        : '#1E88E5',  # Blue accent color
}


# Define styles for layout
styles = {
    'container': {
        'max-width'         : '900px',
        'margin'            : '0 auto',
        'padding'           : '20px',
        'fontFamily'        : 'Segoe UI, Roboto, Helvetica Neue, Arial, sans-serif'
    },
    'card': {
        'max-width'         : '850px',
        # 'border'            : '1px solid #777777',
        # 'borderRadius'      : '5px',
        'padding'           : '20px',
        'paddingBottom'     : '5px',
        'marginBottom'      : '0px',
        'marginLeft'        : 'auto',
        'marginRight'       : 'auto',    
        'marginTop'         : '0px',
        'backgroundColor'   : '#222222',
    },
    'formRow': {
        'display'           : 'flex',
        'flex-direction'    : 'row',
        'flexWrap'          : 'wrap',
        'marginBottom'      : '15px',
        'gap'               : '0px',
        'align-items'       : 'flex-start'
    },
    'formItem': {
        'flex'          : '1',
        # 'minWidth'    : '150px',
        # 'maxWidth'    : '150px',
        'marginRight'   : '10px',
    },

    'formItemMed': {
        'flex'          : '1',
        # 'minWidth'    : '150px',
        # 'maxWidth'    : '150px',
        'marginRight'   : '10px',
    },


    'formItemLarge': {
        'flex'          : '2',
        # 'minWidth'    : '150px',
        # 'maxWidth'    : '150px',
        'marginRight'   : '10px',
    },

    'label': {
        'display'       : 'block',
        'marginBottom'  : '10px',
        'fontWeight'    : 'normal',
        'font-size'     : '13px',
    },

    'input': {
        'width'     : '72%',
        'padding'   : '8px',
        # 'borderRadius': '4px',
        # 'marginRight' : '10px',
        # 'border'            : '1px solid #777777',
        'border'            : 'none',
        'backgroundColor'   : '#151515',
        'color'             : '#bbbbbb'
    },

    'datepicker': {
        # 'padding'           : '8px',
        # 'borderRadius'      : '4px',
        # 'marginRight' : '10px',
        'border'            : 'none',
        # 'backgroundColor'   : '#222222',
        # 'color'             : '#bbbbbb'
    },
    
    'button-submit-query': {
        'backgroundColor'   : '#c1e2b3',
        'color'             : '#111',
        'padding'           : '8px',
        'borderRadius'      : '3px',
        'border'            : '1px solid #a1a2a3',
        'cursor'            : 'pointer',
        'fontSize'          : '14px',
        'marginTop'         : '24px',
        'width'             : '100%'
    },
    'error': {
        'color': '#dc3545',
        'marginTop': '10px'
    },
    'invoice-summary': {
        'margin-top': '12px',
        'fontSize'  : '12px',
        'width'     : '85%',
        'color'     : '#bbbbbb',
        'fontWeight': 'medium',
        'fontFamily': 'Fira Mono, monospace',
        'textAlign': 'right',
    },
}


# Initialize the Dash app
app = dash.Dash(__name__, title='Delivery Analysis', suppress_callback_exceptions=True)


# ------------------------------------------------------------------------------------
# LAYOUT
# ------------------------------------------------------------------------------------
# App layout
app.layout = html.Div(
    [
        dcc.Location(id='url', refresh=False),
        html.Div(id='page-load-trigger', style={'display': 'none'}),
        html.Div(id='replenish-output'),
        html.Link(
            rel="stylesheet",
            href="/assets/style.css"
        ),

        html.H1("Report and Tracker", style={
            'textAlign'     : 'center',
            'marginBottom'  : '0px',
            'marginTop'     : '50px',
            'font-weight'   : 'normal',
            'margin-bottom' : '10px'
        }),
        html.Hr(style={
            'width': '30%', 
            'border': '1px solid #444444', 
            'background-color': '#444444', 
            'margin-bottom': '50px'
        }),

        # ----------------------------------------------------------------------------------
        # RESTOCK REPORT
        # ----------------------------------------------------------------------------------
        html.H3("Restock Report", style={'font-weight': 'normal', 'margin-left':'0px'}),
        html.Div(
            [
                html.Div(
                    [
                        dcc.Textarea(
                            id='restock-report-input',
                            value='',  # Default query
                            style={
                                'width': '100%', 
                                'height': 350, 
                                'whiteSpace': 'nowrap',
                                'overflowX': 'auto', 
                                'padding': '10px',
                                'background-color': '#151515',
                                'color': '#dddddd',
                                'resize' : 'none',
                                'border' : 'none',
                                'outline': 'none',
                                'font-size': '11px',
                                'borderRadius' : '0px'
                            }
                        ),
                        
                        # Submit button
                        html.Button( 
                            'submit', 
                            id='submit-restock-report', 
                            n_clicks=0,
                            style={
                                'width': '106%', 
                                'height': 30, 
                                'margin-top': '10px',
                                # 'margin-left': 'auto',
                                'margin-right': 'auto',
                                'border'            : 'none',
                                'borderRadius'      : '0px',
                                'background-color'  : '#333333',
                                'color'             : '#bbbbbb'
                            }
                        ),

                        html.Button( 
                            'download table', 
                            id='download-restock-table', 
                            n_clicks=0,
                            style={
                                'width': '106%', 
                                'height': 30, 
                                'margin-top': '10px',
                                'border'            : 'none',
                                'borderRadius'      : '0px',
                                'background-color'  : '#333333',
                                'color'             : '#bbbbbb'
                            }
                        ),
                            # Output messages (errors, etc.)
                        html.Div(id='restock-message', style={'margin-top': '10px', 'color': 'skyblue'}),
                        dcc.Download(id="download-restock-excel"),
                    ], 
                    style = {
                        'width'         : '20%', 
                        'margin-right'  : '50px', 
                        'display'       : 'flex', 
                        'flex-direction': 'column'
                    }
                ),

                html.Div(
                    [
                        dash_table.DataTable(
                            id='restock-table',
                            style_as_list_view=True,
                            style_table = {
                                'overflowX': 'auto', 
                                'overflowY': 'auto', 
                                'height'   : 500,
                                'background-color': '#222222',
                                'color': '#aaaaaa',                           
                            },
                            style_cell={
                                'textAlign'     : 'left',
                                'fontSize'      : 11,
                                'padding-left'  : '10px',
                                'padding-right'  : '10px',
                                'height'        : 'auto',
                                'background-color': '#222222',
                                'color': '#aaaaaa',
                            },
                            style_header={
                                'fontWeight'     : 'bold',
                                'padding-top'    : '5px',
                                'padding-bottom' : '5px',
                                'fontSize'      : 11,
                                'maxWidth'      : '150px',
                                'whiteSpace'    : 'normal',
                                'overflow'      : 'hidden',
                                'textOverflow'  : 'ellipsis',
                                'font-weight' : 'normal',
                                'color': '#aaaaaa',

                            },
                            page_size=20
                        ),

                    ], 
                    style = {
                        'width':'75%',
                        'margin-top': '0px'
                    },

                )

            ],
            
            style = {
                'margin-top'    : '10px', 
                'margin-bottom' : '10px', 
                'display'       : 'flex', 
                'flex-direction': 'row',
                'width' : '100%',            
            }
        ),


        # ----------------------------------------------------------------------------------
        # DELIVERY REPORT
        # ----------------------------------------------------------------------------------

        html.H3("Delivery Report", style={'font-weight': 'normal', 'margin-left':'0px'}),
        html.Div(
            [
                html.Div(
                    [
                        dcc.Textarea(
                            id='delivery-report-input',
                            value='',  # Default query
                            style={
                                'width': '100%', 
                                'height': 450, 
                                'whiteSpace': 'nowrap',
                                'background-color': '#151515',
                                'color': '#dddddd',
                                'overflowX': 'auto', 
                                'padding': '10px',
                                'resize' : 'none',
                                'border' : 'none',
                                'outline': 'none',
                                'font-size': '11px',
                                'borderRadius' : '0px'
                            }
                        ),
                        
                        # Submit button
                        html.Button( 
                            'submit', 
                            id='submit-delivery-report', 
                            n_clicks=0,
                            style={
                                'width': '106%', 
                                'height': 30, 
                                'margin-top': '10px',
                                # 'margin-left': 'auto',
                                'margin-right': 'auto',
                                'border'            : 'none',
                                'borderRadius'      : '0px',
                                'background-color'  : '#333333',
                                'color' : '#bbbbbb'
                            }
                        ),

                        html.Button( 
                            'download table', 
                            id='download-delivery-table', 
                            n_clicks=0,
                            style={
                                'width'             : '106%', 
                                'height'            : 30, 
                                'margin-top'        : '10px',
                                'border'            : 'none',
                                'borderRadius'      : '0px',
                                'background-color'  : '#333333',
                                'color'             : '#bbbbbb'
                            }
                        ),
                        # Output messages (errors, etc.)
                        html.Div(id='delivery-message', style={'margin-top': '10px', 'color': 'skyblue'}),
                        dcc.Download(id="download-delivery-excel"),
                    ], 
                    style = {
                        'width'         : '20%', 
                        'margin-right'  : '50px', 
                        'display'       : 'flex', 
                        'flex-direction': 'column'
                    }
                ),

                html.Div(
                    [
                        dash_table.DataTable(
                            id='delivery-table',
                            style_as_list_view=True,
                            style_table = {
                                'overflowX': 'auto', 
                                'overflowY': 'auto', 
                                'height'   : 590,
                                'background-color': '#222222',
                                'color': '#aaaaaa',
                            },
                            style_cell={
                                'textAlign'       : 'left',
                                'fontSize'        : 11,
                                'padding-left'    : '10px',
                                'padding-right'   : '10px',
                                'height'          : 'auto',
                                'background-color': '#222222',
                                'color'           : '#aaaaaa',
                                # 'minWidth'      : '150px', 
                                # 'width'         : '400px', 
                            },
                            style_header={
                                # 'backgroundColor': 'lightgrey',
                                'fontWeight'      : 'bold',
                                'padding-top'     : '5px',
                                'padding-bottom'  : '5px',
                                'fontSize'        : 11,
                                'maxWidth'        : '150px',
                                'whiteSpace'      : 'normal',
                                'overflow'        : 'hidden',
                                'background-color': '#222222',
                                'textOverflow'    : 'ellipsis',
                                'font-weight'     : 'normal',
                                'color'           : '#aaaaaa',
                            },
                            page_size=17,
                        ),

                    ], 
                    style = {
                        'width':'75%',
                        'margin-top': '0px'
                    },
                ),
            ],
            
            style = {
                'margin-top'    : '10px', 
                'margin-bottom' : '10px', 
                'display'       : 'flex', 
                'flex-direction': 'row',
                'width' : '100%',            
            }
        ),


        # ----------------------------------------------------------------------------------
        # TRACKER REPORT
        # ----------------------------------------------------------------------------------
        html.H3("Stock Consumption Report", style={'font-weight': 'normal', 'margin-left':'0px'}),

        html.Div(
            style = {
                'display'       : 'flex',
                'flexDirection' : 'row',
                'gap'           : '20px',
                'flexWrap'      : 'wrap'
            }, 
            children = 
            [
                # Left side - Selector panel
                html.Div(
                    style = {
                        'flex'              : '1',
                        'backgroundColor'   : colors['plot_bgcolor'],
                        'color'             : '#ffffff',
                        'padding'           : '20px',
                        'borderRadius'      : '0px',
                        'minWidth'          : '250px'
                    }, 
 
                    children = [
                        html.Button( 
                            'update tracker', 
                            id          = 'submit-update-tracker', 
                            n_clicks    = 0,
                            style       = {
                                'width'             : '100%', 
                                'height'            : 40, 
                                'margin'            : 'auto',
                                'marginBottom'      : '10px',
                                'border'            : 'none',
                                'borderRadius'      : '0px',
                                'background-color'  : '#333333',
                                'color'             : '#bbbbbb'
                            }
                        ),

                        html.H4("Select transport", style={'font-weight': 'normal', 'color':'#dddddd'}),

                        dcc.Dropdown(
                            id='transport-selector',
                            options=[
                                {'label': 'Value 1', 'value': 'R4J1N'},
                            ],
                            value = 'R4J1N',  # Default value
                            style = {
                                'backgroundColor': '#333333',
                                # 'color'          : "#ffffff",
                                'border'         : 'none',
                                'border-radius'  : '0px',
                                'font-size'      : '11px'
                            }
                        ),

                        html.Div(style={'height': '20px'}),  # Spacer
                    ]
                ),
            
                # Right side - Graph panel
                html.Div(
                    style = 
                    {
                        'flex'              : '4',
                        'backgroundColor'   : colors['plot_bgcolor'],
                        'padding'           : '0px',
                        'marginTop'         : '-25px',
                        'borderRadius'      : '0px',
                        'minWidth'          : '500px'
                    }, 
                    children = [
                        dcc.Graph(
                            id      = 'tracker-graph',
                            style   = {'height': '70vh'},
                            config  = {
                                'displayModeBar': False, 
                                'scrollZoom'    : False
                            }
                        )
                    ]
                )
        ]),


    html.H1("Create Invoice", style = {
        'textAlign'     : 'center',
        'marginBottom'  : '0px',
        'marginTop'     : '140px',
        'font-weight'   : 'normal',
        'margin-bottom' : '10px'
    }),
    html.Hr( style={
        'width': '30%', 
        'border': '1px solid #444444', 
        'background-color': '#444444', 
        'margin-bottom': '40px'
    }),
    # ----------------------------------------------------------------------------------
    # INVOICE GENERATOR
    # ----------------------------------------------------------------------------------
    html.Div(style=styles['card'], children=[
        html.Div(style=styles['formRow'], children=[

            html.Div([
                html.Label("customer id", style=styles['label']),
                dcc.Input(
                    id      ="customer-id-input",
                    type    ="text",
                    value   = "0110005",
                    style   = styles['input']
                )
            ],
            style={
                    'flex'          : '1',
                    'marginRight'   : '0px',            
                    # 'border'        : '1px solid #ddd',
            }),
            
            html.Div(
                [
                    html.Label("correction volume", style=styles['label']),
                    dcc.Input(
                        id          = "minvol-balance-input",
                        type        = "text",
                        placeholder = "0",
                        value       = "0",
                        style       = styles['input']
                    )
                ],
                style={
                    'flex'          : '1',
                    'marginRight'   : '0px',
                    # 'border'   : '1px solid #ddd',
                }
            ),


            html.Div(
                [
                    html.Label("start date", style=styles['label']),
                    dcc.Input(
                        id          = "start-date-input",
                        type        = "text",
                        value       = "2025-01-01",
                        placeholder = "yyyy-mm-dd",
                        style       = styles['input']
                    )
                ],
                style = {
                    'flex' : '1',
                    # 'marginRight'   : '-10px',
                    # 'border'        : '1px solid #ddd',

                }
            ),
            
            html.Div(
                [
                    html.Label("end date", style=styles['label']),
                    dcc.Input(
                        id          = "end-date-input",
                        type        = "text",
                        value       = "2025-02-28",
                        placeholder = "yyyy-mm-dd",
                        style       = styles['input']
                    )
                ],
                style={
                    'flex' : '1',
                    # 'marginRight'   : '-10px',
                    # 'border'        : '1px solid #ddd',
                }
            ),

            html.Div(
                [
                    html.Button(
                        "analyze charges", 
                        id="submit-button", 
                        style = {
                                'width': '100%', 
                                'height': 30, 
                                'margin-top'        : '25px',
                                # 'margin-left': 'auto',
                                'margin-right'      : 'auto',
                                'border'            : '0px solid #777777',
                                'borderRadius'      : '0px',
                                'background-color'  : '#333333',
                                'color' : '#bbbbbb'
                        }
                    )
                ],
                style={
                    'flex' : '1',
                    'margin': 'auto',
                }
            ),


        ]),
    ]),


    html.Div(style=styles['card'], children=[
        html.Div(style=styles['formRow'], children=[

            html.Div([
                html.Label("invoice number", style=styles['label']),
                dcc.Input(
                    id="invoice-number-input",
                    type="text",
                    placeholder="0001/SEM/I/2024",
                    style = styles["input"]
                )
            ],
            style={
                    'flex'          : '1',
                    'marginRight'   : '0px',            
                    # 'border'        : '1px solid #ddd',
            }),


            html.Div([
                html.Label("week period", style=styles['label']),
                dcc.Input(
                    id="week-period-input",
                    type="text",
                    placeholder="W1 Jan 2025",
                    style = styles['input']
                )
            ],
            style={
                    'flex'          : '1',
                    'marginRight'   : '0px',            
                    # 'border'        : '1px solid #ddd',
            }),


            html.Div([
                html.Label("customer address", style=styles['label']),
                        dcc.Textarea(
                            id='customer-address-input',
                            value='',  # Default query
                            style={
                                'width': '85%', 
                                'height': 120, 
                                'whiteSpace': 'nowrap',
                                'overflowX': 'auto', 
                                'padding': '10px',
                                'background-color': '#151515',
                                'color': '#dddddd',
                                'resize' : 'none',
                                'border' : 'none',
                                'outline': 'none',
                                'font-size': '11px',
                                'borderRadius' : '0px'
                            }
                        )
            ],
            style={
                    'flex'          : '2',
                    'marginRight'   : '0px',            
                    # 'border'        : '1px solid #ddd',
            }),

            
            html.Div(
                [
                    html.Button(
                        "generate invoice", 
                        id="generate-invoice-button", 
                        style = {
                                'width': '100%', 
                                'height': 30, 
                                'margin-top': '10px',
                                # 'margin-left': 'auto',
                                # 'margin-right': 'auto',
                                'border'            : '0px solid #777777',
                                'borderRadius'      : '0px',
                                'background-color'  : '#333333',
                                'color' : '#bbbbbb'
                        }
                    )
                ],
                style={
                    'flex' : '1',
                    'margin-top': '17px',
                }
            ),

        ]),
    ]),

    
    # Div for displaying the table
    html.Div(
        children    = [
            html.Div(id='invoice-message', 
                style={
                    'font-size'     : '12px', 
                    'margin'        : 'auto', 
                    'margin-bottom' : '15px' , 
                    'color'         : 'skyblue',
                    'text-align'    : 'center'
                }
            ),
            dash_table.DataTable(
                id='customer-table',
                style_as_list_view=True,
                style_table = {
                    'overflowX': 'auto', 
                    'overflowY': 'auto', 
                    'width'   : '70%',
                    'margin'  : 'auto',
                    'background-color': '#222222',
                    'color': '#aaaaaa',
                },
                style_cell={
                    'textAlign'       : 'left',
                    'fontSize'        : 11,
                    'padding-left'    : '10px',
                    'padding-right'   : '10px',
                    'height'          : 'auto',
                    'background-color': '#222222',
                    'color'           : '#aaaaaa',
                    # 'minWidth'      : '150px', 
                    # 'width'         : '400px', 
                },
                style_header={
                    # 'backgroundColor': 'lightgrey',
                    'fontWeight'      : 'bold',
                    'padding-top'     : '5px',
                    'padding-bottom'  : '5px',
                    'fontSize'        : 11,
                    'maxWidth'        : '150px',
                    'whiteSpace'      : 'normal',
                    'overflow'        : 'hidden',
                    'background-color': '#222222',
                    'textOverflow'    : 'ellipsis',
                    'font-weight'     : 'normal',
                    'color'           : '#aaaaaa',
                },
                page_size=20,
            ),

            html.Div(id='pretotal_volume-message', style=styles['invoice-summary']),
            html.Div(id='pretotal_price-message', style=styles['invoice-summary']),
            html.Div(id='volume_balance-message', style=styles['invoice-summary']),
            html.Div(id='price_balance-message', style=styles['invoice-summary']),
            html.Div(id='total_volume-message', style=styles['invoice-summary']),
            html.Div(id='total_price-message', style=styles['invoice-summary']),
            html.Div(id='total_price_wtax-message', style=styles['invoice-summary']),
        ],

        style = {
            'width':'100%',
            'margin-top': '50px'
        },
    ),


    dcc.Download(id="download-invoice-excel"),

    ],

    style={
        'padding'           : '30px', 
        'font-family'       : 'Verdana, Helvetica, Arial',
        'backgroundColor'   : '#222222',
        'color'             : '#dddddd',
        'margin'            : '-10px'
    }
)


# ------------------------------------------------------------------------------------
# REPLENISH TABLE ON RELOAD BROWSER
# ------------------------------------------------------------------------------------
# @app.callback(
#     [
#         Output('delivery-table', 'data'),
#         Output('delivery-table', 'columns'),
#         Output('restock-table', 'data'),
#         Output('restock-table', 'columns'),
#     ],
#     Input('url', 'pathname'),
#     prevent_initial_call=False  # Important: This ensures the callback runs on initial page load
# )
# def replenish_db_on_page_load(pathname):
#     print("Page loaded/reloaded - running initialization function!")    
#     replenish_table()

#     query   = "SELECT * FROM delivery"
#     df      = db.query_table_as_pandas(db.database_file, query)
#     if len(df) == 0:
#         return [], [], "invalid report format."            
#     df = utils.remove_df_underscore(df)
#     # Prepare data for DataTable
#     delivery_data    = df.to_dict('records')
#     delivery_columns = [{"name": i, "id": i} for i in df.columns]        


#     query = "SELECT * FROM restock"
#     df    = db.query_table_as_pandas(db.database_file, query)

#     if len(df) == 0:
#         return [], [], "Please enter your report."            

#     df = utils.remove_df_underscore(df)

#     # Prepare data for DataTable
#     restock_data    = df.to_dict('records')
#     restock_columns = [{"name": i, "id": i} for i in df.columns]        



#     return delivery_data, delivery_columns, restock_data, restock_columns


# ------------------------------------------------------------------------------------
# DELIVERY INSERT
# ------------------------------------------------------------------------------------
# Callback to execute delivery insertion and update table
@app.callback(
    [Output('delivery-table', 'data'),
     Output('delivery-table', 'columns'),
     Output('delivery-message', 'children')],
    [Input('submit-delivery-report', 'n_clicks')],
    [State('delivery-report-input', 'value')],
    prevent_initial_call=False,
)
def execute_delivery_update(n_clicks, report):
    if n_clicks == 0 or not report:
        # return [], [], "Please enter your report."
        select_all_query = "SELECT * FROM delivery"
        df               = db.query_table_as_pandas(db.database_file, select_all_query)

        if len(df) == 0:
            return [], [], "invalid report format."            

        df = utils.remove_df_underscore(df)

        # Prepare data for DataTable
        data    = df.to_dict('records')
        columns = [{"name": i, "id": i} for i in df.columns]        

        return data, columns, f""
    
    
    try:
        rowrep = db.generate_delivery_rowrep(report)
        db.insert_row_from_dict(db.database_file, "delivery", rowrep)
        
        # Execute query and fetch results into a DataFrame
        select_all_query = "SELECT * FROM delivery"
        df               = db.query_table_as_pandas(db.database_file, select_all_query)
        df               = utils.remove_df_underscore(df)

        # Prepare data for DataTable
        data    = df.to_dict('records')
        columns = [{"name": i, "id": i} for i in df.columns]
        
        return data, columns, f"Insert executed successfully. {len(data)} rows returned."
    
    except Exception as e:
        return [], [], f"Error executing query: {str(e)}"


@app.callback(
    Output("download-delivery-excel", "data"),
    Input("download-delivery-table", "n_clicks"),
    prevent_initial_call=True,
)
def generate_delivery_excel(n_clicks):
    # Create an Excel file in memory
    output = io.BytesIO()

    select_all_query = "SELECT * FROM delivery"
    df               = db.query_table_as_pandas(db.database_file, select_all_query)

    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        # Convert the DataFrame to an Excel file
        df.to_excel(writer, sheet_name='Sheet1')
        
        # Get the xlsxwriter workbook and worksheet objects
        workbook = writer.book
        worksheet = writer.sheets['Sheet1']
    
        # Create a cell format with text wrapping enabled
        header_format = workbook.add_format({
            'text_wrap': True,  # Enable text wrapping
            'valign'   : 'vcenter',    
            'align'    : 'center',    
            'bold'     : True      
        })
        
        # Apply the multiline format to the header row (row 0)
        for col_num, value in enumerate(df.columns.values):
            # +1 to account for the index column
            worksheet.write(0, col_num + 1, value, header_format)
    
        # Set row height for the header row to accommodate multiple lines
        worksheet.set_row(0, 40)  # Height in points
        
        # Set the column width based on the maximum length in each column
        colnames  = list(df.columns)
        widthlist = []
        for col in colnames:
            max_len = max(df[col].astype(str).map(len).max(), len(col))
            widthlist.append(max_len + 2)
        
        for idx, col in enumerate(df.columns):
            colwidth = widthlist[idx]
            worksheet.set_column(idx + 1, idx + 1, colwidth)
        
        # Also adjust the index column (column 0)
        max_index_len = max(len(str(i)) for i in df.index)
        index_width   = max(max_index_len, len(str(df.index.name) if df.index.name else '')) + 2
        worksheet.set_column(0, 0, index_width)
    
    # Return the file as a download
    return dcc.send_bytes(output.getvalue(), "delivery.xlsx")




# --------------------------------------------------------------------------------------------------
# RESTOCK INSERT
# --------------------------------------------------------------------------------------------------
# Callback to execute delivery insertion and update table
@app.callback(
    [Output('restock-table', 'data'),
     Output('restock-table', 'columns'),
     Output('restock-message', 'children')],
    [Input('submit-restock-report', 'n_clicks')],
    [State('restock-report-input', 'value')],
    prevent_initial_call=False,
)
def execute_restock_update(n_clicks, report):
    if n_clicks == 0 or not report:
        # return [], [], "Please enter your report."
        select_all_query = "SELECT * FROM restock"
        df               = db.query_table_as_pandas(db.database_file, select_all_query)

        if len(df) == 0:
            return [], [], "Please enter your report."            

        df = utils.remove_df_underscore(df)

        # Prepare data for DataTable
        data    = df.to_dict('records')
        columns = [{"name": i, "id": i} for i in df.columns]        
        return data, columns, f""
        
    try:
        rowrep = db.generate_restock_rowrep(report)
        db.insert_row_from_dict(db.database_file, "restock", rowrep)
        
        # Execute query and fetch results into a DataFrame
        select_all_query = "SELECT * FROM restock"
        df               = db.query_table_as_pandas(db.database_file, select_all_query)
        df               = utils.remove_df_underscore(df)

        # Prepare data for DataTable
        data    = df.to_dict('records')
        columns = [{"name": i, "id": i} for i in df.columns]
        
        return data, columns, f"Insert executed successfully. {len(data)} rows returned."
    
    except Exception as e:
        return [], [], f"Error executing query: {str(e)}"


@app.callback(
    Output("download-restock-excel", "data"),
    Input("download-restock-table", "n_clicks"),
    prevent_initial_call=True,
)
def generate_restock_excel(n_clicks):
    # Create an Excel file in memory
    output = io.BytesIO()

    select_all_query = "SELECT * FROM restock"
    df               = db.query_table_as_pandas(db.database_file, select_all_query)
    df               = utils.remove_df_underscore(df)
    output           = utils.write_df_to_excel(df, output)
    
    # Return the file as a download
    return dcc.send_bytes(output.getvalue(), "restock.xlsx")



# --------------------------------------------------------------------------------------------------
# TRACKER
# --------------------------------------------------------------------------------------------------
@app.callback(
    Output('transport-selector', 'options'),
    Input('submit-update-tracker', 'value'),
    # prevent_initial_call=True,
)
def update_transport_selector(n_clicks):
    print("tracker summoned")
    options = db.get_transports_as_options()
    print(options)
    return options


@app.callback(
    Output('tracker-graph', 'figure'),
    Input('transport-selector', 'value'),
    # prevent_initial_call=True,
)
def update_tracker_graph(transport_plate_number):
    fig = db.generate_tracker_set(transport_plate_number)
    return fig



# Callback for form submission
@app.callback(
    [
        Output('customer-table', 'data'),
        Output('customer-table', 'columns'),
        Output('invoice-message', 'children'),
        Output('pretotal_volume-message', 'children'),
        Output('pretotal_price-message', 'children'),
        Output('volume_balance-message', 'children'),
        Output('price_balance-message', 'children'),
        Output('total_volume-message', 'children'),
        Output('total_price-message', 'children'),
        Output('total_price_wtax-message', 'children'),
    ],
    [
        Input("submit-button", "n_clicks"),
    ],
    [
        State("customer-id-input", "value"),
        State("minvol-balance-input", "value"),
        State("start-date-input", "value"),
        State("end-date-input", "value"),
    ],
    prevent_initial_call=True
)
def update_charge_table(n_clicks, customer_id, vol_balance=0, start_date="2020-01-01", end_date="2030-01-01"):
    if n_clicks is None:
        return html.Div("Enter search criteria and click Submit to view results.")
    
    if not customer_id:
        return html.Div("Please enter a Customer ID.", style=styles['error'])
    
    if not start_date or not end_date:
        return html.Div("Please select both start and end dates.", style=styles['error'])
    

    try:
        end_date_obj = strptime(end_date, "%Y-%m-%d")
    except ValueError:
        return html.Div("Incorrect date format.", style=styles['error'])

    due_date_obj = end_date_obj + timedelta(days=7)
    due_date_str = due_date_obj.strftime("%Y-%m-%d")

    vol_balance = float(vol_balance) if vol_balance else 0
    res         = db.generate_charge_table(db.database_file, customer_id, start_date, end_date, vol_balance)

    df                  = res["dataframe"]
    pretotal_volume     = res["pretotal_volume"]
    pretotal_price      = res["pretotal_price"]
    total_volume        = res["total_volume"]
    total_price         = res["total_price"]
    total_price_wtax    = res["total_price_wtax"]
    volume_balance      = res["volume_balance"]
    price_balance       = res["price_balance"]
    tax_coef            = res["tax_coef"]
    charged_tax         = res["charged_tax"]
    
    # display invoice data
    # print("pretotal_volume: ", pretotal_volume)
    # print("pretotal_price: ", pretotal_price)
    # print("volume_balance: ", volume_balance)
    # print("price_balance: ", price_balance)
    # print("pretotal_volume + volume_balance: ", total_volume)
    # print("pretotal_price + price_balance: ", total_price)
    # print("total_price_wtax: ", total_price_wtax)


    colnames        = list(df.columns)
    spaced_colnames = []
    for col in colnames:
        spaced_col = col.replace("_", " ")
        spaced_colnames.append(spaced_col)
    
    df.columns = spaced_colnames

    # Prepare data for DataTable
    df = df.to_pandas().round(2)
    df['date'] = df['date'].dt.date.astype(str)
    data       = df.to_dict("records")
    columns    = [{"name": i, "id": i} for i in df.columns]     

    m1 = f"{len(data)} records found"
    m2 = f"{round(pretotal_volume, 2)} ---------- Pretotal Volume"
    m3 = f"{round(pretotal_price, 2)} ----------- Pretotal Price"
    m4 = f"{round(volume_balance, 2)} ----------- Volume Balance"
    m5 = f"{round(price_balance, 2)} ------------ Price Balance"
    m6 = f"{round(total_volume, 2)} ------------- Total Volume"
    m7 = f"{round(total_price, 2)} -------------- Total Price"
    m8 = f"{round(total_price_wtax, 2)} --- Total Price (with tax)"
    return data, columns, m1, m2, m3, m4, m5, m6, m7, m8



@app.callback(
    Output("download-invoice-excel", "data"),
    [
        Input("generate-invoice-button", "n_clicks"),
    ],
    [
        State("customer-id-input", "value"),
        State("minvol-balance-input", "value"),
        State("start-date-input", "value"),
        State("end-date-input", "value"),
        State('invoice-number-input', 'value'),
        State('week-period-input', 'value'),
        State('customer-address-input', 'value'),
    ],
    prevent_initial_call=True,
)
def generate_invoice_excel(n_clicks,
                           customer_id,
                           vol_balance,
                           start_date,
                           end_date,
                           invoice_number,
                           week_period,
                           customer_address):
    if n_clicks is None:
        print("No button click detected.")
        return dash.no_update

    if not invoice_number or not week_period or not customer_address:
        print("Please fill in all fields.")
        return dash.no_update

    vol_balance = float(vol_balance) if vol_balance else 0
    res         = db.generate_charge_table(db.database_file, 
                                           customer_id, 
                                           start_date, 
                                           end_date, 
                                           vol_balance)

    df                  = res["dataframe"]
    unit_price          = res["unit_price"]
    charged_tax         = res["charged_tax"]
    pretotal_volume     = res["pretotal_volume"]
    pretotal_price      = res["pretotal_price"]
    total_volume        = res["total_volume"]
    total_price         = res["total_price"]
    total_price_wtax    = res["total_price_wtax"]
    volume_balance      = res["volume_balance"]
    price_balance       = res["price_balance"]

    customer_name       = db.get_customer_name('operation.db', customer_id)
    adrs                = inlay.split_string_by_length(customer_address, 5)

    try:
        end_date_obj = strptime(end_date, "%Y-%m-%d")
    except ValueError:
        return html.Div("Incorrect date format.", style=styles['error'])

    due_date_obj = end_date_obj + timedelta(days=7)
    due_date_str = due_date_obj.strftime("%Y-%m-%d")

    invoice_data        = {
        'invoice_number'    : invoice_number,
        'customer_id'       : customer_id,
        'invoice_period'    : week_period,
        'date'              : end_date,
        'due_date'          : due_date_str,

        'customer_name'     : customer_name,
        'customer_address_1': adrs[0],
        'customer_address_2': adrs[1],
        'customer_address_3': adrs[2],
        'tax_rate' : 0.11,   
        'items'    : [
            {
                'item'          : 'Pemakaian Gas',
                'volume'        : utils.format_float_to_string(total_volume),
                'unit_price'    : utils.format_float_to_string(unit_price),
                'price'         : utils.format_float_to_string(total_price),
                'note'          : ""
            },
            {
                'item'          : 'Penyesuaian Penyerapan Minimum Bulanan',
                'volume'        : volume_balance,
                'unit_price'    : utils.format_float_to_string(unit_price),
                'price'         : price_balance,
                'note'          : ""
            },
        ],
        'inweek'      : "W3",
        'inmonth'     : "Desember",
        'inyear'      : "2024",
        'inprice'     : utils.format_float_to_string(total_price),
        'dpp_price'   : utils.format_float_to_string(total_price),
        'charged_tax' : utils.format_float_to_string(charged_tax),
        'total_taxed' : utils.format_float_to_string(total_price_wtax)
    }

    # Generate the invoice
    output = io.BytesIO()

    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        inlay.generate_invoice(writer, invoice_data)

    return dcc.send_bytes(output.getvalue(), f"invoice_{customer_id}.xlsx")


# Run the app
# if __name__ == '__main__':
#     app.run(debug=True, port=8000)
#     port = int(os.environ.get("PORT", 8050))
#     app.run_server(host="0.0.0.0", port=port)


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'local':
        # Run locally (localhost)
        app.run(debug=True, port=8000)  # Enable debug mode
    else:
        # Run on the server (0.0.0.0)
        port = int(os.environ.get("PORT", 8050))
        app.run(host="0.0.0.0", port=port, debug=True)  # Enable debug mode
