#!/usr/bin/env python3
import dash
import dash_html_components as html
import dash_core_components as dcc
import dash_leaflet as dl
from dash.dependencies import Input, Output, State
from datetime import datetime as dt
import plotly.graph_objects as go
import psycopg2
import sys
import os

#################################
#Draw HTML elements
# The external stylesheet holds the location button icon.
app = dash.Dash(external_stylesheets=['https://maxcdn.bootstrapcdn.com/font-awesome/4.7.0/css/font-awesome.min.css'],
                prevent_initial_callbacks=True)
app.title = 'Smart Rider: Ride smarter, for less!'
app.layout = html.Div([
    html.H1('Smart Rider', style={'width': '100%', 'text-align': 'center'}),
    dl.Map([dl.TileLayer(), dl.LayerGroup(id="layer"), dl.LocateControl(options={'locateOptions': {'enableHighAccuracy': True}})],
           id="map", style={'width': '100%', 'height': '50vh', 'margin': "auto"}, center=[40.707,-74.010], zoom=12),
    html.Div(children='''
        Please first choose your origin on the map, then adjust parameters below.
    ''', style={'color':'black'}),
    html.Div([
        html.Div([
            html.Div(
                '''Departure Month:''',
                style={
                    'paddingTop' : 20,
                    'paddingBottom' : 10
                }
            ),
            dcc.Slider(
                id='departure_month',
                min=1,
                max=12,
                value=dt.now().month,
                step=1,
                marks={1:'Jan',2:'Feb',3:'Mar',4:'Apr',5:'May',6:'Jun',7:'Jul',8:'Aug',9:'Sep',10:'Oct',11:'Nov',12:'Dec'},
                updatemode='drag'
            ),
        ],
        style={
            "width" : '50%',
            'float' : 'left',
            'paddingRight' : 30,
            'paddingLeft' : 10,
            'boxSizing' : 'border-box'
            }
        ),
        html.Div([
            html.Div(
                '''Traffic:''',
                style={
                    'paddingTop' : 20,
                    'paddingBottom' : 10
                }
            ),
            dcc.RadioItems(
                id="traffic",
                options=[
                    {'label': 'Light', 'value': 1},
                    {'label': 'Moderate', 'value': 2},
                    {'label': 'Heavy', 'value': 3}
                ],
                value=2,
                labelStyle={
                    'display': 'inline-block',
                    'paddingRight' : 10,
                    'paddingLeft' : 10,
                    'paddingBottom' : 5,
                    },
            ),
        ],
        style={
            "width" : '25%',
            'float' : 'left',
            'paddingRight' : 30,
            'paddingLeft' : 10,
            'boxSizing' : 'border-box'
            }
        ),
        html.Div([
            html.Div(
                '''Social restriction, e.g. COVID?:''',
                style={
                    'paddingTop' : 20,
                    'paddingBottom' : 10
                }
            ),
            dcc.Checklist(
                id='covid',
                options=[
                {'label': 'Yes', 'value': 1}
                ],
                value=1
            ),
        ],
        style={
            "width" : '25%',
            'float' : 'left',
            'paddingRight' : 10,
            'paddingLeft' : 10,
            'boxSizing' : 'border-box'
            }
        ),
    ]),
    html.Div([
        html.Br(),
        html.Br(),
        html.Br(),
        html.Br(),
        dcc.Loading(
                id="graphloader",
                type="dot",
                children=html.Div([
                    dcc.Graph(
                        id='ratesgraph',
                        style={
                            'display': 'none'
                        },
                        figure={
                        'layout': go.Layout(
                            paper_bgcolor='rgba(0,0,0,0)',
                            plot_bgcolor='rgba(0,0,0,0)'
                        )}
                    )],
                    style={
                        'width': '100%',
                        'paddingTop' : 0,
                        'paddingBottom' : 0
                    }
                ),
        ),
    ], style={
                'width': '100%',
                'paddingTop' : 0,
                'paddingBottom' : 0
            }
    )
])

######################
#Handle HTML element status change events

@app.callback([Output('ratesgraph','style'),Output('ratesgraph','figure'),Output('layer', 'children')],
              [Input("map", "click_lat_lng"),
               Input('departure_month', 'value'),
               Input('traffic', 'value'),
               Input('covid', 'value')
              ])
######################
#Retrieve data based on input and plot graph
def update_graph(click_lat_lng,departure_month,traffic,covid):
    global myloc
    if click_lat_lng is not None:
        myloc = click_lat_lng
    elif myloc is not None:
        click_lat_lng = myloc
    if click_lat_lng is not None:
        ##################
        #Construct query string based on facts of whether COVID and other inputs
        if covid:
            mysql_weekday = 'SELECT avg(rate) as rate from rates where traffic=' + str(traffic) + ' AND iscovid=1 AND isweekend=0 AND zoneid in (SELECT locationid from taxi_zones where ST_intersects(ST_SetSRID( ST_Point(' + str(click_lat_lng[1]) + ', ' + str(click_lat_lng[0]) + '),4326),geom))  group by target_hour order by target_hour;'
            mysql_weekend = 'SELECT avg(rate) as rate from rates where traffic=' + str(traffic) + ' AND iscovid=1 AND isweekend=1 AND zoneid in (SELECT locationid from taxi_zones where ST_intersects(ST_SetSRID( ST_Point(' + str(click_lat_lng[1]) + ', ' + str(click_lat_lng[0]) + '),4326),geom))  group by target_hour order by target_hour;'
            mysql_holiday = 'SELECT avg(rate) as rate from rates where traffic=' + str(traffic) + ' AND iscovid=1 AND isholiday=1 AND zoneid in (SELECT locationid from taxi_zones where ST_intersects(ST_SetSRID( ST_Point(' + str(click_lat_lng[1]) + ', ' + str(click_lat_lng[0]) + '),4326),geom))  group by target_hour order by target_hour;'
        else:
            mysql_weekday = 'SELECT avg(rate) as rate from rates where target_month = ' + str(departure_month) + ' AND traffic=' + str(traffic) + ' AND iscovid=0 AND isweekend=0 AND zoneid in (SELECT locationid from taxi_zones where ST_intersects(ST_SetSRID( ST_Point(' + str(click_lat_lng[1]) + ', ' + str(click_lat_lng[0]) + '),4326),geom))  group by target_hour order by target_hour;'
            mysql_weekend = 'SELECT avg(rate) as rate from rates where target_month = ' + str(departure_month) + ' AND traffic=' + str(traffic) + ' AND iscovid=0 AND isweekend=1 AND zoneid in (SELECT locationid from taxi_zones where ST_intersects(ST_SetSRID( ST_Point(' + str(click_lat_lng[1]) + ', ' + str(click_lat_lng[0]) + '),4326),geom))  group by target_hour order by target_hour;'
            mysql_holiday = 'SELECT avg(rate) as rate from rates where target_month = ' + str(departure_month) + ' AND traffic=' + str(traffic) + ' AND iscovid=0 AND isholiday=1 AND zoneid in (SELECT locationid from taxi_zones where ST_intersects(ST_SetSRID( ST_Point(' + str(click_lat_lng[1]) + ', ' + str(click_lat_lng[0]) + '),4326),geom))  group by target_hour order by target_hour;'
        #####################
        #Grab data
        result_weekday = getdata(mysql_weekday)
        result_weekend = getdata(mysql_weekend)
        result_holiday = getdata(mysql_holiday)
        #####################
        #Show the graph, plot the graph, place marker on map.
        return [{'display': 'block'}, plotresult(result_weekday,result_weekend,result_holiday),
            dl.Marker(position=click_lat_lng, children=dl.Tooltip("({:.3f}, {:.3f})".format(*click_lat_lng)))
           ]

#####################
#Draw graph and return graph object
def plotresult(result_weekday,result_weekend,result_holiday):
    # Create traces
    bgcolor = '#e5ecf6'
    fig = go.Figure(layout=go.Layout(
        title=go.layout.Title(text="Average taxi rate ($/Mile)"),
        title_x=0.5
    ))
    fig.add_trace(go.Scatter(x=list(range(24)), y=result_weekday,
                        mode='lines+markers',
                        name='Weekdays'))
    fig.add_trace(go.Scatter(x=list(range(24)), y=result_weekend,
                        mode='lines+markers',
                        name='Weekends'))
    fig.add_trace(go.Scatter(x=list(range(24)), y=result_holiday,
                        mode='lines+markers',
                        name='Holidays'))
    fig.layout.plot_bgcolor = bgcolor
    fig.layout.paper_bgcolor = bgcolor
    return fig

########################
#Query database and save it to python list object
def getdata(sql):
    cur = conn.cursor()
    try:
        cur.execute(sql)
        result = list(sum(cur.fetchall(), ()))
        cur.close()
        return result
    except:
        conn.rollback()
        cur.close()
        return []

if __name__ == '__main__':
    myloc = None

    try:
        conn = psycopg2.connect(
               host=os.environ['PGHOST'],
               database=os.environ['PGDB'],
               user=os.environ['PGUSER'],
               password=os.environ['PGPWD'])
    except:
        print('Can not connect to the database!')
        sys.exit(-1)

    app.run_server(port=8080,host='::')
