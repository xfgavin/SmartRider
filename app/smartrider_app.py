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
                disabled=True
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
                value=[1]
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
                        }
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
    ),
    ##########################
    #Reference NYC TLC and project's github
    html.Div([
        html.Br(),
        html.Br(),
        html.Div([
            html.A(
                html.Img(
                    src=app.get_asset_url('nyc-tlc-logo.png'),
                    style={
                        'height':'20%',
                        'width':'20%'
                    },
                    alt='Based on NYC TLC trip data 2009-present'
                ),
                href='https://registry.opendata.aws/nyc-tlc-trip-records-pds/',
                target='_blank',
                title='Based on NYC TLC trip data 2009-present'
            ),
            html.A(
                html.Img(
                    src=app.get_asset_url('github.png'),
                    style={
                        'height':'5%',
                        'width':'5%'
                    },
                    alt='Check out source code on github'
                ),
                href='https://github.com/xfgavin/smartrider',
                target='_blank',
                title='Check out source code on github'
            )],
            style={
                'float': 'right'
            }
        )
    ])
])

######################
#Handle HTML element status change events

@app.callback([Output('departure_month','disabled'),Output('ratesgraph','style'),Output('ratesgraph','figure'),Output('layer', 'children')],
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
            month_slider_disabled = True
        else:
            mysql_weekday = 'SELECT avg(rate) as rate from rates where target_month = ' + str(departure_month) + ' AND traffic=' + str(traffic) + ' AND iscovid=0 AND isweekend=0 AND zoneid in (SELECT locationid from taxi_zones where ST_intersects(ST_SetSRID( ST_Point(' + str(click_lat_lng[1]) + ', ' + str(click_lat_lng[0]) + '),4326),geom))  group by target_hour order by target_hour;'
            mysql_weekend = 'SELECT avg(rate) as rate from rates where target_month = ' + str(departure_month) + ' AND traffic=' + str(traffic) + ' AND iscovid=0 AND isweekend=1 AND zoneid in (SELECT locationid from taxi_zones where ST_intersects(ST_SetSRID( ST_Point(' + str(click_lat_lng[1]) + ', ' + str(click_lat_lng[0]) + '),4326),geom))  group by target_hour order by target_hour;'
            mysql_holiday = 'SELECT avg(rate) as rate from rates where target_month = ' + str(departure_month) + ' AND traffic=' + str(traffic) + ' AND iscovid=0 AND isholiday=1 AND zoneid in (SELECT locationid from taxi_zones where ST_intersects(ST_SetSRID( ST_Point(' + str(click_lat_lng[1]) + ', ' + str(click_lat_lng[0]) + '),4326),geom))  group by target_hour order by target_hour;'
            month_slider_disabled = False
        #####################
        #Grab data
        result_weekday = getdata(mysql_weekday)
        result_weekend = getdata(mysql_weekend)
        result_holiday = getdata(mysql_holiday)
        #####################
        #Show the graph, plot the graph, place marker on map.
        return [month_slider_disabled,{'display': 'block'}, plotresult(result_weekday,result_weekend,result_holiday),
            dl.Marker(position=click_lat_lng, children=dl.Tooltip("({:.3f}, {:.3f})".format(*click_lat_lng)))
           ]

#####################
#Draw graph and return graph object
def plotresult(result_weekday,result_weekend,result_holiday):
    # Create traces
    bgcolor = '#e5ecf6'
    
    result_weekday = [round(x,2) for x in result_weekday]
    result_weekend = [round(x,2) for x in result_weekend]
    result_holiday = [round(x,2) for x in result_holiday]
    fig = go.Figure(layout=go.Layout(
        title=go.layout.Title(text="Taxi rate by hour"),
        title_x=0.5,  # Sets background color to white
        xaxis=dict(
            title="Hour",
            linecolor="#BCCCDC",  # Sets color of X-axis line
            showgrid=False  # Removes X-axis grid lines
        ),
        yaxis=dict(
            title="Rate ($/Mi)",  
            linecolor="#BCCCDC",  # Sets color of Y-axis line
            showgrid=False,  # Removes Y-axis grid lines    
        )
    ))
    hour_list = ['12AM','1AM','2AM','3AM','4AM','5AM','6AM','7AM','8AM','9AM','10AM','11AM','12PM','1PM','2PM','3PM','4PM','5PM','6PM','7PM','8PM','9PM','10PM','11PM']
    fig.add_trace(go.Scatter(x=hour_list, y=result_weekday,
                        mode='lines+markers',
                        name='Weekdays'))
    fig.add_trace(go.Scatter(x=hour_list, y=result_weekend,
                        mode='lines+markers',
                        name='Weekends'))
    fig.add_trace(go.Scatter(x=hour_list, y=result_holiday,
                        mode='lines+markers',
                        name='Holidays'))
    fig.layout.plot_bgcolor = 'rgba(0,0,0,0)'
    fig.layout.paper_bgcolor = 'rgba(0,0,0,0)'
    fig.update_xaxes(type='category', showgrid=False, zeroline=False,tickangle = 90)
    fig.update_yaxes(showgrid=False, zeroline=False)
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
