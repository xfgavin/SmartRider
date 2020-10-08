#!/usr/bin/env python3
import dash
import dash_html_components as html
import dash_core_components as dcc
import dash_leaflet as dl
from dash.dependencies import Input, Output, State
#For calendar
from datetime import datetime as dt
import re
import networkx as nx
import osmnx as ox
from io import BytesIO
import base64
import matplotlib.pyplot as plt
#import numpy as np




# The external stylesheet holds the location button icon.
app = dash.Dash(external_stylesheets=['https://maxcdn.bootstrapcdn.com/font-awesome/4.7.0/css/font-awesome.min.css'],
                prevent_initial_callbacks=True)
app.layout = html.Div([
    html.H1('Smart Rider', style={'width': '100%', 'text-align': 'center'}),
    dl.Map([dl.TileLayer(), dl.LayerGroup(id="layer"), dl.LocateControl(options={'locateOptions': {'enableHighAccuracy': True}})],
           id="map", style={'width': '100%', 'height': '50vh', 'margin': "auto"}, center=[40.707,-74.010], zoom=12),
    html.Div(id="text"),
    html.Div(children='''
        Please input your origin, destination, date, time range, and adjust traffic situation.
    '''),
    html.Div([
        dcc.Input(id='address_start', value='origin', type='text'),
        dcc.Input(id='address_end', value='destination', type='text'),
        html.Br(),
        '   Leave on:',
        dcc.DatePickerSingle(
            id='tripdate',
            min_date_allowed=dt.today(),
            max_date_allowed=dt(2099, 12, 30),
            initial_visible_month=dt(dt.today().year, dt.today().month, dt.today().day),
            date=str(dt.today()),style={'display':'block'}
        ),
        'Between:',
        dcc.Dropdown(
            id='time_range',
            options=[
                {'label': '12am-1am', 'value': '0'},
                {'label': '1am-2am', 'value': '1'},
                {'label': '2am-3am', 'value': '2'},
                {'label': '3am-4am', 'value': '3'},
                {'label': '4am-5am', 'value': '4'},
                {'label': '5am-6am', 'value': '5'},
                {'label': '6am-7am', 'value': '6'},
                {'label': '7am-8am', 'value': '7'},
                {'label': '8am-9am', 'value': '8'},
                {'label': '9am-10am', 'value': '9'},
                {'label': '10am-11am', 'value': '10'},
                {'label': '11am-12pm', 'value': '11'},
                {'label': '12pm-1pm', 'value': '12'},
                {'label': '1pm-2pm', 'value': '13'},
                {'label': '2pm-3pm', 'value': '14'},
                {'label': '3pm-4pm', 'value': '15'},
                {'label': '4pm-5pm', 'value': '16'},
                {'label': '5pm-6pm', 'value': '17'},
                {'label': '6pm-7pm', 'value': '18'},
                {'label': '7pm-8pm', 'value': '19'},
                {'label': '8pm-9pm', 'value': '20'},
                {'label': '9pm-10pm', 'value': '21'},
                {'label': '10pm-11pm', 'value': '22'},
                {'label': '11pm-12am', 'value': '23'},
            ],
            value=dt.today().hour, style={'width': 150}
        ),
        'Traffic: ',
        dcc.Slider(
            id='traffic',
            min=1,
            max=3,
            value=2,
            step=1,
            marks={1:'Light',2:'Moderate',3:'Heavy'},
        ),
        'COVID or similar: ',
        dcc.Slider(
            id='covid',
            min=1,
            max=2,
            value=2,
            step=1,
            marks={1:'Yes',2:'No'},
        ),
    ]),
    html.Div(id='output_div'),
    html.Div([html.Img(id = 'cur_plot', src = '')],
             id='plot_div'),
])

G = ox.graph_from_place('New York, New York, USA', network_type='drive')
# impute missing edge speeds then calculate edge travel times
G = ox.add_edge_speeds(G)
G = ox.add_edge_travel_times(G)

## get the nearest network nodes to two lat/lng points
#orig = ox.get_nearest_node(G, (40.640248,-74.0139777))
#dest = ox.get_nearest_node(G, (40.6430648,-73.989591))
#route = ox.shortest_path(G, orig, dest, weight='travel_time')
#fig, ax = ox.plot_graph_route(G, route, node_size=0)


@app.callback(Output("layer", "children"), [Input("map", "click_lat_lng")])
def map_click(click_lat_lng):
    return [dl.Marker(position=click_lat_lng, children=dl.Tooltip("({:.3f}, {:.3f})".format(*click_lat_lng)))]

@app.callback(Output("text", "children"), [Input("map", "location_lat_lon_acc")])
def update_location(location):
    return "You are within {} meters of (lat,lon) = ({},{})".format(location[2], location[0], location[1])

@app.callback(Output('output_div', 'children'),
              [Input('submit-button', 'n_clicks')],
              [State('username', 'value')],
              )
def update_output(clicks, input_value):
    if clicks is not None:
        print(clicks, input_value)
# Draw the map if 1) refresh signal received 2) New user position is entered
#@app.callback(Output('graph', 'figure'),
#              [Input('submit-button', 'n_clicks')],state=[State(component_id='my-id', component_property='value')])
              #[Input('interval-component', 'n_intervals'),Input('submit-button', 'n_clicks')],state=[State(component_id='my-id', component_property='value')])
#def make_figure(n_interval, n_clicks, input_value):
#def make_figure(n_clicks, input_value):
#  # get the nearest network nodes to two lat/lng points
#  orig = ox.get_nearest_node(G, (40.640248,-74.0139777))
#  dest = ox.get_nearest_node(G, (40.6430648,-73.989591))
#  route = ox.shortest_path(G, orig, dest, weight='travel_time')
#  fig, ax = ox.plot_graph_route(G, route, node_size=0)
#  return fig

def fig_to_uri(in_fig, close_all=True, **save_args):
    # type: (plt.Figure) -> str
    """
    Save a figure as a URI
    :param in_fig:
    :return:
    """
    out_img = BytesIO()
    in_fig.savefig(out_img, format='png', **save_args)
    if close_all:
        in_fig.clf()
        plt.close('all')
    out_img.seek(0)  # rewind file
    encoded = base64.b64encode(out_img.read()).decode("ascii").replace("\n", "")
    return "data:image/png;base64,{}".format(encoded)
@app.callback(
    Output(component_id='cur_plot', component_property='src'),
    [Input(component_id='time_range', component_property='value'), Input(component_id = 'box_size', component_property='value')]
)
def update_graph(input_value, n_val):
    #fig, ax1 = plt.subplots(1,1)
    #np.random.seed(len(input_value))
    #ax1.matshow(np.random.uniform(-1,1, size = (n_val,n_val)))
    #ax1.set_title(input_value)
    orig = ox.get_nearest_node(G, (40.640248,-74.0139777))
    dest = ox.get_nearest_node(G, (40.6430648,-73.989591))
    route = ox.shortest_path(G, orig, dest, weight='travel_time')
    fig, ax = ox.plot_graph_route(G, route, node_size=0)
    out_url = fig_to_uri(fig)
    return out_url

if __name__ == '__main__':
    app.run_server(port=8080,host='0.0.0.0')

##############Multiple markers
#import dash
#import dash_html_components as html
#import dash_leaflet as dl
#import pandas as pd
#import numpy as np
#
## Create example data frame.
#lats = [56, 56, 56]
#lons = [10, 11, 12]
#df = pd.DataFrame(columns=["lat", "lon"], data=np.column_stack((lats, lons)))
#
## Create markers from data frame.
#markers = [dl.Marker(position=[row["lat"], row["lon"]]) for i, row in df.iterrows()]
#
## Create example app.
#app = dash.Dash(external_stylesheets=['https://codepen.io/chriddyp/pen/bWLwgP.css'])
#app.layout = html.Div([
#    dl.Map(children=[dl.TileLayer(url="https://a.tile.openstreetmap.org/{z}/{x}/{y}.png"), dl.LayerGroup(markers)],
#           style={'width': "100%", 'height': "100%"}, center=[56, 11], zoom=9, id="map"),
#], style={'width': '1000px', 'height': '500px'})
#
#
#
#if __name__ == '__main__':
#    app.run_server(port=8080,host='0.0.0.0')
