import datetime

import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly
from dash.dependencies import Input, Output
import pymongo
import plotly.graph_objects as go
import time


print("------- BIENVENIDO A iHashTag --------")


external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app.layout = html.Div(
    html.Div([
        html.H4('Hashtag Live Sentiment'),
        html.Div(id='live-update-text'),
        dcc.Graph(id='live-update-graph'),
        dcc.Interval(
            id='interval-component',
            interval=1*1000, # in milliseconds
            n_intervals=0
        )
    ])
)


@app.callback(Output('live-update-text', 'children'),
              [Input('interval-component', 'n_intervals')])
def update_metrics(n):
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["dashdata"]
    mycol = mydb["#Impeachment"]
    x = mycol.find_one()
    style = {'padding': '5px', 'fontSize': '16px'}

    file = open("resultados.txt", "a")
    string = time.strftime("%d/%m/%y %H:%M:%S") + "," + x["muypositivo"] + ","+ x["positivo"] + "," + "," + \
             x["neutro"] + "," + x["negativo"] + "," +x["average"] + "\n"
    file.write(string)
    file.close()
    print("hola")
    return [
        html.Span('Muy Positivos: {0:.2f}'.format(float(x["muypositivo"])), style=style),
        html.Span('Positivos: {0:.2f}'.format(float(x["positivo"])), style=style),
        html.Span('Neutros: {0:.2f}'.format(float(x["neutro"])), style=style),
        html.Span('Negativos: {0:0.2f}'.format(float(x["negativo"])), style=style),
        html.Span('Muy negativos: {0:0.2f}'.format(float(x["muynegativo"])), style=style),
        html.Span('Media Total: {0:0.2f}'.format(float(x["average"])), style=style)
    ]

# Multiple components can update everytime interval gets fired.
@app.callback(Output('live-update-graph', 'figure'),
              [Input('interval-component', 'n_intervals')])
def update_graph_live(n):
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["dashdata"]
    mycol = mydb["#Impeachment"]


    data = {
        'positivo': [],
        'negativo': [],
        'neutro': []
    }

    x = mycol.find_one()
    data['positivo'].append(float(x["positivo"]))
    data['negativo'].append(float(x["negativo"]))
    data['neutro'].append(float(x["neutro"]))


    # Create the graph with subplots
    fig = go.Figure(data=go.Bar(name = 'Tweet Sentiment',x=["Muy Positivo", "Positivo", "Neutral", "Negativo", "Muy Negativo"],
                                y=[float(x['muypositivo']),float(x["positivo"]), float(x["neutro"]), float(x["negativo"]), float(x["muynegativo"])],
                                marker_color=['cyan','green','gray','red','pink'], marker_line_color='rgb(8,48,107)',
                                marker_line_width=1.5, opacity=0.6, text=[float(x['muypositivo']),float(x["positivo"]), float(x["neutro"]), float(x["negativo"]), float(x["muynegativo"])],
                                textposition='auto'
                                ))

    fig.add_trace(
        go.Scatter(name="Average",
            x=["Muy Positivo", "Positivo", "Neutral", "Negativo", "Muy Negativo"],
            y=[float(x['average']), float(x['average']), float(x['average']), float(x['average']), float(x['average'])]
        ))

    return fig


if __name__ == '__main__':
    app.run_server(debug=True)
