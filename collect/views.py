from django.shortcuts import render
from django.shortcuts import render, HttpResponse
from django.views.decorators.csrf import csrf_exempt
from rest_framework import viewsets
from time import sleep
from .serializers import PersonSerializer

from .models import Person
# Create your views here.
from time import sleep
from json import dumps
from kafka import KafkaProducer
import json
from json import JSONEncoder
from sklearn.cluster import KMeans

import seaborn as sns
import matplotlib.pyplot as plt
from plotly.subplots import make_subplots
import time
from datetime import datetime

from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement, BatchStatement

import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px

import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
from django_plotly_dash import DjangoDash

geo_df = json.load(open("nepal.geojson",'r'))


producer = KafkaProducer(bootstrap_servers=['localhost:9092','localhost:9093','localhost:9094'], value_serializer=lambda x: dumps(x).encode('utf-8'))
cluster = Cluster(contact_points=['172.31.64.191','172.31.68.237','172.31.64.174'])
session = cluster.connect('diabetesdb')

def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)

session.set_keyspace('diabetesdb')
#session.set_keyspace('diabetesdb')
session.row_factory = pandas_factory
#rows = session.execute("""select * from diabetesb """)

district_id_df = pd.read_csv("district_withId.csv")

@csrf_exempt
def app1(request):
    if request.method == "POST":
        print(request.POST)
        name = request.POST.get('name')
        age = int(request.POST.get('age'))
        glucose = int(request.POST.get('glucose'))
        bloodpressure = int(request.POST.get('bloodpressure'))
        skinthickness = int(request.POST.get('skinthickness'))
        insulin = int(request.POST.get('insulin'))
        bmi = int(request.POST.get('bmi'))
        pregnancy = int(request.POST.get('pregnancy'))
        diabetes_pdf = int(request.POST.get('diabetes_pdf'))
        created_at = datetime.now().strftime("%Y-%m-%d, %H:%M:%S")
        location = request.POST.get('location')

        if age < 21 or age > 100 :
            return HttpResponse('<h1>Age should be between 21 and 100</h1>')

        elif glucose < 44 or glucose > 200:
            return HttpResponse('<h1>Glucose level should be between 44 and 200</h1>')

        elif bloodpressure < 24 or bloodpressure > 125:
            return HttpResponse('<h1>Blood Pressure should be within range 24 and 125</h1>')

        elif skinthickness < 7 or skinthickness > 111:
            return HttpResponse('<h1>Skin Thickness should be within range 7 and 111</h1>')

        elif insulin < 14 or insulin > 750:
            return HttpResponse('<h1>Insulin should be in range 14 and 750</h1>')

        elif bmi < 18  or bmi > 85:
            return HttpResponse('<h1>BMI should be in range 18 and 85</h1>')

        elif pregnancy < 0 or pregnancy > 20:
            return HttpResponse('<h1>Pregnancy should be in range 0 and 20</h1>')

        elif diabetes_pdf < 0 or diabetes_pdf > 3:
            return HttpResponse('<h1>Diabetes Pedigree Function(PDF) should be in range 0 and 3</h1>')
        else:
            pass

        if name and age:
            #obj = Person.objects.create(name=name, age=age, glucose=glucose, bloodpressure=bloodpressure, skinthickness=skinthickness, insulin=insulin, bmi=bmi,pregnancy= pregnancy, diabetes_pdf= diabetes_pdf)
            #obj.save()

            #for kafka 
            data = {'anonymous_name': name,'Age':age,'Glucose':glucose,'BloodPressure':bloodpressure,'SkinThickness':skinthickness,'Insulin':insulin,'BMI':bmi,'DiabetesPedigreeFunction':diabetes_pdf,'Pregnancies':pregnancy, 'created_at':created_at,'location':location}
            producer.send('diabetes', value=data)
            print(data)

    return render(request, 'collect/home.html')


@csrf_exempt
def plot1(request):
    rows = session.execute("""select * from diabetesb""")
    df = rows._current_rows
    df_sort = df.sort_values(by=['age'])
    app = DjangoDash('SimpleExample') 
    app.layout = html.Div([
        html.Div([
            dcc.Graph(id='scatter-plot')
            ]),
        dcc.RangeSlider(
            id='range-slider',
            min=0, max=2.5, step=0.1,
            marks={0: '0', 2.5: '2.5'},
            value=[0.5, 2]
            ),
        ])
    @app.callback(
        Output("scatter-plot", "figure"), 
        [Input("range-slider", "value")])
    def update_bar_chart(slider_range):
        low, high = slider_range
        fig= px.scatter_3d(df_sort, x='glucose', y='bloodpressure', z='bmi', color='prediction', size="pregnancies")
        return fig

    
    return render(request, 'collect/plot1.html')

@csrf_exempt
def plot2(request):
    rows = session.execute("""select * from diabetesb""")
    df = rows._current_rows
    df_sort = df.sort_values(by=['age'])
    app = DjangoDash('SimpleExample2') 
    app.layout = html.Div([
        html.Div([
            dcc.Graph(id='scatter-plot')
            ]),
        dcc.RangeSlider(
            id='range-slider',
            min=0, max=2.5, step=0.1,
            marks={0: '0', 2.5: '2.5'},
            value=[0.5, 2]
            ),
        ])
    @app.callback(
        Output("scatter-plot", "figure"), 
        [Input("range-slider", "value")])
    def update_bar_chart(slider_range):
        low, high = slider_range
        fig= px.scatter(df_sort, x='insulin', y='skinthickness', color='prediction', size="age")
        return fig

    return render(request, 'collect/plot2.html')


@csrf_exempt
def plot3(request):
    rows = session.execute("""select * from diabetesb""")
    df = rows._current_rows
    df_sort = df.sort_values(by=['age'])
    app = DjangoDash('SimpleExample3') 
    app.layout = html.Div([
        html.Div([
            dcc.Graph(id='scatter-plot')
            ]),
        dcc.RangeSlider(
            id='range-slider',
            min=0, max=2.5, step=0.1,
            marks={0: '0', 2.5: '2.5'},
            value=[0.5, 2]
            ),
        ])
    @app.callback(
        Output("scatter-plot", "figure"), 
        [Input("range-slider", "value")])
    def update_bar_chart(slider_range):
        low, high = slider_range
        fig= px.scatter(df_sort, x='age', y='pregnancies', color='prediction', size="diabetespedigreefunction")
        return fig
    
    return render(request, 'collect/plot3.html')

@csrf_exempt
def plot4(request):
    rows = session.execute("""select * from diabetesb""")
    df = rows._current_rows
    df_sort = df.sort_values(by=['age'])
    dfCounts = pd.DataFrame(df["location"].value_counts()).reset_index()
    dfCounts = dfCounts.rename(columns = {'index':'location', 'location':'counts'})
    final_df = pd.merge(dfCounts,district_id_df,on='location')

    app = DjangoDash('SimpleExample4') 
    print(final_df)
    fig = px.choropleth_mapbox(final_df, geojson=geo_df,locations='id',color='counts',
                           color_continuous_scale="Viridis",
                           mapbox_style="carto-positron",
                           zoom=6.17,hover_name = 'location',hover_data = {'location':False, 'counts':True},
                           opacity=0.7,center = {"lat": 28.4390, "lon": 84.1240},
                          )
    fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
    fig.update_layout(height=620, width=1300)
    app.layout = html.Div([
        html.Div([
            dcc.Graph(id="choropleth",figure=fig)
            ]),
        ])
    @app.callback(
        Output("choropleth", "figure"),) 
    def update_bar_chart():
        rows = session.execute("""select * from diabetesb""")
        df = rows._current_rows
        df_sort = df.sort_values(by=['age'])
        dfCounts = pd.DataFrame(df["location"].value_counts()).reset_index()
        dfCounts = dfCounts.rename(columns = {'index':'location', 'location':'counts'})
        final_df = pd.merge(dfCounts,district_id_df,on='location')
        fig = px.choropleth_mapbox(final_df, geojson=geo_df,locations='id',color='counts',
                           color_continuous_scale="Viridis",
                           mapbox_style="carto-positron",
                           zoom=6.17,hover_name = 'location',
                           opacity=0.7,center =  {"lat": 28.4390, "lon": 84.1240},
                          )
        fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        fig.update_layout(height=620, width=1300)
        
        return fig

    return render(request, 'collect/plot4.html')

def plot5(request):
    rows = session.execute("""select * from diabetesb""")
    df = rows._current_rows
    df1 = df.loc[:, ['glucose', 'insulin']]
    cluster1 = KMeans(n_clusters=2)
    cluster1.fit(df1)
    print(df1)
    labels1 = cluster1.predict(df1)
    fig = px.scatter(df1,x='glucose',y='insulin' ,color=labels1 ,template="ggplot2")
    app = DjangoDash('SimpleExample5') 
    app.layout = html.Div([
        html.Div([
            dcc.Graph(id='clustergram-input',figure=fig)
            ]),
        ])

    return render(request, 'collect/plot5.html')

    #fig.show(layout=layout)


def plotEDA(request):
    diab=pd.read_csv('diabetic.csv')
    #fig = sns.countplot(x='Outcome',data=diab)
    #plt.show()
    df_count = diab.value_counts("Outcome").reset_index(name="counts")
    fig = px.bar(df_count, x="Outcome",y="counts", color="Outcome")
    textd = ['non-diabetic' if cl==0 else 'diabetic' for cl in diab['Outcome']]
    pair_plot_fig = go.Figure(data=go.Splom(
                  dimensions=[dict(label='Pregnancies', values=diab['Pregnancies']),
                              dict(label='Glucose', values=diab['Glucose']),
                              dict(label='BloodPressure', values=diab['BloodPressure']),
                              dict(label='SkinThickness', values=diab['SkinThickness']),
                              dict(label='Insulin', values=diab['Insulin']),
                              dict(label='BMI', values=diab['BMI']),
                              dict(label='DiabPedigreeFun', values=diab['DiabetesPedigreeFunction']),
                              dict(label='Age', values=diab['Age'])],
                  marker=dict(color=diab['Outcome'],
                              size=5,
                              colorscale='Bluered',
                              line=dict(width=0.5,
                                        color='rgb(230,230,230)')),
                  text=textd,
                  diagonal=dict(visible=False)))

    pair_plot_fig.update_layout( dragmode='select', width=1300, height=1000, hovermode='closest')
    histogram_fig = make_subplots(rows=3, cols=3,  subplot_titles=("Age","Pregnancies","BMI","Insulin","Glucose","BloodPressure","SkinThickness","DiabetesPedigreeFunction"))
    histogram_fig.add_trace( go.Histogram(x=diab['Age']), row=1, col=1)
    histogram_fig.add_trace( go.Histogram(x=diab['Pregnancies']), row=1, col=2)
    histogram_fig.add_trace( go.Histogram(x=diab['BMI']), row=1, col=3)
    histogram_fig.add_trace( go.Histogram(x=diab['Insulin']), row=2, col=1)
    histogram_fig.add_trace( go.Histogram(x=diab['Glucose']), row=2, col=2)
    histogram_fig.add_trace( go.Histogram(x=diab['BloodPressure']), row=2, col=3)
    histogram_fig.add_trace( go.Histogram(x=diab['SkinThickness']), row=3, col=1)
    histogram_fig.add_trace( go.Histogram(x=diab['DiabetesPedigreeFunction']), row=3, col=2)

    heatmap_fig = px.imshow(diab[diab.columns[:8]].corr())
    fig.update_layout( title={'text': "Count Plot",'y':0.9, 'x':0.5, 'xanchor': 'center', 'yanchor': 'top'})
    histogram_fig.update_layout( title={'text': "Histogram Plot",'y':0.9, 'x':0.5, 'xanchor': 'center', 'yanchor': 'top'})
    pair_plot_fig.update_layout( title={'text': "Pair Plot",'y':0.9, 'x':0.5, 'xanchor': 'center', 'yanchor': 'top'})
    heatmap_fig.update_layout( title={'text': "Heatmap Plot",'y':0.9, 'x':0.5, 'xanchor': 'center', 'yanchor': 'top'})

    app = DjangoDash('SimpleExample6') 
    app.layout = html.Div([
        html.Div([
            dcc.Graph(figure=pair_plot_fig)
            ]),
        html.Div([
            html.Div([
                dcc.Graph(id='bar-chart',figure=fig),
                ], className="six columns"),
            html.Div([
                dcc.Graph(figure=heatmap_fig)
                ], className="six columns"),
        ], className="row"),
        html.Div([
            dcc.Graph(figure=histogram_fig)
            ]),
        ])
    app.css.append_css({
            'external_url': 'https://codepen.io/chriddyp/pen/bWLwgP.css'
            })

    @app.callback(Output("bar-chart", "figure"))
    def update_bar_chart():
        fig = px.bar(df_count, x="Outcome",y="counts")
        return fig
    return render(request, 'collect/plotEDA.html')



class PersonViewSet(viewsets.ModelViewSet):
    queryset = Person.objects.all().order_by('created_at').reverse()
    serializer_class = PersonSerializer



