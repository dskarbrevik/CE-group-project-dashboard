import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd
import boto3
import random
from datetime import datetime, timedelta, timezone
import math
import time

dynamo = boto3.resource('dynamodb',
                        region_name='us-east-1')

table = dynamo.Table("tweet_metrics")


external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

def get_data(table,time_points=150, delay_steps=3):
    start = time.time()
    response = table.scan()
    data = response['Items']

    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])
    end_read_db = time.time()
    df = pd.DataFrame(data)

    timestamp = datetime.now(tz=timezone.utc)-timedelta(seconds=delay_steps*10)
    seconds = int(math.floor(timestamp.second/10)*10)
    if seconds<20:
        seconds=0
    elif seconds<40:
        seconds=20
    else:
        seconds=40
    timestamp = timestamp.replace(second=seconds, microsecond=0)

    data = []
    tmp_df = pd.DataFrame()
    for i in range(time_points):
        tmp_time = str(timestamp-timedelta(seconds=20*i))
        tmp_data = df[df['id']==tmp_time]
        if tmp_data.shape[0]==1:
            tmp_df = tmp_df.append(tmp_data)
    tmp_df = tmp_df.fillna(0)
    tmp_df = tmp_df.drop("time_tl",axis=1)
    cols = list(tmp_df.columns)
    cols.remove('id')
    for col in cols:
        splits = col.split("_")
        if "_".join(splits[-2:])=='sentiment_score':
            tmp_df[col] = tmp_df[col].astype(float)
            tmp_df = tmp_df.rename(columns={col:splits[0].capitalize()})
        elif "_".join(splits[-2:])=='tweet_count':
            tmp_df[col] = tmp_df[col].astype(int)
            tmp_df = tmp_df.rename(columns={col:col.replace("_"," ").capitalize()})
    end = time.time()
    print(f"get_data db pull took {round(end_read_db-start,2)} seconds.")
    print(f"get_data full update took {round(end-start,2)} seconds.")
    return(tmp_df)


app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
application = app.server


df1 = get_data(table)
df2 = df1[['id','Biden tweet count', 'Trump tweet count']]
df2 = df2.rename(columns={'Biden tweet count':'Biden',
                          'Trump tweet count':'Trump'})

fig1 = px.line(df1, x='id', y=['Biden','Trump'],
               labels=dict(id="Time", value="Sentiment Score", variable="Term/Keyword"))
fig1.update_yaxes(title_font=dict(size=18))
fig2 = px.line(df2, x='id', y=['Biden', 'Trump'],
               labels=dict(id="Time", value="Number of Tweets", variable="Term/Keyword"))
fig2.update_yaxes(title_font=dict(size=18))

app.layout = html.Div(children=[
    html.H1(children='Twitter Keyword Sentiment Analysis'),

    html.Div(children='''
        Realtime tracking of public sentiment through Twitter's streaming API.
    '''),
    dcc.Graph(id='sentiment-graph', figure=fig1),
    dcc.Graph(id='tweet-count-graph', figure=fig2),
    dcc.Interval(id='interval-component',
                 interval=10*1000,
                 n_intervals=0)
])

@app.callback([Output('sentiment-graph', 'figure'),
               Output('tweet-count-graph', 'figure')],
              [Input('interval-component', 'n_intervals')])
def update_example_graph(n):
    df1 = get_data(table)
    df2 = df1[['id','Biden tweet count', 'Trump tweet count']]
    df2 = df2.rename(columns={'Biden tweet count':'Biden',
                              'Trump tweet count':'Trump'})

    fig1 = px.line(df1, x='id', y=['Biden','Trump'],
                   labels=dict(id="Time", value="Sentiment Score", variable="Term/Keyword"))
    fig1.update_yaxes(title_font=dict(size=18))
    fig2 = px.line(df2, x='id', y=['Biden', 'Trump'],
                   labels=dict(id="Time", value="Number of Tweets", variable="Term/Keyword"))
    fig2.update_yaxes(title_font=dict(size=18))

    print(f"updated plots at {datetime.now()}")
    return(fig1,fig2)

if __name__ == '__main__':
    app.run(debug=False, port=8080)
