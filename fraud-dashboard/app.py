import streamlit as st
import pandas as pd
import hazelcast
from hazelcast.sql import HazelcastSqlError
import plotly.express as px
import os
from streamlit_plotly_events import plotly_events
from streamlit_autorefresh import st_autorefresh
import time
import uuid

st.set_page_config(layout="wide")

@st.cache_resource
def get_hazelcast_client(cluster_members=['127.0.0.1']):
    ##client = hazelcast.HazelcastClient(**{'cluster_members':cluster_members})
    client = hazelcast.HazelcastClient(cluster_members=cluster_members, use_public_ip=True,smart_routing=False)
    #run Mapping required to run SQL Queries on JSON objects in predictionResult Map
    client.sql.execute(
        """
            CREATE OR REPLACE MAPPING predictionResult (
            __key VARCHAR,
            transaction_number VARCHAR,
            transaction_date VARCHAR,
            amount DOUBLE,
            merchant VARCHAR,
            merchant_lat DOUBLE,
            merchant_lon DOUBLE,
            credit_card_number VARCHAR,
            customer_name VARCHAR,
            customer_city VARCHAR,
            customer_age_group VARCHAR,
            customer_gender VARCHAR,
            customer_lat DOUBLE,
            customer_lon DOUBLE,
            distance_from_home DOUBLE,
            fraud_model_prediction INT,
            fraud_probability DOUBLE,
            inference_time_ns BIGINT,
            transactions_last_24_hours DOUBLE,
            transactions_last_week DOUBLE,
            amount_spent_last_24_hours DOUBLE,
            timestamp_ms BIGINT
            )
            TYPE IMap
              OPTIONS ('keyFormat' = 'varchar','valueFormat' = 'json-flat');
        """).result()
    
    client.sql.execute(
        """
        CREATE OR REPLACE MAPPING total_map (
            __key INTEGER,
            total_records  DOUBLE,
            total_amount  DOUBLE,
            avg_amount  DOUBLE,
            avg_distance_km DOUBLE
            )
            TYPE IMap
              OPTIONS ('keyFormat' = 'int','valueFormat' = 'json-flat');

        """).result()
    
    return client

#@st.cache_data
def get_df(_client, sql_statement, date_cols):
    #mapping hazelcast SQL Types to Pandas dtypes
    #https://hazelcast.readthedocs.io/en/stable/api/sql.html#hazelcast.sql.SqlColumnType.VARCHAR
    sql_to_df_types = {0:'string',6:'float32',8:'float32',5:'int32',7:'float32',4:'int32',1:'bool'}

    #sql_result = client.sql.execute(sql_statement).result()
    sql_result = client.sql.execute(sql_statement)
    sql_result = sql_result.result()

    #get column metadata from SQL result
    metadata = sql_result.get_row_metadata()
    column_names = [c.name for c in metadata.columns]
    column_types = [sql_to_df_types[c.type] for c in metadata.columns]
    columns_dict = dict(zip(column_names, column_types))


    #build a dict col_name -> list of values 
    column_values = {}
    for c in column_names:
        column_values[c] = []

    #for every row in result
    for row in sql_result:
        for c in column_names:
            value = row.get_object(c)
            column_values[c].append(value)
            
    #create dataframe
    df = pd.DataFrame({key: pd.Series(values) for (key, values) in column_values.items()})
    #apply the right data type of each column
    df = df.astype(columns_dict)
    
    #additional cols for every datetime col
    for col in date_cols:
        if col in column_names:
            df[col] = pd.to_datetime(df[col])
            df[col+'_day_of_week'] = df[col].dt.dayofweek
            df[col+'_month'] = pd.DatetimeIndex(df[col]).month
            df[col+'_hour'] = df[col].dt.hour
    
    return df

#@st.cache_data(ttl=30)
def get_dashboard_totals(fraud_probability_threshold,transaction_amount):
    result = {}
    
    #run queries as jobs and store their outputs into total_map
    sql_statement = """
        CREATE JOB {0} AS
        SINK INTO total_map
        SELECT 1, count(*) as total_records, sum(amount) as total_amount, avg(amount) as avg_amount, avg(distance_from_home) as avg_distance_km
        FROM predictionResult
        """
    sql_statement = sql_statement.format('get_total_'+ str(uuid.uuid4().hex))
    client.sql.execute(sql_statement).result()

    sql_statement = """
        CREATE JOB {0} AS
        SINK INTO total_map
        SELECT 2, count(*) as total_records, sum(amount) as total_amount, avg(amount) as avg_amount, avg(distance_from_home) as avg_distance_km
        FROM predictionResult
        WHERE fraud_probability > ? and amount >= ?
        """
    sql_statement = sql_statement.format('get_total_'+ str(uuid.uuid4().hex))
    client.sql.execute(sql_statement,fraud_probability_threshold,transaction_amount).result()
    
    #get results from map
    total_map = client.get_map("total_map").blocking()

    #Get Overall totals
    res = total_map.get(1).loads()
    if res:
        result['total_records'] = res['total_records']
        result['total_amount'] = round(float(res['total_amount']),2) if res['total_amount'] else 0
        result['avg_amount'] = round(float(res['avg_amount']),2) if res['avg_amount'] else 0
        result['avg_distance_km'] = round(float(res['avg_distance_km']),2) if res['avg_distance_km'] else 0
    
    #Get fraud totals 
    res = total_map.get(2).loads()
    if res:
        result['potential_fraud_records'] = res['total_records'] if res['total_records'] else 0
        result['potential_fraud_amount'] = round(float(res['total_amount']),2) if res['total_amount'] else 0
        result['potential_fraud_per_transaction'] = round(float(res['avg_amount']),2) if res['avg_amount'] else 0
        result['avg_distance_in_potential_fraud_transaction'] = round(float(res['avg_distance_km']),2) if res['avg_distance_km'] else 0
        
    return result

@st.cache_data(ttl=60)
def get_df_3(sql_statement_to_execute,cols):

    result_data = {}
    for col in cols:
        result_data[col]=[]

    tries = 10
    for i in range(tries):
        try:
            with client.sql.execute(sql_statement_to_execute).result() as result:
                for row in result:
                    for col in cols:
                        result_data[col].append(row[col])
        except HazelcastSqlError:
            if i < tries-1:
                time.sleep(0.2 * (1+i))
                continue
        break

    dataframe_result = pd.DataFrame(result_data)
    return dataframe_result

@st.cache_data
def get_categorical_variables():
    categorical_features =['customer_name','customer_city','customer_age_group','customer_gender']
    return categorical_features

#Connect to hazelcast - use env variable HZ_ENDPOINT, if provided
hazelcast_node = os.environ['HZ_ENDPOINT']
start = time.time()
if hazelcast_node:
    client = get_hazelcast_client([hazelcast_node])
else:
    client = get_hazelcast_client()
end_time = time.time()

#get categorical variable names
categorical_features = get_categorical_variables()

#sidebar 
st.sidebar.header('Transaction Fraud Criteria')
probability_threshold = st.sidebar.slider('Fraud Probability Above ',50,100,75,1)
transaction_amount = st.sidebar.slider('Transactions Above (Amount)',100,10000,1000,100)
st.sidebar.header('Key Dimensions','key dimensions')
category_selected = st.sidebar.selectbox('', categorical_features)
st.sidebar.header('Refresh Settings','refresh')
refresh_interval = st.sidebar.slider('Auto Refresh Period (Seconds)',3,30,15,1)

#Continue Loading data
fraud_threshold = probability_threshold / 100
start = time.time()
totals = get_dashboard_totals(fraud_threshold,transaction_amount)
end_time = time.time()


#Main page title and header
st.title('Fraud Analysis Dashboard')
st.header('Most Recent Transactions (300k)','tx_metrics')

col1, col2, col3,col4  = st.columns(4)
with col1:
    st.metric('Total Transactions', totals['total_records'],help='SELECT count(*) from predictionResult')
with col2:
    st.metric('Total Amount', totals['total_amount'],help='Total Trasaction Amount')
with col3:
    st.metric('Avg Amount', totals['avg_amount'],help='Avg Transaction Amount')
with col4:
    st.metric('Avg Distance (km) from home',totals['avg_distance_km'],help='Distance from home in Km')

#Suspected Fraud Summary
st.header('Suspected Fraudulent Transactions','tx_fraud_metrics')
col1_f, col2_f, col3_f,col4_f  = st.columns(4)
with col1_f:    
    st.metric('Total Transactions',totals['potential_fraud_records'],help='SELECT count(*) from predictionResult where fraud_probability > ' + str(probability_threshold) + '%')
with col2_f:
    st.metric('Total Amount ', totals['potential_fraud_amount'],help='Total $ Amount of Predicted Fraud in Transactions with Fraud probability >= ' + str(probability_threshold) + '%')
with col3_f: 
    avg_amount_delta = round(totals['potential_fraud_per_transaction'] - totals['avg_amount'],2)
    
    st.metric('Avg Amount per Transaction',  totals['potential_fraud_per_transaction'], avg_amount_delta,help='Avg Transaction Amount in Transactions with Fraud probability >= ' + str(probability_threshold) + '%')
with col4_f:
    avg_distance_delta = round(totals['avg_distance_in_potential_fraud_transaction'] - totals['avg_distance_km'],2)
    st.metric('Avg Distance (km) from home', totals['avg_distance_in_potential_fraud_transaction'],avg_distance_delta,help='Distance from home in Km in Transactions with Fraud probability > ' + str(probability_threshold) + '%')

#Fraud Sources Header
st.header('Where is Potential Fraud Coming from?','key_dimensions')
col_chart1, col_chart2 = st.columns(2)

#Bubble Chart - SQL Statement
start = time.time()
sql_statement = """
    select count(*) as total_transactions, avg(fraud_probability) as fraud_probability,
        avg(distance_from_home) as distance_from_home,sum(amount) as total_amount,
        {category_selected} 
    from predictionResult 
    where fraud_probability > {fraud_threshold} and amount >= {transaction_amount}
    group by {category_selected}
    order by total_transactions DESC
    limit 50
    """.format(category_selected=category_selected,fraud_threshold=fraud_threshold,transaction_amount=transaction_amount)

#Bubble Chart Dataframe
bubble_data_cols = ['total_transactions','fraud_probability','distance_from_home','total_amount',category_selected]
df_bubble = get_df_3(sql_statement,['total_transactions','fraud_probability','distance_from_home','total_amount',category_selected])
end_time = time.time()

#Bubble Chart
fig = px.scatter(df_bubble, x="total_amount", y="total_transactions",size="total_amount",color=category_selected,
                 hover_name="fraud_probability", log_y=False, log_x=False, size_max = 40, width=600, height=500)

with col_chart1:
    st.subheader("Fraud Hotspots by " + category_selected )
    st.write (end_time-start)
    selected_points = plotly_events(fig,click_event=True,hover_event=False)

#Start by getting impacted from bubble chart
categorical_impacted = df_bubble[category_selected].to_list()
categorical_values= '\'' + '\',\''.join(categorical_impacted) + '\''

# if user already chosen a bubble on chart
total_amount_chosen = 0
if (selected_points):
    total_amount_chosen = selected_points[0]['x']
    selected_value = df_bubble.loc[df_bubble['total_amount'] == total_amount_chosen ][category_selected].to_list()[0]
    categorical_values = '\'' + selected_value + '\''

# MAP SQL 
start = time.time()
map_sql_statement = """
    select merchant_lat as lat,merchant_lon as lon
    from predictionResult 
    where fraud_probability > {fraud_threshold} and {category_selected} in ({categorical_values}) and amount >= {transaction_amount}
    order by fraud_probability DESC
    limit 50
    """.format(category_selected=category_selected,categorical_values=categorical_values,fraud_threshold=fraud_threshold,transaction_amount=transaction_amount)

df_map = get_df(client,sql_statement=map_sql_statement,date_cols=[])
end_time = time.time()

with col_chart2:
    st.subheader("Transaction Locations")
    st.write (end_time-start)
    st.map(df_map,use_container_width=False)
    
    
#Analyst SQL Playground
st.header('Analyst - SQL Playground','sql_playground')
sql_statement = st.text_area('Enter a SQL Query', 'SELECT * \nFROM predictionResult \nLIMIT 100',200)

#heuristic button animation only
deploy_heuristic_button = st.button('Deploy New Fraud Detection Pattern', type="secondary", disabled=False)
if deploy_heuristic_button:
    st.balloons()

#SQL Results
st.header('SQL Results','data')
if sql_statement:
    start = time.time()
    df3 = get_df(client,sql_statement,[])
    end_time = time.time()
    st.write(df3)

count = st_autorefresh(interval = refresh_interval * 1000, limit=10000, key="refresh-counter")

#Other fields in the PredictionResult map (JSONObject)
    #transaction_weekday_code INT,
    #transaction_hour_code INT,
    #transaction_month_code INT,
    #gender_code INT,
    #customer_zip_code INT,
    #customer_city_population INT,
    #customer_age INT,
    #customer_setting_code INT,
    #customer_age_group_code INT,
    #customer_job_code INT,
    #category_code INT