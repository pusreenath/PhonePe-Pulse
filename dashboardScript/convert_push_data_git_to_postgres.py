# Created by: @pusreenath https://github.com/pusreenath on 6th May 2023

#Importing Libraries
import pandas as pd
import psycopg2

# Inserting data into postgresSQL
def insert_values(df,table_name,conn,curr):
    columns = df.columns.tolist()
    placeholders = ['%s' if df[col].dtype.kind not in 'fi' else '%s::{}'.format('float' if df[col].dtype.kind == 'f' else 'int') for col in columns]
    insert_query = 'insert into {} ({}) values ({})'.format(table_name, ','.join(columns), ','.join(placeholders))
    # Roll back the current transaction
    conn.rollback()
    curr.execute(f'truncate table {table_name}')
    for row in df.itertuples(index=False):
        curr.execute(insert_query, tuple(row))
        conn.commit()

# Reading the path of the data
def read_quarter_path(datapath):
    quarter_path=list()
    folder_list = os.listdir(datapath)
    for folder in folder_list:
        folder_path = datapath+"\\" +folder
        tu_list = os.listdir(folder_path)
        for tu in tu_list:
            if folder == "map":
                statefolder_path = folder_path+"\\" +tu + "\hover\country\india\state"
            else:
                statefolder_path = folder_path+"\\" +tu + "\country\india\state"
            agg_state_list = os.listdir(statefolder_path)
            for state in agg_state_list:
                state_path = statefolder_path+"\\" +state+ "\\"
                year_list = os.listdir(state_path)
                for year in year_list:
                    year_path = state_path + year +'\\'
                    quarter_list = os.listdir(year_path)
                    for quarter in quarter_list:
                        quarter_path.append(year_path + quarter)
    return quarter_path


datapath = r"C:\Users\upend\Downloads\Guvi\PhonePe-Pulse\data"
json_path_list = read_quarter_path(datapath)

# Creating dataframes for the retrived data
agg_tdata_df = pd.DataFrame()
agg_udata_df = pd.DataFrame()
# map_tdata_df = pd.DataFrame()
# map_udata_df = pd.DataFrame()

districts_df = pd.DataFrame(columns = ['entityName', 'metric_type', 'metric_count', 'metric_amount', 'state', 'year', 'quarter', 'name', 'registeredUsers'])
pincodes_df = pd.DataFrame()

# Inference
# As Map is a custom dataframe and the information can be found by calculating in aggregated and top dataframes, 
# We will be skipping the Map dataframe.

# Normalizing the dataframes.
for json_path in json_path_list:
    split_list = json_path.split('\\')
    state = split_list[-3]
    year = int(split_list[-2])
    quarter = int(split_list[-1].split('.')[0])
    data = open(json_path, 'r')
    data_json = json.load(data)
    if "aggregated" in json_path:
        transactionData_df = pd.DataFrame()
        user_df = pd.DataFrame()
        if "transaction" in json_path:
            transactionData_df = pd.json_normalize(data_json, ['data', 'transactionData'], meta=['success', 'code', ['data', 'from'], ['data', 'to'], 'responseTimestamp'], sep='_')
            transactionData_df = transactionData_df.explode('paymentInstruments').reset_index(drop=True)
            transactionData_df = pd.concat([transactionData_df.drop(['paymentInstruments'], axis=1), transactionData_df['paymentInstruments'].apply(pd.Series)], axis=1)
            transactionData_df['state'] = state
            transactionData_df['year'] = year
            transactionData_df['quarter'] = quarter
            agg_tdata_df = pd.concat([agg_tdata_df,transactionData_df])
        else:
            user_df = pd.json_normalize(
                data_json,
                record_path=['data', 'usersByDevice'],
                meta=['success', 'code', ['data', 'aggregated', 'registeredUsers'], ['data', 'aggregated', 'appOpens'], 'responseTimestamp'], sep='_')
            user_df['state'] = state
            user_df['year'] = year
            user_df['quarter'] = quarter
            agg_udata_df = pd.concat([agg_udata_df,user_df])
#     elif "map" in json_path:
#         transactionData_df = pd.DataFrame()
#         user_df = pd.DataFrame()        
#         if "transaction" in json_path:
#             transactionData_df = pd.json_normalize(
#                 data_json,
#                 record_path=['data', 'hoverDataList'],
#                 meta=['success', 'code', 'responseTimestamp'])
#             transactionData_df = transactionData_df.explode('metric').reset_index(drop=True)
#             transactionData_df = pd.concat([transactionData_df.drop(['metric'], axis=1), transactionData_df['metric'].apply(pd.Series)], axis=1)
#             transactionData_df['state'] = state
#             transactionData_df['year'] = year
#             transactionData_df['quarter'] = quarter
#             map_tdata_df = pd.concat([map_tdata_df,transactionData_df])
#         else:
#             hover_data = data_json['data']['hoverData']
#             user_df = pd.DataFrame.from_dict(hover_data, orient='index')
#             user_df = user_df.reset_index().rename(columns={'index': 'name', 'registeredUsers': 'registered_users', 'appOpens': 'app_opens'})
#             user_df['state'] = state
#             user_df['year'] = year
#             user_df['quarter'] = quarter
#             map_udata_df = pd.concat([map_udata_df,user_df])
    elif "top" in json_path:        
        # Creating a DataFrame from the districts data
        dist_df = pd.json_normalize(data_json['data']['districts'], sep='_')
        dist_df['state'] = state
        dist_df['year'] = year
        dist_df['quarter'] = quarter
        if "transaction" in json_path:
            dist_df['transaction_or_users'] = "transaction"
        else:
            dist_df['transaction_or_users'] = "users"
        districts_df = pd.concat([districts_df,dist_df], axis=0, ignore_index=True)
        # Creating a DataFrame from the pincodes data
        pin_df = pd.json_normalize(data_json['data']['pincodes'], sep='_')
        pin_df['state'] = state
        pin_df['year'] = year
        pin_df['quarter'] = quarter
        if "transaction" in json_path:
            pin_df['transaction_or_users'] = "transaction"
        else:
            pin_df['transaction_or_users'] = "users"
        pincodes_df = pd.concat([pincodes_df,pin_df], axis=0, ignore_index=True)

#removing success,code,type from agg_tdata_df and success,code from agg_udata_df as they are constants

agg_tdata_df.drop(columns=['success','code','type'],inplace = True)
agg_udata_df.drop(columns=['success','code'],inplace = True)

#removing metric_type from districts_df and pincodes_df as they are constants
districts_df.drop(columns=['metric_type'],inplace = True)
pincodes_df.drop(columns=['metric_type'],inplace = True)

#removing responseTimestamp from agg_tdata_df and agg_udata_df as we already have year and quarter fields
# removing 'data_from','data_to' from agg_tdata_df as we already have year and quarter fields 
agg_tdata_df.drop(columns=['responseTimestamp','data_from','data_to'],inplace = True)
agg_udata_df.drop(columns=['responseTimestamp'],inplace = True)

agg_udata_df.drop(columns=['percentage'],inplace = True)

# Converting Data type object to int for columns data.aggregated.registeredUsers, data.aggregated.appOpens,count
agg_udata_df[['data_aggregated_registeredUsers', 'data_aggregated_appOpens','count']] = agg_udata_df[['data_aggregated_registeredUsers', 'data_aggregated_appOpens','count']].astype('int64')


# Converting Data type object to int for columns metric_count, year, quarter
districts_df[[ 'year', 'quarter']] = districts_df[[ 'year', 'quarter']].astype('int64') 
# Converting Nan values of metric_count, registeredUsers to 0
districts_df['metric_count'].fillna(0,inplace = True)
districts_df['registeredUsers'].fillna(0,inplace = True)

# Converting Nan values of metric_count,registeredUsers to 0
pincodes_df['metric_count'].fillna(0,inplace = True)
pincodes_df['registeredUsers'].fillna(0,inplace = True)
# Converting Data type object to int for columns metric_count, registeredUsers
pincodes_df['metric_count']= pincodes_df['metric_count'].astype('int64') 
pincodes_df['registeredUsers']= pincodes_df['registeredUsers'].astype('int64') 

#postgres

# Connect to the database
conn = psycopg2.connect(
    host="localhost",
    user="postgres",
    password="postgres@123",
    port=5432,
    database="phonepe_pulse"
)

curr = conn.cursor()

insert_values(agg_tdata_df,"agg_tdata",conn,curr)
insert_values(agg_udata_df,"agg_udata",conn,curr)
insert_values(districts_df,"districts",conn,curr)
insert_values(pincodes_df,"pincodes",conn,curr)