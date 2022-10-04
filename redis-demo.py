import redis
import json
import time
import pandas as pd


def connect(redis_url):
    '''
    Connects to the redis database
    ARGS: url
    Return: redis instance
    '''
    r = redis.StrictRedis.from_url(redis_url)
    return r

def set_row(r,key,value):
    r.set(key,value)
    return

def set_company(r,df):
    '''
    Loads data from a dataframe into the redis database
    ARGS: redis instance, filename
    Return: Boolean 
    '''
    for index in df.index:
        data = df.loc[index].to_dict()
        r.set(data['name'],json.dumps(data))
    return

def set_company_pipeline(r,df):
    '''
    Loads data from a dataframe into the redis database
    ARGS: redis instance, filename
    Return: Boolean 
    '''
    pipeline = r.pipeline()
    for index in df.index:
        data = df.loc[index].to_dict()
        pipeline.set(data['name'],json.dumps(data))
    pipeline.execute()
    return

def set_company_revenue_pipeline(r,df):
    '''
    Loads data from a dataframe into the redis database
    ARGS: redis instance, filename
    Return: Boolean 
    '''
    df1 = df[['name','revenues','profits','market_value']]
    pipeline = r.pipeline()
    for index in df1.index:
        data = df1.loc[index].to_dict()
        pipeline.set(data['name']+'_revenue',json.dumps(data))

    pipeline.execute()
    return

def set_company_profit_pipeline(r,df):
    '''
    Loads data from a dataframe into the redis database
    ARGS: redis instance, filename
    Return: Boolean 
    '''
    df1 = df[['name','profits','profits_percent_change']]
    pipeline = r.pipeline()
    for index in df1.index:
        data = df1.loc[index].to_dict()
        pipeline.set(data['name']+'_profit',json.dumps(data))
    pipeline.execute()
    return

def set_company_size_pipeline(r,df):
    '''
    Loads data from a dataframe into the redis database
    ARGS: redis instance, filename
    Return: Boolean 
    '''
    df1 = df[['name','market_value','employees','assets']]
    pipeline = r.pipeline()
    for index in df1.index:
        data = df1.loc[index].to_dict()
        pipeline.set(data['name']+'_size',json.dumps(data))
    pipeline.execute()
    return

def get_all(r):
    '''
    Gets all the values by their keys
    ARGS: redis instance
    '''
    for key in r.keys():
        print(r.get(key))
    return

def get_row(r,key):
    '''
    Returns the corresponding row given a key
    ARGS: redis instance, key
    '''
    return r.get(key)

def delete_row(r,key):
    r.delete(key)
    return

def delete_all(r):
    pipeline = r.pipeline()
    for key in r.keys():
        pipeline.delete(key)
    pipeline.execute()
    return

def processData():
    df = pd.read_csv('Fortune 1000 Companies by Revenue.csv')
    return df

def main():
    print("Connecting to database...")
    r = connect('redis://default:redispw@localhost:49153')
    print("Connected to redis database at redis://default:redispw@localhost:49153")

    print("Clearing database for testing...")
    delete_all(r)
    print("Database cleared")

    print("Cleaning data to load...")
    data = processData()
    print("Data cleaned successfuly.")

    print("Comparing Pipeline vs None Pipelined set")
    start = time.time()
    set_company(r,data)
    end = time.time()
    non_pipelined_set = end - start

    start = time.time()
    set_company_pipeline(r,data)
    end = time.time()
    pipelined_set = end - start

    print("Pipelined set:",pipelined_set,"vs None pipelined set: ",non_pipelined_set)

    print("Setting company revenue, company profit, company size, using pipelining")
    set_company_revenue_pipeline(r,data)
    set_company_profit_pipeline(r,data)
    set_company_size_pipeline(r,data)
    print("Done")

if __name__ == "__main__":
    main()

