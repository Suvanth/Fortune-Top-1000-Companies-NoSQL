import json
import pandas as pd

#format
# name,
# revenues,
# revenue_percent_change,
# profits,
# profits_percent_change,
# assets,
# market_value,
# change_in_rank,
# employees 

def processData():
    constructedArr=[]
    df = pd.read_csv('data.csv')
    # df = df.drop('rank', axis=1)
    # print(df.head())
    for index, row in df.iterrows():
        currentData=[]
        currentData.append(row['rank'])  #0
        currentData.append(row['name']) #1
        currentData.append(row['revenues']) #2
        currentData.append(row['revenue_percent_change']) #3
        currentData.append(row['profits']) #4
        currentData.append(row['profits_percent_change']) #5
        currentData.append(row['assets']) #6
        currentData.append(row['market_value']) #7
        currentData.append(row['change_in_rank']) #8
        currentData.append(row['employees']) #9
        constructDict(currentData)
        # print(currentData)
    #     constructedArr.append[constructDict(currentData)]
    # print(constructedArr)


def constructDict(currentData):
    revenueDetailsArr = [
    {'revenues': currentData[2], 'revenuePercent' : currentData[3]},
    ] 
    profitDetailsArr = [
    {'profits': currentData[4], 'profitPercent' : currentData[5]},
    ] 
    person_dict = {
    'name': currentData[1],
    'rank': currentData[0],
    'change_in_rank': currentData[8],
    'revenueDetails':revenueDetailsArr,
    'profitDetails':profitDetailsArr,
    'assets': currentData[6],
    'marketValue': currentData[7]
    }
    document_json = json.dumps(person_dict)
    print(f'{document_json},')

    



    






    # currentData.append[]
    # val = df['name'].values[0]


    # print(val)


processData()
