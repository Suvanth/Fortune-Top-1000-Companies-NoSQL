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
    df.replace(to_replace="-",value="0")
    # df = df.drop('rank', axis=1)
    # print(df.head())
    for index, row in df.iterrows():
        currentData=[]
        rank = int(row['rank'])
        name = row['name']

        revenues = row['revenues']
        revenues = revenues.replace("$", "").strip()
        revenues = float(revenues.replace(",",""))

        profits = row['profits']
        profits = profits.replace("$", "").strip()
        profits = float(profits.replace(",",""))

        assets = row['assets']
        assets = assets.replace("$", "").strip()
        assets = float(assets.replace(",",""))

        marketValue = row['market_value']
        marketValue = marketValue.replace("$", "").strip()
        marketValue = float(marketValue.replace(",",""))


        revenuePercentChange = row['revenue_percent_change']
        profitPercentChange = row['profits_percent_change']
        employeeCount = int(row['employees'].replace(",",""))






        



        currentData.append(int(row['rank']))  #0             DONE
        currentData.append(row['name']) #1                   DONE
        currentData.append(row['revenues']) #2               DONE
        currentData.append(row['revenue_percent_change']) #3 DONE
        currentData.append(row['profits']) #4                DONE
        currentData.append(row['profits_percent_change']) #5 DONE
        currentData.append(row['assets']) #6                 DONE
        currentData.append(row['market_value']) #7           DONE
        currentData.append(row['change_in_rank']) #8 
        currentData.append(row['employees']) #9              DONE
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
    #print(f'{document_json},')

    



    






    # currentData.append[]
    # val = df['name'].values[0]


    # print(val)


processData()
