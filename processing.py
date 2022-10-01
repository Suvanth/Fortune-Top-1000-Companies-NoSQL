import json
import pandas as pd

def processData():
    df = pd.read_csv('data.csv')
    for index, row in df.iterrows():
        currentData=[]
        rank = int(row['rank'])
        name = row['name']

        revenues = row['revenues']
        revenues = revenues.replace("$", "").strip()
        revenues = float(revenues.replace(",",""))

        profits = row['profits']
        if(profits=='-'):
            profits='0'
        if('(' in profits):
            profits = profits.replace('(','-')
            profits = profits.replace(')','')
        profits = profits.replace('$','')
        profits = float(profits.replace(',','').strip())

        assets = row['assets']
        assets = assets.replace("$", "").strip()
        assets = float(assets.replace(",",""))

        marketValue = row['market_value']
        marketValue = marketValue.replace("$", "").strip()
        if(marketValue=='-'):
            marketValue='0'
        marketValue = float(marketValue.replace(",",""))

        revenuePercentChange = row['revenue_percent_change'].replace('%','')
        if(revenuePercentChange=='-'):
            revenuePercentChange='0'
        revenuePercentChange=float(revenuePercentChange)

        profitPercentChange = row['profits_percent_change']

        profitPercentChange = row['profits_percent_change'].replace('%','')
        if(profitPercentChange=='-'):
            profitPercentChange='0'
        profitPercentChange=float(profitPercentChange)


        employeeCount = row['employees'].replace(",","") 
        if('-' in employeeCount):
            employeeCount='0'
        employeeCount= int(employeeCount)

        rankChange = row['change_in_rank']
        if(rankChange == "-"):
            rankChange ='0'
        rankChange = int(rankChange)

        currentData.append(rank)  #0             
        currentData.append(name) #1                  
        currentData.append(revenues) #2               
        currentData.append(revenuePercentChange) #3 
        currentData.append(profits) #4                
        currentData.append(profitPercentChange) #5 
        currentData.append(assets) #6                 
        currentData.append(marketValue) #7          
        currentData.append(rankChange) #8 
        currentData.append(employeeCount) #9              
        constructDict(currentData)


def constructDict(currentData):
    revenueDetailsArr = [
    {'revenues':currentData[2],'revenuePercentChange':currentData[3]},
    ] 
    profitDetailsArr = [
    {'profits':currentData[4],'profitPercentChange':currentData[5]},
    ] 
    companySizeArr = [
    {'assets':currentData[6],'marketValue':currentData[7],'employeeCount':currentData[9]},
    ] 
    person_dict = {
    'name': currentData[1],
    'rank': currentData[0],
    'change_in_rank': currentData[8],
    'companyRevenue':revenueDetailsArr,
    'companyProfit':profitDetailsArr,
    'companySize':companySizeArr
    }
    document_json = json.dumps(person_dict)
    print(f'{document_json},')

processData()