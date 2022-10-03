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
    df = pd.read_csv('Fortune 1000 Companies by Revenue.csv')
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

        revenuePercentChange = row['revenue_percent_change']
        profitPercentChange = row['profits_percent_change']
        
        employeeCount = row['employees'].replace(",","") 
        if('-' in employeeCount):
            employeeCount='0'
        employeeCount= int(employeeCount)

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
        #constructDict(currentData)
    return df
