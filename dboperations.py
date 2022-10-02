from itertools import groupby
from tokenize import group
import pymongo
import json

class dboperations:
    def createDB(self):
        client_user = pymongo.MongoClient() # creates mongo client
        db_name='FortuneCompanies'
        if(self.checkDBexsists(client_user, db_name)):
            print('Exists !!')
        else:
            db = client_user[db_name]
            with open('fortune1000.json') as file:
                file_data = json.load(file)
            db.CompanyRanks.insert_many(file_data)
            print('collection created and data inserted')

    def checkDBexsists(self, client_instance, db_name):
        list_of_db = client_instance.list_database_names()
        if(db_name in list_of_db):
            return True
        else:
            return False
 
    def dropDemonstration(self):
        client_user = pymongo.MongoClient() # creates mongo client
        db_name='DatabaseDrop'
        db = client_user[db_name]
        with open('fortune1000.json') as file:
            file_data = json.load(file)
        db.CompanyRanksDrop.insert_many(file_data)
        db.CompanyRanksNotDrop.insert_many(file_data)
        print(f'Collections created and present in {db_name}')
        for coll in db.list_collection_names():
            print(coll)
        print('\n')
        print(f'Dropping CompanyRanksDrop collection in {db_name}')        
        collectionToDrop = db["CompanyRanksDrop"]
        collectionToDrop.drop()
        print(f'Updated Collections in {db_name}')
        for coll in db.list_collection_names():
            print(coll)
        print('\n')
        print(f'Databases created and present for client') 

        for dbMong in client_user.list_database_names():
            print(dbMong)
        print('\n')
        print(f'Dropping {db_name}\n')
        client_user.drop_database('DatabaseDrop')
        for dbMong in client_user.list_database_names():
            print(dbMong)
    
    def deleteDocumentsDemo(self):
        client_user = pymongo.MongoClient() # creates mongo client
        db_name='DatabaseDelete'
        db = client_user[db_name]
        with open('fortune1000.json') as file:
            file_data = json.load(file)
        db.CompanyRanksDelete.insert_many(file_data)
        collectionDelete = db['CompanyRanksDelete']

        #Delete one document with the name Walmart
        delete_query = { "name": "Walmart" }
        collectionDelete.delete_one(delete_query)

        for x in collectionDelete.find():
            print(x)
        
        # delete all documents which have a name starting with A
        manyDeleteQuery = { "name": {"$regex": "^A"} }
        x = collectionDelete.delete_many(manyDeleteQuery)
        print(x.deleted_count, " documents deleted.")


        #delete all documents
        x = collectionDelete.delete_many({})
        print(x.deleted_count, " documents deleted.")

        client_user.drop_database('DatabaseDelete')

    def updateDocumentsDemo(self):
        client_user = pymongo.MongoClient() # creates mongo client
        db_name='DatabaseUpdate'
        db = client_user[db_name]
        with open('fortune1000.json') as file:
            file_data = json.load(file)
        db.CompanyRanksUpdate.insert_many(file_data)
        collectionUpdate = db['CompanyRanksUpdate']

        update_query = { "name": "Apple" }
        newvalues = { "$set": { "change_in_rank": 10 } }
        collectionUpdate.update_one(update_query, newvalues)

        for x in collectionUpdate.find():
            print(x)
        

        manyUpdateQuery = { "rank": { "$lt": 5} }
        newvalues = { "$set": { "rank": "-1" } }
        x = collectionUpdate.update_many(manyUpdateQuery, newvalues)
        print(x.modified_count, "documents updated.")

        #updated nested element
        update_query = { "name": "Apple" }
        newvalues = { "$set": { "companySize.employeeCount": 10 } }
        collectionUpdate.update_one(update_query, newvalues)
        client_user.drop_database('DatabaseUpdate')

    
    def sortDocuments(self):
        client_user = pymongo.MongoClient() # creates mongo client
        db_name='DatabaseUpdate'
        db = client_user[db_name]
        with open('fortune1000.json') as file:
            file_data = json.load(file)
        db.CompanyRanksUpdate.insert_many(file_data)
        collectionSort = db['CompanyRanksUpdate']

        #sort("name", 1) #ascending
        # sort("name", -1) #descending
        mydoc = collectionSort.find().sort("name", -1)
        for x in mydoc:
            print(x)

    def constructDict(self, currentData):
        revenueDetailsArr = {'revenues':currentData[2],'revenuePercentChange':currentData[3]}
        profitDetailsArr = {'profits':currentData[4],'profitPercentChange':currentData[5]}
        companySizeArr = {'assets':currentData[6],'marketValue':currentData[7],'employeeCount':currentData[9]}

        person_dict = {
        'name': currentData[1],
        'rank': currentData[0],
        'change_in_rank': currentData[8],
        'companyRevenue':revenueDetailsArr,
        'companyProfit':profitDetailsArr,
        'companySize':companySizeArr
        }
        return person_dict
    
    def insertRecords(self):
        documentParameters = ['1', 'MongoInc', 10000, 20.3, 20000, 12.3, 300000, 2000000, 12, 30]
        document = self.constructDict(documentParameters)
        client_user = pymongo.MongoClient() # creates mongo client
        db_name='DatabaseInsert6'
        db = client_user[db_name]
        db.CompanyRanks.insert_one(document)
        with open('fortune1000.json') as file:
            file_data = json.load(file)
        db.CompanyRanks.insert_many(file_data)
        collectionInsert = db['CompanyRanks']
        countColl = collectionInsert.count_documents({})
        print(f'The expected document count is 10001 as we have created and inserted an additional document      Actual Document count = {countColl}')
    

    def rankCalculations(self):
        client_user = pymongo.MongoClient() # creates mongo client
        db_name='DatabaseCalcnew'
        client_user = pymongo.MongoClient() # creates mongo client
        db = client_user[db_name]
        with open('fortune1000.json') as file:
            file_data = json.load(file)
        db.CompanyRanks.insert_many(file_data)
        cursor=db.CompanyRanks.aggregate(
            [
                {
                    "$group":
                    {
                        "_id": "RankCalcs",
                        "max_change_rank": { "$max": "$change_in_rank" },
                        "min_change_rank": { "$min": "$change_in_rank" },
                        "average_change_rank": { "$avg": "$change_in_rank" },
                    }
                }
            ]
        )
        for item in cursor:
            print(item)

    def kpiCalculations(self):
        client_user = pymongo.MongoClient() # creates mongo client
        db_name='DatabaseCalcKPI2'
        client_user = pymongo.MongoClient() # creates mongo client
        db = client_user[db_name]
        with open('fortune1000.json') as file:
            file_data = json.load(file)
        db.CompanyRanks.insert_many(file_data)
        cursor=db.CompanyRanks.aggregate(
            [
                {
                    "$group":
                    {
                        "_id": "KPICalcs",
                        "max_profit": { "$max": "$companyProfit.profits" },
                        "min_profit": { "$min": "$companyProfit.profits" },
                        "average_profit": { "$min": "$companyProfit.profits" },

                        "max_revenue": { "$max": "$companyRevenue.revenues" },
                        "min_revenue": { "$min": "$companyRevenue.revenues" },
                        "average_revenues": { "$min": "$companyRevenue.revenues" },

                        "max_asset": { "$max": "$companySize.assets" },
                        "min_asset": { "$min": "$companySize.assets" },
                        "average_assets": { "$min": "$companySize.assets" },

                        "max_marketvalue": { "$max": "$companySize.marketValue" },
                        "min_marketvalue": { "$min": "$companySize.marketValue" },
                    }
                }
            ]
        )
        for item in cursor:
            print(item)
    
    def logicalOperators(self):
        client_user = pymongo.MongoClient() # creates mongo client
        db_name='DatabaseLogicOperatorstgy'
        client_user = pymongo.MongoClient() # creates mongo client
        db = client_user[db_name]
        with open('fortune1000.json') as file:
            file_data = json.load(file)
        db.CompanyRanks.insert_many(file_data)
        collection = db['CompanyRanks']
        results= collection.find({ "$and" : [
            {
                "change_in_rank" : {"$lte": 3}
            },
            {
                "rank" : {"$gt": 5}
            }
            ]
        })


        icount = 0
        for doc in results:
            icount+=1
            # print(doc)
        print(icount)
        
        
        


      

dbObject = dboperations()
# dbObject.logicalOperators()
# dbObject.kpiCalculations()
# dbObject.rankCalculations()
# dbObject.updateDocumentsDemo()
# dbObject.createDB()
#dbObject.dropDemonstration()
#dbObject.deleteDocumentsDemo()
dbObject.sortDocuments()
#dbObject.insertRecords()






#start mongo db instance DONE
# brew services start mongodb/brew/mongodb-community
# brew services restart mongodb-community

'''
TASKS
data preparation                               DONE
mongo instance creation                        DONE
client connection handling                     DONE 
create db                                      DONE
Check Db exsists                               DONE
Check Db collections                           DONE
create collection                              DONE
drop collection                                DONE
drop db                                        DONE
delete a individual document                   DONE
delete many documents according to criteria    DONE
update a record                                DONE
sort                                           DONE
Documemnt construction process                 DONE
insert one document                            DONE  
insert mass document                           DONE    

TO DO
two filters and or queries - Logical operators


find with mutli fields


'''








