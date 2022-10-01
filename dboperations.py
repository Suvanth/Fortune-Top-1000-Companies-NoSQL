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
        newvalues = { "$set": { "companySize.0.employeeCount": 10 } }
        collectionUpdate.update_one(update_query, newvalues)
        client_user.drop_database('DatabaseUpdate')






dbObject = dboperations()
# dbObject.updateDocumentsDemo()
# dbObject.createDB()
#dbObject.dropDemonstration()
#dbObject.deleteDocumentsDemo()






#start mongo db instance DONE
# brew services start mongodb/brew/mongodb-community
# brew services restart mongodb-community

#Create
#Data 
#create db              DONE
#create collection      DONE
#insert mass json       DONE  
#drop collection        DONE
#drop db                DONE
#delete a record        DONE
#update a record        DONE


#TO DO
#sort
#insert one json EZ    
#two filters and or queries









