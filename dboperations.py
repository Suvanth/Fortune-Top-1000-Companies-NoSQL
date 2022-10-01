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
        print('collection created and data inserted')
        collectionToDrop = db["CompanyRanksDrop"]
        collectionToDrop.drop()
        


dbObject = dboperations()
# dbObject.createDB()
dbObject.dropDemonstration()


# myclient = pymongo.MongoClient("mongodb://localhost:27017/")
# mydb = myclient["mydatabase"]
# mycol = mydb["customers"]

# mycol.drop()




#start mongo db instance 
# brew services start mongodb/brew/mongodb-community
# brew services restart mongodb-community

#Create
#create db
#create collection

#insert mass json

#insert one json

#update a record

#delete a record
#drop collection 
#drop db

#two filters and or queries

#drop collection 






