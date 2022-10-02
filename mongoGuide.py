from itertools import groupby
from tokenize import group
import pymongo
import json

class mongoGuide:
    def create_db(self, client_user, db_name, data_json_file):
        db = client_user[db_name]
        self.load_mass_json_document(db, data_json_file)
        print('Collection created and data inserted')
        # SAMPLE QUERIES MAPPED TO FUNCTIONS -> uncomment to run
        # self.rank_calculations(db)
        # self.kpi_calculations(db)
        # self.logical_operators(db)

    def drop_collecition_db(self, client_user):
        db_name='DatabaseDrop'
        db = client_user[db_name]
        with open('fortune1000.json') as file:
            file_data = json.load(file)
        db.CompanyRanksDrop.insert_many(file_data)
        db.CompanyRanksNotDrop.insert_many(file_data)
        print(f'++++++++++++++++++++++++++++++++++++++++++++++++++')
        print(f'ACTION Collections created in Database: {db_name}')
        for coll in db.list_collection_names():
            print(coll)
        print(f'++++++++++++++++++++++++++++++++++++++++++++++++++')
        print(f'ACTION Dropping CompanyRanksDrop collection in {db_name}')        
        collectionToDrop = db["CompanyRanksDrop"]
        collectionToDrop.drop()
        print(f'Updated Collections in {db_name}')
        for coll in db.list_collection_names():
            print(coll)
        print(f'++++++++++++++++++++++++++++++++++++++++++++++++++')
        print(f'ACTION Databases created and present for client') 
        for db_mongo in client_user.list_database_names():
            print(db_mongo)
        print(f'++++++++++++++++++++++++++++++++++++++++++++++++++')
        print(f'ACTION Dropping {db_name}')
        client_user.drop_database('DatabaseDrop')
        for db_mongo in client_user.list_database_names():
            print(db_mongo)
        print(f'++++++++++++++++++++++++++++++++++++++++++++++++++')

    def check_db_exsists(self, client_instance, db_name):
        list_of_db = client_instance.list_database_names()
        if (db_name in list_of_db):
            return True
        else:
            return False

    def construct_document(self, current_data):
        revenue_details_arr = {
            'revenues': current_data[2], 'revenuePercentChange': current_data[3]}
        profit_details_arr = {
            'profits': current_data[4], 'profitPercentChange': current_data[5]}
        company_size_arr = {
            'assets': current_data[6], 'marketValue': current_data[7], 'employeeCount': current_data[9]}
        person_dict = {
            'name': current_data[1],
            'rank': current_data[0],
            'change_in_rank': current_data[8],
            'companyRevenue': revenue_details_arr,
            'companyProfit': profit_details_arr,
            'companySize': company_size_arr
        }
        return person_dict

    def load_mass_json_document(self, db, data_json_file):
        with open(data_json_file) as file:
            file_data = json.load(file)
        db.CompanyRanks.insert_many(file_data)
        document_parameters = ['1', 'MongoInc', 10000, 20.3, 20000, 12.3, 300000, 2000000, 12, 30]
        document = self.construct_document(document_parameters)
        db.CompanyRanks.insert_one(document)
        collection_insert = db['CompanyRanks']
        count_coll = collection_insert.count_documents({})
        print(
            f'The expected document count is 10001 as we have created and inserted an additional document Actual Document count = {count_coll}')

    def sort_documents(self, client_user, db_name, data_json_file):
        db = client_user[db_name]
        self.load_mass_json_document(db, data_json_file)
        collectionSort = db['CompanyRanks']
        # sort("name", 1) #ascending
        # sort("name", -1) #descending
        mydoc = collectionSort.find().sort("name", -1)
        for x in mydoc:
            print(x)
    
    def delete_documents(self, client_user):
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

    def update_documents(self, client_user):
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

    def rank_calculations(self, db):
        cursor = db.CompanyRanks.aggregate(
            [
                {
                    "$group":
                    {
                        "_id": "RankCalcs",
                        "max_change_rank": {"$max": "$change_in_rank"},
                        "min_change_rank": {"$min": "$change_in_rank"},
                        "average_change_rank": {"$avg": "$change_in_rank"},
                    }
                }
            ]
        )
        for item in cursor:
            print(item)

    def kpi_calculations(self, db):
        cursor = db.CompanyRanks.aggregate(
            [
                {
                    "$group":
                    {
                        "_id": "KPICalcs",
                        "max_profit": {"$max": "$companyProfit.profits"},
                        "min_profit": {"$min": "$companyProfit.profits"},
                        "average_profit": {"$min": "$companyProfit.profits"},

                        "max_revenue": {"$max": "$companyRevenue.revenues"},
                        "min_revenue": {"$min": "$companyRevenue.revenues"},
                        "average_revenues": {"$min": "$companyRevenue.revenues"},

                        "max_asset": {"$max": "$companySize.assets"},
                        "min_asset": {"$min": "$companySize.assets"},
                        "average_assets": {"$min": "$companySize.assets"},

                        "max_marketvalue": {"$max": "$companySize.marketValue"},
                        "min_marketvalue": {"$min": "$companySize.marketValue"},
                    }
                }
            ]
        )
        for item in cursor:
            print(item)

    def logical_operators(self, db):
        collection = db['CompanyRanks']
        results = collection.find({"$and": [
            {
                "change_in_rank": {"$lte": 3}
            },
            {
                "rank": {"$gt": 5}
            }
        ]
        })
        for doc in results:
            print(doc)
    
if __name__ == '__main__':
    db_object = mongoGuide()
    client_user = pymongo.MongoClient()
    db_name = 'Fortune1000'
    data_json_file = 'fortune1000.json'
    # SAMPLE FUNCTIONS uncomment to run
    # db_object.create_db(client_user,db_name, data_json_file)
    # db_object.drop_collecition_db(client_user)
    # db_object.update_documents(client_user)
    # db_object.delete_documents(client_user)
    # db_object.sort_documents(client_user,db_name, data_json_file)
    client_user.close()