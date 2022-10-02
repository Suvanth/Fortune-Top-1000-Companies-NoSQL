from itertools import groupby
from tokenize import group
import pymongo
import json


class mongoGuide:
    def createDB(self, client_user, db_name, data_json_file):
        if(self.checkDBexsists(client_user, db_name)):
            print(f'The {db_name} already exists')
        else:
            self.load_mass_json_document(client_user, db_name, data_json_file)
            print('Collection created and data inserted')

    def checkDBexsists(self, client_instance, db_name):
        list_of_db = client_instance.list_database_names()
        if(db_name in list_of_db):
            return True
        else:
            return False

    def load_mass_json_document(self, client_user, db_name, data_json_file):
        db = client_user[db_name]
        with open(data_json_file) as file:
            file_data = json.load(file)
        db.CompanyRanks.insert_many(file_data)


if __name__ == '__main__':
    db_object = mongoGuide()
    client_user = pymongo.MongoClient()
    db_name = 'Fortune1000'
    data_json_file = 'fortune1000.json' 
    db_object.createDB(client_user,db_name, data_json_file)


    




