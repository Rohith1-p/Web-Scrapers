import unittest

import pymongo


class Test(unittest.TestCase):
    mongo_host = ''
    mongo_port = ''
    mongo_db = ''
    mongo_db_base = ''
    mongo_collection = 'media'
    mongo_collection_base = 'media'
    mongo_client = pymongo.MongoClient(mongo_host, int(mongo_port))
    mongo_db = mongo_client[mongo_db]
    mongo_db_base = mongo_client[mongo_db_base]
    mongo_collection = mongo_db[mongo_collection]
    mongo_collection_base = mongo_db_base[mongo_collection_base]
