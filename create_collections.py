import pymongo

DATABASE = "noobe"

CAPPED_COLL = "cappedData"
CAPPED_SIZE = 100000

DATA_COLL = "data"

db = pymongo.MongoClient()[DATABASE]

try:
    capped = db.create_collection(CAPPED_COLL, **{"capped": True,
                                                  "autoIndexId": True,
                                                  "size":CAPPED_SIZE,})
except pymongo.errors.CollectionInvalid:
    print("Existent Capped database, not creating")

data = db[DATA_COLL]

data.ensure_index([("timestamp", pymongo.DESCENDING)],
                                  unique=False)

data.ensure_index("id_sensor", unique=False)

data.ensure_index([("timestamp", pymongo.DESCENDING),
                   ("id_sensor", pymongo.ASCENDING)],
                  unique=True)

    
