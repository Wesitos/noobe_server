import pymongo
from pymongo import Connection

DATABASE = "noobe"
CAPPED_COLL = "cappedData"

c = Connection()
c.drop_database(DATABASE)


