import tornado.web
from tornado.ioloop import IOLoop
from tornado import httpserver, gen
from tornado.options import define, options
import tornado.escape
import datetime
from motor import MotorClient
from pymongo import ASCENDING,DESCENDING
from bson.objectid import ObjectId
from parse import parse_message
from tcp_server import TCPSocketHandler

# Capped collection, es una coleccion con numero maximo de
# elementos, nos permite hacer cursores "tailables", lo
# cual nos permite hacer long polling
options.define("database", default="noobe", help="Base de datos de mongoDB")
options.define("capped-coll", default="cappedData", help="'Capped colection' a utilizar")
options.define("store-coll", default="data", help="Coleccion donde almacenar la data")

options.define("port", default=8080, help="run HTTP server on the given port", type=int)
options.define("tcp-port", default=8989, help="run TCP server on the given port", type=int)

_db_object = MotorClient()[options["database"]]

@gen.coroutine
def insert_data(data_dict, db=_db_object,
                store_coll=options["store-coll"],
                capped_coll=options["capped-coll"]):
    """Inserta un objeto en la base de datos"""
    item = data_dict.copy()
    item["_id"] = ObjectId()

    future_store = db[store_coll].insert(item)
    future_capped = db[capped_coll].insert(item)
    yield [future_store, future_capped]

    if future_store.exception or future_capped.exception:
        return None
    else:
        print("Dato insertado en la DB")
        return item["_id"]

@gen.coroutine
def get_data(query, limit, from_store=False,
             tailable=False,
                  db=_db_object,
                  store_coll=options["store-coll"],
                  capped_coll=options["capped-coll"]):
    if from_store:
        return db[store_coll].find(query, sort=[("$natural", DESCENDING)]).limit(limit)
    elif tailable:
        return db[capped_coll].find(query, tailable=True).limit(limit)
    else:
        return db[capped_coll].find(query, sort=[("$natural", DESCENDING)]).limit(limit)


class DataHandler(tornado.web.RequestHandler):
    @gen.coroutine
    def get(self):
        """ Devuelve data de la base de datos

        La URL acepta los parametros "after" y "count"
        "after" hace que se devuelvan los valores insertados
        despues del objeto de _id igual a "after".
        "count" indica el numero maximo de valores devueltos.

        Pide data a la "capped collection", usamos esta
        collection a manera de coleccion de cache para
        poder hacer long polling con el parametro "tailable"
        """
        historic = (self.get_argument("historic", None) is not None)
        after = self.get_argument("after", None)
        before = self.get_argument("before", None)
        count = int(self.get_argument("count", 100) )

        match =  {k:ObjectId(val) for k,val in [("$gt", after), ("lt", before)] if val}

        query = {"_id": {"$elemenMatch": match}} if match else {}

        cursor = yield get_data(query, count, historic)
        data = []
        last_id = None
        first_id  = None 
        # Tomamos todo lo que podamos
        while (yield cursor.fetch_next):
            item = cursor.next_object()
            # Removemos el _id, pero antes, lo almacenamos
            _id = item.pop("_id")
            first_id = first_id or _id
            last_id = _id
            data.append(item)

        # Debemos devolver aunque sea un dato
        if(len(data) == 0):
            cursor = yield get_data(query, count, historic, True)
            while (len(data) == 0 and cursor.alive):
                while (yield cursor.fetch_next):
                    item = cursor.next_object()
                    # Removemos el _id, pero antes, lo almacenamos
                    _id = item.pop("_id")
                    first_id = first_id or _id
                    last_id = _id
                    data.append(item)

        self.write({
            "data": data,
            "last": str(last_id),
            "first": str(first_id),
        })


    @gen.coroutine
    def post(self):
        db = self.settings["db"]

        msg_dict  = parse_message(self.request.body)
        dict_id= yield insert_data(msg_dict)

        self.write("OK" if dict_id else "ERROR")

handlers = [
    (r"/api", DataHandler),
]

settings = {
    "debug": True,
    "xsrf_cookies": False,
    "store_coll": options["store-coll"],
    "capped_coll": options["capped-coll"],
}

web_app = tornado.web.Application(handlers, **settings)

def deploy_server():
    http_server = httpserver.HTTPServer(web_app)
    tcp_server = TCPSocketHandler(insert_data)
    http_server.listen(options.port)
    tcp_server.listen(options["tcp-port"])
    print("HTTP Server Running in port: {:d}".format(options.port))
    print("TCP Server Running in port: {:d}".format(options["tcp-port"]))
    IOLoop.current().start()

if __name__ =="__main__":
    deploy_server()
