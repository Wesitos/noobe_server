import tornado.web
import tornado.ioloop
from tornado import httpserver, gen
from tornado.options import define, options
import tornado.escape
import datetime
import motor
from bson.objectid import ObjectId
from parse import parse_message

# Capped collection, es una coleccion con numero maximo de
# elementos, nos permite hacer cursores "tailables", lo
# cual nos permite hacer long polling
options.define("database", default="noobe", help="Base de datos de mongoDB")
options.define("capped-coll", default="cappedData", help="'Capped colection' a utilizar")
options.define("store-coll", default="data", help="Coleccion donde almacenar la data")

options.define("port", default=9090, help="run on the given port", type=int)

@gen.coroutine
def insert_data(data_dict, db=options["database"],
                data_coll=options["data-coll"],
                capped_coll=options["store-coll"]):
    """Inserta un objeto en la base de datos"""
    item = data_dict.copy()
    item["_id"] = ObjectId()

    future_store = db[self.settings["store_coll"]].insert(item)
    future_capped = db[self.settings["capped_coll"]].insert(item)
    yield [future_store, future_capped]

    if future_store.exception or future_capped.exception:
        return None
    else:
        return item["_id"]

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
        after = self.get_argument("after", None)
        count = int(self.get_argument("count", 100) )

        query = {"_id":{"$gt": ObjectId(after)}} if after else {}

        db = self.settings["db"]
        capped_coll = self.settings["capped_coll"]
        cursor = db[capped_coll].find(query, tailable=True).limit(count)
        data = []

        # Debemos devolver aunque sea un dato
        while len(data) == 0:
            # Tomamos todo lo que podamos
            while (yield cursor.fetch_next):
                item = cursor.next_object()
                # Removemos el _id, pero antes, lo almacenamos
                last_id = item.pop("_id")
                data.append(item)

        self.write({
            "data": data,
            "last": str(last_id),
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
    "db": motor.MotorClient()[options.database],
    "store_coll": options["store-coll"],
    "capped_coll": options["capped-coll"],
}

web_app = tornado.web.Application(handlers, **settings)

def deploy_server():
    http_server = httpserver.HTTPServer(web_app)
    http_server.listen(options.port)
    print("Server Running in port: {:d}".format(options.port))
    tornado.ioloop.IOLoop.instance().start()

if __name__ =="__main__":
    deploy_server()
