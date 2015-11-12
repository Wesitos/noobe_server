import tornado.web
import tornado.ioloop
from tornado import httpserver, gen
from tornado.options import define, options
import tornado.escape
import datetime
import motor
from bson.objectid import ObjectId

# Capped collection, es una coleccion con numero maximo de
# elementos, nos permite hacer cursores "tailables", lo
# cual nos permite hacer long polling
options.define("database", default="noobe", help="Base de datos de mongoDB")
options.define("capped-coll", default="cappedData", help="'Capped colection' a utilizar")
options.define("store-coll", default="data", help="Coleccion donde almacenar la data")

options.define("port", default=9090, help="run on the given port", type=int)

@gen.coroutine
def insert_data(data_dict, db, data_coll, capped_coll):
    """Inserta un objeto en la base de datos"""
    item = data_dict.copy()
    item["_id"] = ObjectId()

    future_store = db[self.settings["store_coll"]].insert(item)
    future_capped = db[self.settings["capped_coll"]].insert(item)

    return [future_store, future_capped]


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
        data_coll = self.settings["data_coll"]
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
        body = tornado.escape.json_decode(self.request.body)
        if not (body.get("timestamp") and
                body.get("id_sensor") and
                body.get("lat") and 
                body.get("lon") and
                type(body.get("data")) == dict):
            self.send_error(400)
        item = {
            "_id": ObjectId(),
            "timestamp": body.get("timestamp"),
            "id_sensor": body.get("id_sensor"),
            "lat": body.get("lat"),
            "lon": body.get("lon"),
            "data": body.get("data"),
        }
        future_store = db[self.settings["data_coll"]].insert(item)
        future_capped = db[self.settings["capped_coll"]].insert(item)

        yield [future_store, future_capped]
        self.write({"inserted": True, "id": str(item["_id"])})

handlers = [
    (r"/api", DataHandler),
]

settings = {
    "debug": True,
    "xsrf_cookies": False,
    "db": motor.MotorClient()[options.database],
    "data_coll": options["data-coll"],
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
