import datetime
import dateutil.parser

td_utc = datetime.timedelta(hours=0)
tz_utc = datetime.timezone(td_utc)
td_lima = datetime.timedelta(hours=-5)
tz_lima = datetime.timezone(td_lima)

d70 = datetime.datetime(1970, 1, 1, 0, 0, 0).replace(tzinfo=tz_utc)


def hora_lima(strtime):
    fechautc = dateutil.parser.parse(strtime)
    fechautc = fechautc.replace(tzinfo=tz_utc)
    print(fechautc.strftime("%Y-%m-%d %H:%M:%S %Z%z"))
    fecha_lima = fechautc.astimezone(tz_lima)
    print(fecha_lima.strftime("%Y-%m-%d %H:%M:%S %Z%z"))
    return fecha_lima


def time2secs(fechahora, fechahoraref):
    diff = fechahora - fechahoraref
    timestamp = diff.days*24*60*60 + diff.seconds
    return timestamp


def time2secs_tz(strtime):
    fecha_lima = hora_lima(strtime)
    d = time2secs(fecha_lima, d70)
    print(d)
    print(datetime.datetime.fromtimestamp(d))


#time2secs_tz("20151114113954")


def coord_decimales(strcoords):
    l = float(strcoords)
    grados = (int)(l/100)
    minutos = (l - grados * 100)/60
    return -(grados + minutos)
