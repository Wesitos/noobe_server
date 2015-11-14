from datetime import datetime
from convesion_utils import time2secs_tz
from convesion_utils import coord_decimales

def parse_message(message):
    # A veces recibimos cadenas binarias
    if isinstance(message, bytes):
        message = message.decode("ascii")

    parts = message.split(",")
    version = int(parts.pop(0))

    return _version_dict[version](parts)


def _parse_v2(msg_list):
    """Parsea un mensaje version 2
    >>> d = _parse_v1(['1','20151114113954','1206.578273','7701.822705','79.30','22.20','999'])
    fechahora =            'yyyyMMddHHmmss'
    coordenadas = 'ddmm.mmmmm' # d: degrees, m: minutes (se debe convertir a grados decimales)
    """

    return {
        "id_nodo": int(msg_list[0]),
        "timestamp": time2secs_tz(msg_list[1]),
        "lat": coord_decimales(msg_list[2]),
        "lon": coord_decimales(msg_list[3]),
        "data": {
            "temp": float(msg_list[4]),
            "hum": float(msg_list[5]),
            "gas": int(msg_list[6])
        }
    }


def _parse_v1(msg_list):
    """Parsea un mensaje version 1

    >>> d = _parse_v1(['1', '1447204817', '-1201835', '-7704910', '223', '78', '148'])
    >>> d == {
    ...       "id_nodo": 1,
    ...       "timestamp": 1447204817,
    ...       "lat": -12.01835,
    ...       "lon": -77.04910,
    ...       "data":{
    ...          "temp": 22.3,
    ...          "hum": 78,
    ...          "gas": 148,
    ...       }
    ...     }
    True
    """
    return {
        "id_nodo": int(msg_list[0]),
        "timestamp": int(msg_list[1]),
        "lat": int(msg_list[2])/1E5,
        "lon": int(msg_list[3])/1E5,
        "data":{
            "temp": int(msg_list[4])/1E1,
            "hum": int(msg_list[5]),
            "gas": int(msg_list[6]),
        }
    }





_version_dict = {
    1: _parse_v1,
    2: _parse_v2
}

if __name__ == "__main__":
    import doctest
    doctest.testmod()
