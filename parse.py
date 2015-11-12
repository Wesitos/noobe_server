from datetime import datetime

def parse_message(message):
    parts = message.split(",")
    version = int(parts.pop(0))

    return _version_dict[version](parts)

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
}

if __name__ == "__main__":
    import doctest
    doctest.testmod()
