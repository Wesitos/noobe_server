from tornado.tcpserver import TCPServer
from tornado import gen
from parse import parse_message

class TCPSocketHandler(TCPServer):
    def __init__(self, insert_func, io_loop=None, ssl_options=None, max_buffer_size=None,
                 read_chunk_size=None):
        super().__init__(io_loop=None, ssl_options=None, max_buffer_size=None,
                         read_chunk_size=None)
        self.insert_data = insert_func

    @gen.coroutine
    def handle_stream(self, stream, address):
        print("Conexion recibida desde", address)
        while not stream.closed():
            yield self.handle_message(stream)

    @gen.coroutine
    def handle_message(self, stream):
        message = yield stream.read_until("\n".encode("ascii"))
        print("Mensaje recibido:", message)
        message_dict = parse_message(message)
        dict_id = yield self.insert_data(message_dict)
        response = "0\n" if dict_id else "1\n"
        yield stream.write(response.encode("ascii"))
