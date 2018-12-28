from http.server import BaseHTTPRequestHandler, HTTPServer

import insteon.io.serial as serial

import binascii
import threading
import base64

def make_handler(srv):
    class HubHandler(BaseHTTPRequestHandler):
        def do_HEAD(self):
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()

        def do_UNAUTHORIZED(self):
            self.send_response(401)
            self.send_header('WWW-Authenticate', 'Basic realm=\"Test\"')
            self.send_header('Content-type', 'text/html')
            self.end_headers()

        def do_GET(self):
            if not 'Authorization' in self.headers:
                self.do_UNAUTHORIZED()
                self.send_text('No authentication received')
                return
            expected_auth = 'Basic ' + \
                base64.b64encode((srv._username + ':' + srv._password).encode('utf-8')).decode('utf-8')
            if self.headers['Authorization'] != expected_auth:
                self.do_UNAUTHORIZED()
                self.send_text('Bad authorization')
                print('Bad authorization: {}'.format(self.headers['Authorization']))
                return

            # Parse the path to either
            if self.path == '/buffstatus.xml':
                print('Getting buffer status')
                self.handle_buffer_request()
            elif self.path == '/1?XB=M=1':
                print('Clearing buffer')
                self.handle_buffer_clear()
            elif self.path.startswith('/3?') and self.path.endswith('=I=3'):
                data = self.path[3:len(self.path) - 4]
                print('Writing: {}'.format(data))
                self.handle_write_request(data)
            else:
                print('Unhandled request: {}'.format(self.path))
                self.send_response(404)
                self.send_header('Content-type', 'text/html')
                self.end_headers()
                self.send_text('Not found!')

        def log_message(self, format, *args):
            pass

        def handle_buffer_request(self):
            self.send_response(200)
            self.send_header('Content-type', 'text/xml')
            self.end_headers()
            data = srv.buffer_status
            self.send_text('<response><BS>' + data + '</BS></response>')

        def handle_buffer_clear(self):
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            srv.clear()
            self.send_text('buffer cleared!')

        def handle_write_request(self, hex_data):
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            try:
                data = binascii.unhexlify(hex_data)
                self.send_text('writing: ' + data.hex())
                srv.write(data)
            except:
                print('Error writing: {}'.format(hex_data))
                self.send_text('error!')
                return

        def send_text(self, text):
            self.wfile.write(bytes(text, 'UTF-8'))
    return HubHandler

class HubServer:
    def __init__(self, io_conn, port, username, password, bufferlen):
        self._io_conn = io_conn
        self._port = port
        self._handler = make_handler(self)
        self._username = username
        self._password = password

        self._buffer_lock = threading.Lock()
        self._buffer = bytearray(bufferlen)
        self._buffer_pos = 0

    @property
    def buffer(self):
        with self._buffer_lock:
            return bytes(self._buffer)

    @property
    def buffer_status(self):
        with self._buffer_lock:
            return (self._buffer.hex() + '{:02x}'.format(2*self._buffer_pos)).upper()


    def write(self, data):
        with self._buffer_lock:
            self._io_conn.write(data)

    def read(self, data):
        with self._buffer_lock:
            for x in data:
                self._buffer[self._buffer_pos] = x
                self._buffer_pos = (self._buffer_pos + 1) % len(self._buffer)

    def clear(self):
        with self._buffer_lock:
            self._buffer = bytearray(len(self._buffer))
            self._buffer_pos = 0

    def run(self):
        # start the read thread
        reader = threading.Thread(target=self._read_thread)
        reader.start()

        self._run_http_server()

        self._io_conn.close()
        reader.join()

    def _run_http_server(self):
        httpd = HTTPServer(("", self._port), self._handler)
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print('Received interrupt, shutting down server')
        httpd.server_close()

    def _read_thread(self):
        while self._io_conn.is_open:
            buf = self._io_conn.read(1)
            if len(buf) > 0:
                self.read(buf)

def run():
    port = 25105 
    username = 'hub'
    password = 'hubpass'
    print('Server started on port {}'.format(port))
    srv = HubServer(serial.SerialConn('/dev/ttyUSB0'), port, username, password, 40)
    srv.run()

if __name__ == '__main__':
    run()
