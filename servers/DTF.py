import socket
import threading
import thread
import Queue
import os
import re
import sys
import base64

class Server(object):
    PORT = 8000
    HOST = '0.0.0.0'
    LENGTH = 4096
    MAX_THREAD = 10
    HELO_RESPONSE = "HELO %s\nIP:%s\nPort:%s\nStudentID:11347076"
    DEFAULT_RESPONSE = "ERROR: INVALID MESSAGE\n\n"
    HELO_REGEX = "HELO .*"

    def __init__(self, port_use=None, handler=None):
        if not port_use:
            port_use = self.PORT
        else:
            self.PORT = port_use
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind((self.HOST, port_use))
        self.handler = handler if handler else self.default_handler
        # Create a queue of tasks with ma
        self.threadQueue = Queue.Queue(maxsize=self.MAX_THREAD)

        # Create thread pool
        for i in range(self.MAX_THREAD):
            thread = ThreadHandler(self.threadQueue, self.LENGTH, self)
            thread.setDaemon(True)
            thread.start()

    def default_handler(self, message, con, addr):
        return False

    def listen(self):
        self.sock.listen(5)

        # Listen for connections and delegate to threads
        while True:
            con, addr = self.sock.accept()

            # If queue full close connection, otherwise send to thread
            if not self.threadQueue.full():
                self.threadQueue.put((con, addr))
            else:
                print "Queue full closing connection from %s:%s" % (addr[0], addr[1])
                con.close()

    def kill_serv(self, con):
        # Kill server
        os._exit(1)
        return

    def helo(self, con, addr, text):
        # Reply to helo request
        reply = text.rstrip()  # Remove newline
        reply = reply.split()[1]
        return_string = self.HELO_RESPONSE % (reply, addr[0], addr[1])
        con.sendall(return_string)
        return

    def default(self, con, addr, text):
        return_string = self.DEFAULT_RESPONSE
        con.sendall(return_string)
        # Default handler for everything else
        print "Default"
        return

    def send_request(self, data, server, port):
        return_data = ""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        sock.connect((server, port))
        sock.sendall(data)

        # Loop until all data received
        while "\n\n" not in return_data:
            data = sock.recv(self.LENGTH)
            if len(data) == 0:
                break
            return_data += data

        # Close and dereference the socket
        sock.close()
        sock = None
        return return_data

class ThreadHandler(threading.Thread):
    def __init__(self, thread_queue, buffer_length, server):
        threading.Thread.__init__(self)
        self.queue = thread_queue
        self.buffer_length = buffer_length
        self.server = server
        self.messageHandler = server.handler

    def run(self):
        # Thread loops and waits for connections to be added to the queue
        while True:
            request = self.queue.get()
            self.handler(request)
            self.queue.task_done()

    def handler(self, (con, addr)):
        message = ""
        # Loop and receive data
        while "\n\n" not in message:
            data = con.recv(self.buffer_length)
            message += data
            if len(data) < self.buffer_length:
                break
        # If valid http request with message body
        if len(message) > 0:
            if message == "KILL_SERVICE\n":
                print "Killing service"
                self.server.kill_serv(con)
            elif re.match(self.server.HELO_REGEX, message):
                self.server.helo(con, addr, message)
            elif self.messageHandler(message, con, addr):
                None
            else:
                print message
                self.server.default(con, addr, message)
        return


def main():
    try:
        if len(sys.argv) > 1 and sys.argv[1].isdigit():
            port = int(sys.argv[1])
            server = Server(port)
        else:
            server = Server()
        server.listen()
    except socket.error, msg:
        print "Unable to create socket connection: " + str(msg)


if __name__ == "__main__": main()

class FileServer(Server):
    UPLOAD_REGEX = "UPLOAD: [a-zA-Z0-9_.]*\nDATA: .*\n\n"
    UPDATE_REGEX = "UPDATE: [a-zA-Z0-9_.]*\nDATA: .*\n\n"
    UPDATE_HEADER = "UPDATE: %s\nDATA: %s\n\n"
    DOWNLOAD_REGEX = "DOWNLOAD: [a-zA-Z0-9_.]*\n\n"
    DOWNLOAD_RESPONSE = "DATA: %s\n\n"
    GET_SLAVES_HEADER = "GET_SLAVES: %s\nPORT: %s\n\n"
    UPLOAD_RESPONSE = "OK: 0\n\n"
    SERVER_ROOT = os.getcwd()
    BUCKET_NAME = "DirectoryServerFiles"
    BUCKET_LOCATION = os.path.join(SERVER_ROOT, BUCKET_NAME)
    DIR_HOST = "0.0.0.0"
    DIR_PORT = 8005

    def __init__(self, port_use=None):
        Server.__init__(self, port_use, self.handler)
        self.BUCKET_LOCATION = os.path.join(self.BUCKET_LOCATION, str(self.PORT))

    def handler(self, message, con, addr):
        if re.match(self.UPLOAD_REGEX, message):
            self.upload(con, addr, message)
        elif re.match(self.DOWNLOAD_REGEX, message):
            self.download(con, addr, message)
        elif re.match(self.UPDATE_REGEX, message):
            self.update(con, addr, message)
        else:
            return False

        return True

    def upload(self, con, addr, text):
        # Handler for file upload requests
        filename, data = self.execute_write(text)
        return_string = self.UPLOAD_RESPONSE
        con.sendall(return_string)
        self.update_slaves(filename, data)
        return

    def download(self, con, addr, text):
        # Handler for file download requests
        request = text.splitlines()
        filename = request[0].split()[1]

        path = os.path.join(self.BUCKET_LOCATION, filename)
        file_handle = open(path, "w+")
        data = file_handle.read()
        return_string = self.DOWNLOAD_RESPONSE % (base64.b64encode(data))
        con.sendall(return_string)
        return

    def update(self, con, addr, text):
        # Handler for file update requests
        self.execute_write(text)
        return_string = self.UPLOAD_RESPONSE
        con.sendall(return_string)
        return

    def execute_write(self, text):
        # Function that process an update/upload request and writes data to the server
        request = text.splitlines()
        filename = request[0].split()[1]
        data = request[1].split()[1]
        data = base64.b64decode(data)

        path = os.path.join(self.BUCKET_LOCATION, filename)
        file_handle = open(path, "w+")
        file_handle.write(data)
        return filename, data

    def update_slaves(self, filename, data):
        # Function that gets all the slaves and updates file on them
        slaves = self.get_slaves()
        update = self.UPDATE_HEADER % (filename, base64.b64encode(data))
        for (host, port) in slaves:
            self.send_request(update, host, int(port))
        return

    def get_slaves(self):
        # Function to get the list of slave file servers
        return_list = []
        request_data = self.GET_SLAVES_HEADER % (self.HOST, self.PORT,)
        lines = self.send_request(request_data, self.DIR_HOST, self.DIR_PORT).splitlines()
        slaves = lines[1:-1]
        for i in range(0, len(slaves), 2):
            host = slaves[i].split()[1]
            port = slaves[i + 1].split()[1]
            return_list.append((host, port))
        return return_list


def main():
    try:
        if len(sys.argv) > 1 and sys.argv[1].isdigit():
            port = int(sys.argv[1])
            server = FileServer(port)
        else:
            server = FileServer()
        server.listen()
    except socket.error, msg:
        print "Unable to create socket connection: " + str(msg)
        con = None


if __name__ == "__main__": main()