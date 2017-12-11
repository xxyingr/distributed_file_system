import socket
import threading
import Queue
import re
import sys
import os
import hashlib
import random
import sqlite3 as db


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
        # Create a queue of tasks
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

class DirectoryServer(Server):
    GET_REGEX = "GET_SERVER: \nFILENAME: [a-zA-Z0-9_./]*\n\n"
    GET_RESPONSE = "PRIMARY_SERVER: %s\nPORT: %s\nFILENAME: %s%s\n\n"
    GET_SLAVES_REGEX = "GET_SLAVES: .*\nPORT: [0-9]*\n\n"
    SLAVE_RESPONSE_HEADER = "SLAVES: %s\n\n"
    SLAVE_HEADER = "\nSLAVE_SERVER: %s\nPORT: %s"
    CREATE_DIR_REGEX = "CREATE_DIR: \nDIRECTORY: [a-zA-Z0-9_./]*\n\n"
    DELETE_DIR_REGEX = "DELETE_DIR: \nDIRECTORY: [a-zA-Z0-9_./]*\n\n"
    DATABASE = "Database/directories.db"

    def __init__(self, port_use=None):
        TCPServer.__init__(self, port_use, self.handler)
        self.create_tables()

    def handler(self, message, con, addr):
        if re.match(self.GET_REGEX, message):
            self.get_server(con, addr, message)
        elif re.match(self.GET_SLAVES_REGEX, message):
            self.get_slaves(con, addr, message)
        else:
            return False
        return True

    def get_server(self, con, addr, text):
        # Handler for file upload requests
        request = text.splitlines()
        full_path = request[1].split()[1]

        path, file = os.path.split(full_path)
        name, ext = os.path.splitext(file)
        filename = hashlib.sha256(full_path).hexdigest() + ext
        host, port = self.find_host(path)

        if not host:
            # The Directory doesn't exist and must be added to the db
            server_id = self.pick_random_host()
            self.create_dir(path, server_id)
            host, port = self.find_host(path)

        # Get the list of slaves that have a copy of the file
        slave_string = self.get_slave_string(host, port)
        return_string = self.GET_RESPONSE % (host, port, filename, slave_string)
        print return_string
        con.sendall(return_string)
        return

    def get_slaves(self, con, addr, text):
        # Function that gets the list of slave servers
        request = text.splitlines()
        host = request[0].split()[1]
        port = request[1].split()[1]
        slave_string = self.get_slave_string(host, port)
        return_string = self.SLAVE_RESPONSE_HEADER % slave_string
        con.sendall(return_string)
        return

    def find_host(self, path):
        # Function that takes a path and returns the server that contains that directories files
        return_host = (False, False)
        con = db.connect(self.DATABASE)
        with con:
            cur = con.cursor()
            cur.execute("SELECT Server FROM Directories WHERE Path = ?", (path,))
            server = cur.fetchone()
            if server:
                server_id = server[0]
                cur = con.cursor()
                cur.execute("SELECT Server, Port FROM Servers WHERE Id = ?", (server_id,))
                return_host = cur.fetchone()
        return return_host

    def pick_random_host(self):
        # Function to pick a random host from the database
        return_host = False
        con = db.connect(self.DATABASE)
        with con:
            cur = con.cursor()
            cur.execute("SELECT Id FROM Servers")
            servers = cur.fetchall()
            if servers:
                return_host = random.choice(servers)[0]
        return return_host

    def get_slave_string(self, host, port):
        # Function that generates a slave string
        return_string = ""
        con = db.connect(self.DATABASE)
        with con:
            cur = con.cursor()
            cur.execute("SELECT Server, Port FROM Servers WHERE NOT (Server=? AND Port=?)", (host, port,))
            servers = cur.fetchall()
        for (host, port) in servers:
            header = self.SLAVE_HEADER % (host, port)
            return_string = return_string + header
        return return_string

    def create_dir(self, path, host):
        # Function to create a directory in the DB
        con = db.connect(self.DATABASE)
        with con:
            cur = con.cursor()
            cur.execute("INSERT INTO Directories (Path, Server) VALUES (?, ?)", (path, host,))
        con.commit()
        con.close()

    def add_server(self, host, port):
        # Function to add a server to the DB
        con = db.connect(self.DATABASE)
        with con:
            cur = con.cursor()
            cur.execute("INSERT INTO Servers (Server, Port) VALUES (?, ?)", (host, port,))
        con.commit()
        con.close()

    def remove_dir(self, path):
        # Function to remove a directory from the DB
        con = db.connect(self.DATABASE)
        with con:
            cur = con.cursor()
            cur.execute("DELETE FROM Directories WHERE Path = ?", (path,))
        con.commit()
        con.close()

    def remove_server(self, server):
        # Function to remove a server from the DB
        con = db.connect(self.DATABASE)
        with con:
            cur = con.cursor()
            cur.execute("DELETE FROM Servers WHERE Server = ?", (server,))
        con.commit()
        con.close()

    def create_tables(self):
        # Function to add the tables to the database
        con = db.connect(self.DATABASE)
        with con:
            cur = con.cursor()
            cur.execute("CREATE TABLE IF NOT EXISTS Servers(Id INTEGER PRIMARY KEY, Server TEXT, Port TEXT)")
            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS SERVS ON Servers(Server, Port)")
            cur.execute("CREATE TABLE IF NOT EXISTS Directories(Id INTEGER PRIMARY KEY, Path TEXT, Server INTEGER, FOREIGN KEY(Server) REFERENCES Servers(Id))")
            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS DIRS ON Directories(Path)")



def main():
    try:
        if len(sys.argv) > 1 and sys.argv[1].isdigit():
            port = int(sys.argv[1])
            server = DirectoryServer(port)
        else:
            server = DirectoryServer()
        server.listen()
    except socket.error, msg:
        print "Unable to create socket connection: " + str(msg)
        con = None


if __name__ == "__main__": main()
