import time
import os
import multiprocessing as mp
import socket
import zmq
import random
from locking_server import LockerServer


class LockerClient(object):
    """ Implements a Lock using req-rep scheme with LockerServer """


    def __init__(self, url="tcp://127.0.0.1:7899", 
                 lock_name=LockerServer.DEFAULT_LOCK):
        self.lock_name = lock_name
        self.url = url
        self._context = None
        self._socket = None
        self.id = None

    def __getstate__(self):
        result_dict = self.__dict__.copy()
        # Do not pickle zmq data
        result_dict['_context'] = None
        result_dict['_socket'] = None
        return result_dict

    def start(self):
        """Starts connection to server.

        Makes ping-pong test as well

        """
        self._context = zmq.Context()
        self._socket = self._context.socket(zmq.REQ)
        self._socket.connect(self.url)
        self.test_ping()
        # create a unique id based on host name and process id
        self.id = (socket.getfqdn().replace(LockerServer.DELIMITER, '-') + 
                   '__' + str(os.getpid()))

    def send_done(self):
        """Notifies the Server to shutdown"""
        if self._socket is None:
            self.start()
        self._socket.send_string(LockerServer.DONE)
        self._socket.recv_string()  # Final receiving of closing

    def test_ping(self):
        """Connection test"""
        self._socket.send_string(LockerServer.PING)
        response = self._socket.recv_string()
        if response != LockerServer.PONG:
            raise RuntimeError('Connection Error to Lock Server')

    def finalize(self):
        """Closes socket"""
        if self._socket is not None:
            self._socket.close()
            self._socket = None
            self._context = None

    def acquire(self):
        """Acquires lock and returns `True`

        Blocks until lock is available.

        """
        if self._context is None:
            self.start()
        request = (LockerServer.LOCK + LockerServer.DELIMITER +
                   self.lock_name + LockerServer.DELIMITER + self.id)
        while True:
            self._socket.send_string(request)
            response = self._socket.recv_string()
            if response == LockerServer.GO:
                return True
            elif response == LockerServer.WAIT:
                time.sleep(self.SLEEP)
            else:
                raise RuntimeError('Response `%s` not understood' % response)

    def release(self):
        """Releases lock"""
        request = (LockerServer.UNLOCK + LockerServer.DELIMITER +
                   self.lock_name + LockerServer.DELIMITER + self.id)
        self._socket.send_string(request)
        response = self._socket.recv_string()
        if response != LockerServer.UNLOCKED:
            raise RuntimeError('Could not release lock `%s` (`%s`) '
                               'because of `%s`!' % (self.lock_name, 
                                                     self.id, response))


def the_job(idx):
    """Simple job executed in parallel

    Just sleeps randomly and prints to console.
    Capital letters signal parallel printing

    """
    lock = LockerClient()

    random.seed(idx)
    sleep_time = random.uniform(0.0, 0.07)  # Random sleep time

    lock.acquire()

    print('This')
    time.sleep(sleep_time)
    print('is')
    time.sleep(sleep_time*1.5)
    print('a')
    time.sleep(sleep_time*3.0)
    print('sequential')
    time.sleep(sleep_time)
    print('block')
    time.sleep(sleep_time)
    print('by %d' % idx)

    lock.release()

    print('_HAPPENING')
    time.sleep(sleep_time)
    print('_IN')
    time.sleep(sleep_time/2.0)
    print('_PARALLEL')
    time.sleep(sleep_time*1.5)
    print('_BY %d' % idx)


def run_pool():
    # Create a pool and run the job in parallel
    pool = mp.Pool(8)
    pool.map(the_job, range(16))
    pool.close()
    pool.join()

    # Tell server to shutdown
    lock = LockerClient()
    lock.send_done()


if __name__ == '__main__':
    run_pool()
