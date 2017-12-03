import logging
import zmq


class LockerServer(object):
    """ Server that manages locks across a network """

    PING = 'PING'
    PONG = 'PONG'
    DONE = 'DONE'
    LOCK = 'LOCK'
    RELEASE_ERROR = 'RELEASE_ERROR'
    MSG_ERROR = 'MSG_ERROR'
    UNLOCK = 'UNLOCK'
    UNLOCKED = 'UNLOCKED'
    GO = 'GO'
    WAIT = 'WAIT'
    DELIMITER = ':'
    DEFAULT_LOCK = '_DEFAULT_'
    CLOSE = 'CLOSE'

    def __init__(self, url="tcp://192.168.1.14:789"):
        self._locks = {}
        self._url = url
        self._logger = None

    def _lock(self, name, id_):
        if name not in self._locks:
            # Lock is available and unlocked.
            # Client locks it (aka addition to dict) and
            # can go on into a sequential code block.
            self._locks[name] = id_
            return self.GO
        else:
            # Lock is not available and locked by someone else.
            # Client needs to wait until it is released
            return self.WAIT

    def _unlock(self, name, id_):
        locker_id = self._locks[name]
        if locker_id != id_:
            # Locks can only be locked and 
            # then unlocked by the same client.
            response = (self.RELEASE_ERROR + self.DELIMITER +
                        'Lock was acquired by `%s` and not by '
                        '`%s`.' % (locker_id, id_))
            self._logger.error(response)
            return response
        else:
            # Unlocks the lock (aka removal from dict).
            del self._locks[name]
            return self.UNLOCKED
    def run(self):
        """Runs Server"""
        try:
            self._logger = logging.getLogger('LockServer')
            self._logger.info('Starting Lock Server')
            context = zmq.Context()
            socket_ = context.socket(zmq.REP)
            socket_.bind(self._url)
            while True:

                msg = socket_.recv_string()
                name = None
                id_ = None
                if self.DELIMITER in msg:
                    msg, name, id_ = msg.split(self.DELIMITER)
                if msg == self.DONE:
                    socket_.send_string(self.CLOSE + self.DELIMITER +
                                        'Closing Lock Server')
                    self._logger.info('Closing Lock Server')
                    break
                elif msg == self.LOCK:
                    if name is None or id_ is None:
                        response = (self.MSG_ERROR + self.DELIMITER +
                                    'Please provide name and id for locking')
                        self._logger.error(response)
                    else:
                        response = self._lock(name, id_)
                    socket_.send_string(response)
                elif msg == self.UNLOCK:
                    if name is None or id_ is None:
                        response = (self.MSG_ERROR + self.DELIMITER +
                                    'Please provide name and id for unlocking')
                        self._logger.error(response)
                    else:
                        response = self._unlock(name, id_)
                    socket_.send_string(response)
                elif msg == self.PING:
                    socket_.send_string(self.PONG)
                else:
                    response = (self.MSG_ERROR + self.DELIMITER +
                               'MSG `%s` not understood' % msg)
                    self._logger.error(response)
                    socket_.send_string(response)
        except Exception:
            self._logger.exception('Crashed Lock Server!')
            raise


def run_server():
    logging.basicConfig(level=logging.DEBUG)
    server = LockerServer()
    server.run()


if __name__ == '__main__':
    run_server()
