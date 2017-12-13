from contextlib import closing
from httplib import HTTPConnection
from tempfile import SpooledTemporaryFile
import json
import os.path

from contextlib import closing
from httplib import HTTPConnection

class memoize:

    def __init__(self, fn):
        """fn: the function to decorate."""

        self.fn = fn
        self.cache = {}

    def __call__(self, *args, **kwds):
        """Check if we already have the answer and return it, otherwise
           compute it and store the result."""

        key = tuple(args) + tuple(kwds)

        if key in self.cache:
            return self.cache[key]

        ans = self.fn(*args, **kwds)
        return self.cache.setdefault(key, ans)

    def renew(self, *args, **kwds):
        """Delete the previous return value of the function for arguments
           *args & **kwds and recompute the result.
        """

        key = tuple(args) + tuple(kwds)

        if key in self.cache:
            del self.cache[key]

        return self(*args, **kwds)


def load_config(config, filepath):
    """Load the config file filename (JSON) if it exists and updates
       config, otherwise do nothing.
    """

    if not os.path.exists(filepath):
        return

    with open(filepath) as f:
        c = json.loads(f.read())
        config.update(c)


def get_host_port(s):

    host, port = s.split(':')
    return host, int(port)

def get_server(filepath, host, port):
    """Return a server owning filepath.
       host & port: the address & port of a name server.
    """

    with closing(HTTPConnection(host, port)) as con:
        con.request('GET', filepath)
        response = con.getresponse()
        status, srv = response.status, response.read()

    if status == 200:
        return srv

    return None

class Error(IOError):

    pass

class File(SpooledTemporaryFile):
    """Is a distant file, it's stored in memory if it size if less than
       the max_size parameter, otherwise it's stored on the disk.
    """

    def __init__(self, filepath, mode='rtc'):
        """filepath: the path of the distant file"""

        self.mode = mode
        self.filepath = filepath
        host, port = get_host_port()
        self.srv = get_server(filepath, host, port)

        if self.srv is None:
            raise Error('Impossible to find a server that serve %s.'
                    % filepath)

        self.last_modified = None
        SpooledTemporaryFile.__init__(self, _config['max_size'], mode.replace('c', ''))

        if 'a' in mode or 'w' in mode:
            # automatically gets a lock if we're in write/append mode
            host, port = utils.get_host_port(_config['lockserver'])
            self.lock_id = int(utils.get_lock(filepath, host, port))


        if 'c' in mode:
            File._cache[filepath] = self

    def __exit__(self, exc, value, tb):
        """Send the change to the DFS, and close the file."""

        self.close()

        if 'c' not in self.mode:
            return SpooledTemporaryFile.__exit__(self, exc, value, tb)

        return False

    def close(self):
        """Send the change to the DFS, and close the file."""

        self.flush()

        if 'c' not in self.mode:
            SpooledTemporaryFile.close(self)

    def flush(self):
        """Flush the data to the server."""

        SpooledTemporaryFile.flush(self)
        self.commit()

    def commit(self):
        if 'a' in self.mode or 'w' in self.mode:
            # send the file from the begining
            self.seek(0)
            data = self.read()
            host, port = utils.get_host_port(self.srv)
            with closing(HTTPConnection(host, port)) as con:
                con.request('PUT', self.filepath + '?lock_id=%s' % self.lock_id,
                            data)

                response = con.getresponse()
                self.last_modified = response.getheader('Last-Modified')
                status = response.status

    def from_cache(filepath):
        """Try to retrieve a file from the cache
           filepath: the path of the file to retrieve from cache.
           Return None if the file isn't in the cache or if the cache expired.
        """

        if filepath in File._cache:
            f = File._cache[filepath]

            host, port = get_host_port()
            fs = get_server(filepath, host, port)
            host, port = get_host_port(fs)

            with closing(HTTPConnection(host, port)) as con:
                con.request('HEAD', filepath)

                if (f.last_modified ==
                        con.getresponse().getheader('Last-Modified')):
                    f.seek(0)
                    return f
                else:
                    del File._cache[filepath]

        return None


def rename(filepath, newfilepath):
    """Rename filepath to newfilepath."""

    with open(filepath) as f:
        with open(newfilepath, 'w') as nf:
            nf.write(f.read())

        unlink(filepath, f.lock_id)


open = File
File._cache = {}
_config = {
    'filedb': None,
    'max_size': 1024**2
}
File._cache = {}