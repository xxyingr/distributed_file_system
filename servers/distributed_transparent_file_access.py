#-*- coding: utf-8 -*-

import logging
import os.path
import time

from contextlib import closing
from httplib import HTTPConnection

import web

def get_local_path(filepath):
    """Convert the filepath url to an absolute path in the FS."""

    return os.path.join(os.getcwd(), _config['fsroot'], filepath[1:])


def raise_if_dir_or_not_servable(filepath):
    """Raise a 406 notacceptable if the filepath isn't supposed to be
       served, or if it's a directory.
    """

    p = get_local_path(filepath)

    if (os.path.dirname(filepath) not in _config['directories'] or
            os.path.isdir(p)):
        raise web.notacceptable()


def raise_if_not_exists(filepath):
    """Raise a 204 No Content if the file doesn't exists."""

    p = get_local_path(filepath)

    if not os.path.exists(p):
        raise web.webapi.HTTPError('204 No Content',
                                   {'Content-Type': 'plain/text'})

class FileServer:

    def GET(self, filepath):
    
        web.header('Content-Type', 'text/plain; charset=UTF-8')

        raise_if_dir_or_not_servable(filepath)
        raise_if_not_exists(filepath)

        p = get_local_path(filepath)
        web.header('Last-Modified', time.ctime(os.path.getmtime(p)))
        with open(p) as f:
            return f.read()

    def PUT(self, filepath):
        """Replace the file by the data in the request."""

        raise_if_dir_or_not_servable(filepath)

        p = get_local_path(filepath)

        with open(p, 'w') as f:
            f.write(web.data())

        web.header('Last-Modified', time.ctime(os.path.getmtime(p)))

        return ''

    def DELETE(self, filepath):
        web.header('Content-Type', 'text/plain; charset=UTF-8')

        raise_if_dir_or_not_servable(filepath)
        raise_if_not_exists(filepath)

        os.unlink(get_local_path(filepath))
        return 'OK'

    def HEAD(self, filepath):
        """If the file exists, return the last-modified http
           header which corresponds to the last time was modified."""

        web.header('Content-Type', 'text/plain; charset=UTF-8')

        raise_if_dir_or_not_servable(filepath)
        raise_if_not_exists(filepath)

        p = get_local_path(filepath)
        web.header('Last-Modified', time.ctime(os.path.getmtime(p)))
        return ''


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
    """Return a tuple ('host', port) from the string s.
       e.g.: 'localhost:80' → ('localhost', 80)
    """

    host, port = s.split(':')
    return host, int(port)

def get_server(filepath, host, port):

    with closing(HTTPConnection(host, port)) as con:
        con.request('GET', filepath)
        response = con.getresponse()
        status, srv = response.status, response.read()

    if status == 200:
        return srv

    return None

_config = {
        'directoryserver': None,
        'directories': [],
        'fsroot': 'fs/',
        'srv': None,
        }

logging.info('Loading config file fileserver.dfs.json.')
load_config(_config, 'fileserver.dfs.json')

_config['directories'] = set(_config['directories'])
