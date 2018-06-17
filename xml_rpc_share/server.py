from xmlrpc.client import ServerProxy, Fault
from xmlrpc.server import SimpleXMLRPCServer
from urllib.parse import urlparse
from os.path import join, isfile, abspath
import sys

MAX_HISTORY_LENGTH = 6
SimpleXMLRPCServer.allow_reuse_address = 1
UNHANDLED = 100
ACCESS_DENIED = 200

"""
define exception class
"""


class UnhandledQuery(Fault):
    def __init__(self, message="Couldn't handle the query"):
        super().__init__(UNHANDLED, message)


class AccessDenied(Fault):
    def __init__(self, message="Access denied"):
        super().__init__(ACCESS_DENIED, message)


def inside(dir, name):
    """
    check if exist the file in directory
    """
    dir = abspath(dir)
    name = abspath(name)
    return name.startswith(join(dir, ''))


def get_port(url):
    'get port from url'
    name = urlparse(url)[1]
    parts = name.split(':')
    return int(parts[-1])


class Node:
    def __init__(self, url, dirname, secret):
        self.url = url
        self.dirname = dirname
        self.secret = secret
        self.known = set()

    def query(self, query, history=[]):
        try:
            return self._handle(query)
        except UnhandledQuery:
            history = history + [self.url]
            if len(history) >= MAX_HISTORY_LENGTH:
                raise
            return self._broadcast(query, history)

    def _handle(self, query):
        dir = self.dirname
        name = join(dir, query)
        if not isfile(name): raise UnhandledQuery
        if not inside(dir, name): raise AccessDenied
        return open(name).read()

    def _broadcast(self, query, history):
        for other in self.known.copy():
            if other in history: continue
            try:
                s = ServerProxy(other)
                return s.query(query, history)
            except Fault as f:
                if f.faultCode == UNHANDLED:
                    pass
                else:
                    self.known.remove(other)
            except:
                self.known.remove(other)
        raise UnhandledQuery

    def hello(self, other):
        self.known.add(other)
        return 0

    def fetch(self, query, secret):
        if secret != self.secret: raise AccessDenied
        result = self.query(query)
        with open(join(self.dirname, query), 'w') as f:
            f.write(result)
        return 0

    def _start(self):
        s = SimpleXMLRPCServer(("", get_port(self.url)), logRequests=False)
        s.register_instance(self)
        s.serve_forever()


def main():
    url, directory, secret = sys.argv[1:]
    n = Node(url, directory, secret)
    n._start()


if __name__ == '__main__': main()
