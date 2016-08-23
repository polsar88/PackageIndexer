import logging, sys
from socketserver import BaseRequestHandler
from threading import Lock


class PackageIndexer(BaseRequestHandler):

    # This should be a power of 2 for best performance.
    TCP_SOCKET_BUFFER_BYTES = 4096

    # Request message formatting parameters.
    REQ_TOKEN_SEPARATOR = b'|'
    REQ_DEPS_SEPARATOR = b','

    # Request commands.
    CMD_INDEX = b'INDEX'
    CMD_REMOVE = b'REMOVE'
    CMD_QUERY = b'QUERY'

    # Responses.
    RES_OK = b'OK'
    RES_FAIL = b'FAIL'
    RES_ERROR = b'ERROR'

    NEWLINE = b'\n'

    # This is a hash map from each indexed package to its set of dependencies.
    PACKAGES = {}

    # This is a hash map from each indexed package to the packages which depend on it.
    DEPS = {}

    LOCK = Lock()


    def __init__(self, request, client_address, server):
        self.logger = logging.getLogger(type(self).__name__)
        logging.basicConfig(stream = sys.stdout, level = logging.INFO)

        super().__init__(request, client_address, server)


    def numIndexedPackages(self):
        return len(PackageIndexer.PACKAGES)


    def handle(self):
        # Persist connection across multiple requests.
        while True:
            try:
                self.receiveRequest()
            except ConnectionAbortedError:
                self.logger.info('Connection closed')
                return


    def receiveRequest(self):
        # Keep receiving data packets until newline terminates it.
        data = b''
        while True:
            packet = self.request.recv(PackageIndexer.TCP_SOCKET_BUFFER_BYTES)
            data += packet
            if len(packet) == 0 or packet[-1 : ] == PackageIndexer.NEWLINE:
                break

        if len(data) == 0:
            raise ConnectionAbortedError

        # Log client request.
        self.logger.info(
            'Request from %s:%d received: %s' % (self.client_address[0], self.client_address[1], repr(data)))

        # If the request message does not end with a newline, or there is a newline somewhere else than at the end,
        # it is invalid. Otherwise, we process the request.
        if data[-1 : ] != PackageIndexer.NEWLINE:
            responseMsg = PackageIndexer.RES_ERROR
        else:
            requestMsg = data.strip()  # Chop off the leading and trailing whitespace.
            if PackageIndexer.NEWLINE in requestMsg:
                responseMsg = PackageIndexer.RES_ERROR
            else:
                responseMsg = self.processRequest(requestMsg)

        # Send back the response.
        res = responseMsg + PackageIndexer.NEWLINE
        self.logger.info('Response: %s' % res)
        self.request.send(res)


    def processRequest(self, requestMsg):
        '''This method dispatches the passed request message to the appropriate handler based on the command.'''

        # Split the request message into tokens.
        tokens = requestMsg.split(PackageIndexer.REQ_TOKEN_SEPARATOR)
        if len(tokens) != 3:
            return PackageIndexer.RES_ERROR
        cmd, name, deps = tokens[0], tokens[1], self.parseDepsToken(tokens[2])

        # Verify package and dependency names.
        if not self.isPackageNameValid(name):
            return PackageIndexer.RES_ERROR  # Invalid package name.
        for dep in deps:
            if not self.isPackageNameValid(dep):
                return PackageIndexer.RES_ERROR  # Invalid dependency name.

        # Dispatch the command.
        if cmd == PackageIndexer.CMD_INDEX:
            return self.indexPackage(name, deps)
        elif cmd == PackageIndexer.CMD_REMOVE:
            if len(deps) > 0:
                return PackageIndexer.RES_ERROR  # Dependencies should not be specified for the REMOVE command.
            return self.removePackage(name)
        elif cmd == PackageIndexer.CMD_QUERY:
            if len(deps) > 0:
                return PackageIndexer.RES_ERROR  # Dependencies should not be specified for the QUERY command.
            return self.queryPackage(name)
        else:
            return PackageIndexer.RES_ERROR  # Invalid command.


    def parseDepsToken(self, depsToken):
        '''Returns a set containing all dependencies in the passed token.'''

        if len(depsToken) == 0:
            return set()  # No dependencies.
        return set(depsToken.split(PackageIndexer.REQ_DEPS_SEPARATOR))


    def isPackageNameValid(self, name):
        return len(name) > 0


    def indexPackage(self, name, deps):
        '''Handler method for the INDEX command.'''

        with PackageIndexer.LOCK:
            # Check that every package dependency is indexed and that the package does not depend on itself.
            for dep in deps:
                if dep not in PackageIndexer.PACKAGES or dep == name:
                    return PackageIndexer.RES_FAIL

            if name in PackageIndexer.PACKAGES:
                # Remove the package as a dependency for the packages on which it no longer depends.
                for dep in PackageIndexer.PACKAGES[name] - deps:
                    PackageIndexer.DEPS[dep].remove(name)  # Removing an element from a set is O(1) operation.

            # Index the package or update its set of dependecies.
            PackageIndexer.PACKAGES[name] = deps

            # Add the package as a dependency for the packages on which it depends.
            for dep in deps:
                if dep not in PackageIndexer.DEPS:
                    PackageIndexer.DEPS[dep] = set()
                PackageIndexer.DEPS[dep].add(name)  # Re-adding an existing element to a set has no effect.

            return PackageIndexer.RES_OK


    def removePackage(self, name):
        '''Handler method for the REMOVE command.'''

        with PackageIndexer.LOCK:
            if name not in PackageIndexer.PACKAGES:
                return PackageIndexer.RES_OK  # Package is not indexed.
            if name in PackageIndexer.DEPS and len(PackageIndexer.DEPS[name]) > 0:
                return PackageIndexer.RES_FAIL  # Other package(s) depend on this package.

            # Remove the package from the index.
            for dep in PackageIndexer.PACKAGES[name]:
                PackageIndexer.DEPS[dep].remove(name)  # Removing an element from a set is O(1) operation.
            del PackageIndexer.PACKAGES[name]

            return PackageIndexer.RES_OK


    def queryPackage(self, name):
        '''Handler method for the QUERY command.'''

        with PackageIndexer.LOCK:
            return PackageIndexer.RES_OK if name in PackageIndexer.PACKAGES else PackageIndexer.RES_FAIL
