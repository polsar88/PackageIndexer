#!/usr/bin/env python3


import socketserver

from PackageIndexer import PackageIndexer


SERVER_ADDRESS = 'localhost'
SERVER_PORT = 8080


def main():
    server = socketserver.ThreadingTCPServer(
        server_address = (SERVER_ADDRESS, SERVER_PORT), RequestHandlerClass = PackageIndexer)
    server.serve_forever()

if __name__ == '__main__':
    main()
