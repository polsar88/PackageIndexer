#!/usr/bin/env python3


import logging
from socket import socket, AF_INET, SOCK_STREAM
from socketserver import BaseRequestHandler
import unittest
from unittest.mock import call, patch


from PackageIndexer import PackageIndexer


class PackageIndexerTest(unittest.TestCase):

    @patch.object(BaseRequestHandler, '__init__', return_value = None)
    def setUp(self, superInitMock):
        self.indexer = PackageIndexer(1, 2, 3)
        self.indexer.request = socket(AF_INET, SOCK_STREAM)  # Create a dummy socket.
        self.indexer.client_address = ('localhost', 9999)
        self.superInitMock = superInitMock
        logging.disable(logging.INFO)  # Turn off logging.


    def tearDown(self):
        self.indexer.request.close()


    def testInit(self):
        self.assertEqual(self.indexer.numIndexedPackages(), 0)
        self.superInitMock.assert_called_once_with(1, 2, 3)


    @patch.object(socket, 'recv')
    @patch.object(PackageIndexer, 'processRequest', return_value = b'response')
    @patch.object(socket, 'send')
    def testReceiveRequest(self, sendMock, processRequestMock, recvMock):
        # Empty message.
        recvMock.return_value = b''
        self.assertRaises(ConnectionAbortedError, self.indexer.receiveRequest)
        recvMock.assert_called_once_with(PackageIndexer.TCP_SOCKET_BUFFER_BYTES)
        self.assertFalse(processRequestMock.called)
        self.assertFalse(sendMock.called)

        # Malformed message comes in 1 packet.
        recvMock.reset_mock()
        sendMock.reset_mock()
        recvMock.side_effect = [b'bad\nMsg\n']
        self.indexer.receiveRequest()
        recvMock.assert_called_once_with(PackageIndexer.TCP_SOCKET_BUFFER_BYTES)
        self.assertFalse(processRequestMock.called)
        sendMock.assert_called_once_with(PackageIndexer.RES_ERROR + PackageIndexer.NEWLINE)

        # Malformed message comes in 2 packets.
        recvMock.reset_mock()
        sendMock.reset_mock()
        recvMock.side_effect = [b'bad\nMsg1', b'badMsg2\n']
        self.indexer.receiveRequest()
        recvMock.assert_has_calls([call(PackageIndexer.TCP_SOCKET_BUFFER_BYTES)] * 2)
        self.assertFalse(processRequestMock.called)
        sendMock.assert_called_once_with(PackageIndexer.RES_ERROR + PackageIndexer.NEWLINE)

        # Message comes in 1 packet.
        recvMock.reset_mock()
        sendMock.reset_mock()
        recvMock.side_effect = [b'msg\n']
        self.indexer.receiveRequest()
        recvMock.assert_called_once_with(PackageIndexer.TCP_SOCKET_BUFFER_BYTES)
        processRequestMock.assert_called_once_with(b'msg')
        sendMock.assert_called_once_with(b'response' + PackageIndexer.NEWLINE)

        # Message comes in 2 packets.
        recvMock.reset_mock()
        processRequestMock.reset_mock()
        sendMock.reset_mock()
        recvMock.side_effect = [b'msg1', b'msg2\n']
        self.indexer.receiveRequest()
        recvMock.assert_has_calls([call(PackageIndexer.TCP_SOCKET_BUFFER_BYTES)] * 2)
        processRequestMock.assert_called_once_with(b'msg1msg2')
        sendMock.assert_called_once_with(b'response' + PackageIndexer.NEWLINE)


    def testProcessRequest_Errors(self):
        self.assertEqual(self.indexer.processRequest(b''), PackageIndexer.RES_ERROR)
        self.assertEqual(self.indexer.processRequest(b' '), PackageIndexer.RES_ERROR)
        self.assertEqual(self.indexer.processRequest(b'|'), PackageIndexer.RES_ERROR)
        self.assertEqual(self.indexer.processRequest(b'||'), PackageIndexer.RES_ERROR)
        self.assertEqual(self.indexer.processRequest(b'|||'), PackageIndexer.RES_ERROR)

        # Package name not specified.
        self.assertEqual(self.indexer.processRequest(b'INDEX|||'), PackageIndexer.RES_ERROR)
        self.assertEqual(self.indexer.processRequest(b'REMOVE|||'), PackageIndexer.RES_ERROR)
        self.assertEqual(self.indexer.processRequest(b'QUERY|||'), PackageIndexer.RES_ERROR)

        # Invalid command.
        self.assertEqual(self.indexer.processRequest(b'index|pckg|dep'), PackageIndexer.RES_ERROR)
        self.assertEqual(self.indexer.processRequest(b'INDEXX|pckg|dep'), PackageIndexer.RES_ERROR)

        # Invalid package name.
        self.assertEqual(self.indexer.processRequest(b'INDEX||dep'), PackageIndexer.RES_ERROR)

        # Invalid dependencies.
        self.assertEqual(self.indexer.processRequest(b'INDEX|pckg|dep,'), PackageIndexer.RES_ERROR)
        self.assertEqual(self.indexer.processRequest(b'INDEX|pckg|dep1,,dep2'), PackageIndexer.RES_ERROR)

        # Dependencies specified for non-INDEX commands.
        self.assertEqual(self.indexer.processRequest(b'REMOVE|pckg|dep'), PackageIndexer.RES_ERROR)
        self.assertEqual(self.indexer.processRequest(b'QUERY|pckg|dep'), PackageIndexer.RES_ERROR)


    @patch.object(PackageIndexer, 'indexPackage', return_value = 'retVal')
    def testProcessRequest_Index(self, indexMock):
        self.assertEqual(self.indexer.processRequest(b'INDEX|pckg|'), 'retVal')
        indexMock.assert_called_once_with(b'pckg', set())

        indexMock.reset_mock()
        self.assertEqual(self.indexer.processRequest(b'INDEX|pckg|dep1'), 'retVal')
        indexMock.assert_called_once_with(b'pckg', {b'dep1'})

        indexMock.reset_mock()
        self.assertEqual(self.indexer.processRequest(b'INDEX|pckg|dep1,dep1'), 'retVal')
        indexMock.assert_called_once_with(b'pckg', {b'dep1'})

        indexMock.reset_mock()
        self.assertEqual(self.indexer.processRequest(b'INDEX|pckg|dep1,dep2'), 'retVal')
        indexMock.assert_called_once_with(b'pckg', {b'dep1', b'dep2'})

        indexMock.reset_mock()
        self.assertEqual(self.indexer.processRequest(b'INDEX|pckg|dep1,dep2,dep1'), 'retVal')
        indexMock.assert_called_once_with(b'pckg', {b'dep1', b'dep2'})


    @patch.object(PackageIndexer, 'removePackage', return_value = 'retVal')
    def testProcessRequest_Remove(self, removeMock):
        self.assertEqual(self.indexer.processRequest(b'REMOVE|pckg|'), 'retVal')
        removeMock.assert_called_once_with(b'pckg')


    @patch.object(PackageIndexer, 'queryPackage', return_value = 'retVal')
    def testProcessRequest_Query(self, queryMock):
        self.assertEqual(self.indexer.processRequest(b'QUERY|pckg|'), 'retVal')
        queryMock.assert_called_once_with(b'pckg')


    def testParseDepsToken(self):
        self.assertEqual(self.indexer.parseDepsToken(b''), set())
        self.assertEqual(self.indexer.parseDepsToken(b' '), {b' '})
        self.assertEqual(self.indexer.parseDepsToken(b','), {b''})
        self.assertEqual(self.indexer.parseDepsToken(b'a'), {b'a'})
        self.assertEqual(self.indexer.parseDepsToken(b'ab'), {b'ab'})
        self.assertEqual(self.indexer.parseDepsToken(b'a,'), {b'a', b''})
        self.assertEqual(self.indexer.parseDepsToken(b'a,a'), {b'a'})
        self.assertEqual(self.indexer.parseDepsToken(b'a,b'), {b'a', b'b'})
        self.assertEqual(self.indexer.parseDepsToken(b'a,,c'), {b'a', b'', b'c'})
        self.assertEqual(self.indexer.parseDepsToken(b'a,b,c'), {b'a', b'b', b'c'})
        self.assertEqual(self.indexer.parseDepsToken(b'c,c,c'), {b'c'})
        self.assertEqual(self.indexer.parseDepsToken(b'one,two,three'), {b'one', b'two', b'three'})
        self.assertEqual(self.indexer.parseDepsToken(b'one,one,one'), {b'one'})
        self.assertEqual(self.indexer.parseDepsToken(b'one,two,one'), {b'one', b'two'})
        self.assertEqual(self.indexer.parseDepsToken(b'one, two, t hree '), {b'one', b' two', b' t hree '})


    def testIsPackageNameValid(self):
        self.assertFalse(self.indexer.isPackageNameValid(''))

        self.assertTrue(self.indexer.isPackageNameValid('a'))
        self.assertTrue(self.indexer.isPackageNameValid('1'))
        self.assertTrue(self.indexer.isPackageNameValid('ab'))
        self.assertTrue(self.indexer.isPackageNameValid('12'))

        self.assertTrue(self.indexer.isPackageNameValid('packageName'))
        self.assertTrue(self.indexer.isPackageNameValid('package-name'))
        self.assertTrue(self.indexer.isPackageNameValid('package_name'))


    def testCommandSequence_TwoPackages(self):
        # No packages indexed.
        self.assertEqual(self.indexer.queryPackage(b'pckg'), PackageIndexer.RES_FAIL)
        self.assertEqual(self.indexer.removePackage(b'pckg'), PackageIndexer.RES_OK)

        # Indexing 1st package.
        self.assertEqual(self.indexer.indexPackage(b'pckg1', {b'pckg1'}), PackageIndexer.RES_FAIL)
        self.assertEqual(self.indexer.numIndexedPackages(), 0)
        self.assertEqual(self.indexer.indexPackage(b'pckg1', {b'pckg2'}), PackageIndexer.RES_FAIL)
        self.assertEqual(self.indexer.numIndexedPackages(), 0)
        self.assertEqual(self.indexer.indexPackage(b'pckg1', set()), PackageIndexer.RES_OK)
        self.assertEqual(self.indexer.numIndexedPackages(), 1)
        self.assertEqual(self.indexer.queryPackage(b'pckg1'), PackageIndexer.RES_OK)
        self.assertEqual(self.indexer.queryPackage(b'pckg2'), PackageIndexer.RES_FAIL)
        self.assertEqual(self.indexer.removePackage(b'pckg2'), PackageIndexer.RES_OK)

        # Indexing 2nd package.
        self.assertEqual(self.indexer.indexPackage(b'pckg2', {b'pckg3'}), PackageIndexer.RES_FAIL)
        self.assertEqual(self.indexer.indexPackage(b'pckg2', {b'pckg1', b'pckg3'}), PackageIndexer.RES_FAIL)
        self.assertEqual(self.indexer.indexPackage(b'pckg2', set()), PackageIndexer.RES_OK)
        self.assertEqual(self.indexer.numIndexedPackages(), 2)
        self.assertEqual(self.indexer.queryPackage(b'pckg1'), PackageIndexer.RES_OK)
        self.assertEqual(self.indexer.queryPackage(b'pckg2'), PackageIndexer.RES_OK)

        # Removing the 1st package.
        self.assertEqual(self.indexer.removePackage(b'pckg1'), PackageIndexer.RES_OK)
        self.assertEqual(self.indexer.numIndexedPackages(), 1)
        self.assertEqual(self.indexer.queryPackage(b'pckg1'), PackageIndexer.RES_FAIL)
        self.assertEqual(self.indexer.queryPackage(b'pckg2'), PackageIndexer.RES_OK)

        # Re-adding the 2nd package.
        self.assertEqual(self.indexer.indexPackage(b'pckg2', {b'pckg1'}), PackageIndexer.RES_FAIL)
        self.assertEqual(self.indexer.indexPackage(b'pckg2', set()), PackageIndexer.RES_OK)
        self.assertEqual(self.indexer.numIndexedPackages(), 1)
        self.assertEqual(self.indexer.queryPackage(b'pckg1'), PackageIndexer.RES_FAIL)
        self.assertEqual(self.indexer.queryPackage(b'pckg2'), PackageIndexer.RES_OK)

        # Adding the 1st package with 1 dependency.
        self.assertEqual(self.indexer.indexPackage(b'pckg1', {b'pckg2'}), PackageIndexer.RES_OK)
        self.assertEqual(self.indexer.numIndexedPackages(), 2)
        self.assertEqual(self.indexer.queryPackage(b'pckg1'), PackageIndexer.RES_OK)
        self.assertEqual(self.indexer.queryPackage(b'pckg2'), PackageIndexer.RES_OK)
        self.assertEqual(self.indexer.removePackage(b'pckg2'), PackageIndexer.RES_FAIL)  # 'pckg1' depends on 'pckg2'
        self.assertEqual(self.indexer.queryPackage(b'pckg1'), PackageIndexer.RES_OK)
        self.assertEqual(self.indexer.queryPackage(b'pckg2'), PackageIndexer.RES_OK)

        # Removing the 1st package.
        self.assertEqual(self.indexer.removePackage(b'pckg1'), PackageIndexer.RES_OK)
        self.assertEqual(self.indexer.numIndexedPackages(), 1)
        self.assertEqual(self.indexer.queryPackage(b'pckg1'), PackageIndexer.RES_FAIL)
        self.assertEqual(self.indexer.queryPackage(b'pckg2'), PackageIndexer.RES_OK)
        self.assertEqual(self.indexer.removePackage(b'pckg1'), PackageIndexer.RES_OK)
        self.assertEqual(self.indexer.numIndexedPackages(), 1)

        # Removing the 2nd package.
        self.assertEqual(self.indexer.removePackage(b'pckg2'), PackageIndexer.RES_OK)
        self.assertEqual(self.indexer.numIndexedPackages(), 0)
        self.assertEqual(self.indexer.queryPackage(b'pckg1'), PackageIndexer.RES_FAIL)
        self.assertEqual(self.indexer.queryPackage(b'pckg2'), PackageIndexer.RES_FAIL)
        self.assertEqual(self.indexer.removePackage(b'pckg1'), PackageIndexer.RES_OK)
        self.assertEqual(self.indexer.numIndexedPackages(), 0)


    def testCommandSequence_FourPackages(self):
        self.assertEqual(self.indexer.indexPackage(b'pckg1', set()), PackageIndexer.RES_OK)
        self.assertEqual(self.indexer.indexPackage(b'pckg2', {b'pckg1'}), PackageIndexer.RES_OK)
        self.assertEqual(self.indexer.indexPackage(b'pckg3', {b'pckg1', b'pckg2'}), PackageIndexer.RES_OK)
        self.assertEqual(self.indexer.indexPackage(b'pckg4', {b'pckg2', b'pckg3'}), PackageIndexer.RES_OK)

        self.assertEqual(self.indexer.numIndexedPackages(), 4)
        self.assertEqual(self.indexer.queryPackage(b'pckg1'), PackageIndexer.RES_OK)
        self.assertEqual(self.indexer.queryPackage(b'pckg2'), PackageIndexer.RES_OK)
        self.assertEqual(self.indexer.queryPackage(b'pckg3'), PackageIndexer.RES_OK)
        self.assertEqual(self.indexer.queryPackage(b'pckg4'), PackageIndexer.RES_OK)

        # Trying to remove a package on which another package depends should fail.
        self.assertEqual(self.indexer.removePackage(b'pckg1'), PackageIndexer.RES_FAIL)
        self.assertEqual(self.indexer.removePackage(b'pckg2'), PackageIndexer.RES_FAIL)
        self.assertEqual(self.indexer.removePackage(b'pckg3'), PackageIndexer.RES_FAIL)
        self.assertEqual(self.indexer.numIndexedPackages(), 4)

        # Updating package dependencies.
        self.assertEqual(self.indexer.indexPackage(b'pckg4', {b'pckg1', b'pckg2', b'pckg4'}), PackageIndexer.RES_FAIL)
        self.assertEqual(self.indexer.indexPackage(b'pckg4', {b'pckg1', b'pckg2'}), PackageIndexer.RES_OK)
        self.assertEqual(self.indexer.removePackage(b'pckg1'), PackageIndexer.RES_FAIL)
        self.assertEqual(self.indexer.removePackage(b'pckg2'), PackageIndexer.RES_FAIL)
        self.assertEqual(self.indexer.removePackage(b'pckg3'), PackageIndexer.RES_OK)  # No longer a dependency.
        self.assertEqual(self.indexer.queryPackage(b'pckg3'), PackageIndexer.RES_FAIL)
        self.assertEqual(self.indexer.numIndexedPackages(), 3)

        # Removing 4th package.
        self.assertEqual(self.indexer.removePackage(b'pckg4'), PackageIndexer.RES_OK)
        self.assertEqual(self.indexer.numIndexedPackages(), 2)
        self.assertEqual(self.indexer.queryPackage(b'pckg4'), PackageIndexer.RES_FAIL)

        # Removing remaining packages.
        self.assertEqual(self.indexer.removePackage(b'pckg1'), PackageIndexer.RES_FAIL)
        self.assertEqual(self.indexer.removePackage(b'pckg2'), PackageIndexer.RES_OK)
        self.assertEqual(self.indexer.numIndexedPackages(), 1)
        self.assertEqual(self.indexer.queryPackage(b'pckg2'), PackageIndexer.RES_FAIL)

        self.assertEqual(self.indexer.removePackage(b'pckg1'), PackageIndexer.RES_OK)
        self.assertEqual(self.indexer.numIndexedPackages(), 0)
        self.assertEqual(self.indexer.queryPackage(b'pckg1'), PackageIndexer.RES_FAIL)


if __name__ == '__main__':
    unittest.main()
