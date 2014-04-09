__author__ = 'caninemwenja'

from twisted.protocols.basic import LineReceiver
from twisted.internet import reactor
from twisted.internet.protocol import ClientFactory

import sys


def quit():
    reactor.stop()
    print "Bye!"


class Console(object):

    def __init__(self):
        self.history = []

    def write(self, content):
        print content

    def read(self):
        user_input = raw_input("=> ")
        self.history.append(user_input)

        return user_input


class SiafuClientProtocol(LineReceiver):

    def __init__(self, addr, console):
        self.addr = addr
        self.console = console

    def loop(self):
        try:
            user_input = self.console.read()

            if user_input == '':
                self.loop()
                return

            if user_input == 'quit':
                self.transport.loseConnection()
                return

            self.sendLine(user_input)
        except EOFError:
            self.transport.loseConnection()

    def connectionMade(self):
        # self.console.write("Connected to server")
        pass

    def connectionLost(self, reason):
        if reason.getErrorMessage() != 'Connection was closed cleanly.':
            self.console.write("Connection Lost: {0}".format(reason.getErrorMessage()))

    def lineReceived(self, line):
        self.console.write(line)
        self.loop()


class SiafuClientFactory(ClientFactory):

    def __init__(self):
        self.console = Console()
        self.console.write("==> Welcome to Siafu <==")
        self.console.write("")
        self.console.write("to exit type 'quit'")
        self.console.write("")
        self.console.write("")

    def buildProtocol(self, addr):
        return SiafuClientProtocol(addr, self.console)

    def clientConnectionFailed(self, connector, reason):
        self.console.write("Could not connect to server: {0}".format(reason.getErrorMessage()))
        quit()

    def clientConnectionLost(self, connector, reason):
        if reason.getErrorMessage() != 'Connection was closed cleanly.':
            self.console.write("Connection was lost: {0}".format(reason.getErrorMessage()))
        quit()

if len(sys.argv) < 3:
    print "Usage: {0} server port".format(sys.argv[0])
    exit()

server = sys.argv[1]
port = int(sys.argv[2])

reactor.connectTCP(server, port, SiafuClientFactory())
reactor.run()
