__author__ = 'caninemwenja'

from twisted.protocols.basic import LineReceiver
from twisted.internet.protocol import Factory
from twisted.internet import reactor

from siafu import Siafu, SiafuError, SiafuSyntaxError

import logging

import settings

settings.configure_logging()


class SiafuProtocol(LineReceiver):

    def __init__(self, addr):
        self.addr = addr
        self.siafu = Siafu(settings.DATABASE)

    def connectionMade(self):
        logging.info("new connection: {0}".format(self.addr))
        self.sendLine("Connected, Send it")

    def connectionLost(self, reason):
        logging.info("connection lost: {0}".format(self.addr))

    def lineReceived(self, line):
        logging.info("Received line: {0} from {1}".format(
            line, self.addr))
        try:
            result = self.siafu.process_sql(line)
            self.sendLine(result)
        except SiafuSyntaxError, sse:
            logging.exception(sse)
            self.sendLine("Error: {0}".format(sse.__unicode__()))
        except SiafuError, se:
            logging.exception(se)
            self.sendLine("Error: {0}".format(se.__unicode__()))
        except Exception, e:
            logging.exception(e)
            self.sendLine("Error: {0}".format(e.__unicode__()))


class SiafuFactory(Factory):

    def buildProtocol(self, addr):
        return SiafuProtocol(addr)

try:
    logging.info("turning on")
    reactor.listenTCP(7890, SiafuFactory())
    logging.info("listening")
    reactor.run()
except KeyboardInterrupt:
    logging.info("shut down")
