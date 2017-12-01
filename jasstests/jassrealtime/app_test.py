# Run this instead of the usual app.py for integration testing

from jassrealtime.webapi.app import *
from jasstests.jassrealtime.webapi.handlers.performance_handlers import LongRunningGetHandler,\
    ShortRunningGetHandler

if __name__ == "__main__":
    handlers.append((r"/test/longrunningrequest/(.*)", LongRunningGetHandler))
    handlers.append((r"/test/shortrunningrequest/(.*)", ShortRunningGetHandler))
    server =  HTTPServer(make_app())
    server.bind(8889)
    server.start(get_nb_cores())
    tornado.ioloop.IOLoop.current().start()