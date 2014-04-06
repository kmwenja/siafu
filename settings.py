__author__ = 'caninemwenja'

import logging
import logging.config

DEBUG = True

DATABASE = 'sqlite:///siafu.db'

LOGGING = {
    'version': 1.0,
    'disable_existing_loggers': True,
    'formatters': {
        'simple': {
            'format': '%(asctime)s %(levelname)s '
                      '%(message)s'
        },
        'verbose': {
            'format': '%(asctime)s %(levelname)s siafu-server '
                      '%(name)s %(module)s.py %(message)s'
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'DEBUG' if DEBUG else 'INFO',
            'formatter': 'simple'
        },
        'sys-log': {
            'class': 'logging.handlers.SysLogHandler',
            'address': '/dev/log',
            'formatter': 'verbose'
        }
    },
    'loggers': {
        '': {
            'handlers': ['console', 'sys-log'],
            'level': 'DEBUG' if DEBUG else 'INFO',
            'propagate': True,
        }
    }
}


def configure_logging():
    logging.config.dictConfig(LOGGING)
    logging.getLogger(__name__).info("Logging configured")