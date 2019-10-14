"""-"""

import os

SETTINGS = {
    'logging': {
        'level': 'DEBUG'
    },
    'service': {
        'port': os.getenv('PORT')
    },
    'redis': {
        'url': os.getenv('REDIS_URL')
    }
}
