"""-"""

import os

SETTINGS = {
    'logging': {
        'level': 'DEBUG'
    },
    'service': {
        'port': os.getenv('PORT'),
        'name': 'PREP-NEXGDDP'
    },
    'redis': {
        'url': os.getenv('REDIS_URL')
    }
}
