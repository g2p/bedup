from __future__ import absolute_import
import datetime

ZERO = datetime.timedelta(0)

class Utc(datetime.tzinfo):
    def utcoffset(self, dt):
        return ZERO

    def tzname(self, dt):
        return 'UTC'

    def dst(self, dt):
        return ZERO

UTC = Utc()

def system_now():
    # datetime.utcnow is broken
    return datetime.datetime.now(tz=UTC)

