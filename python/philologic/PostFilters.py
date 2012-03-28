#!/usr/bin/env python

from __future__ import division

import sqlite3
from math import log

def index_metadata_fields(loader_obj):
    conn = sqlite3.connect(loader_obj.destination + '/toms.db')
    c = conn.cursor()
    for field in loader_obj.metadata_fields:
        query = 'create index %s_index on toms (%s)' % (field, field)
        try:
            c.execute(query)
        except sqlite3.OperationalError:
            pass
    conn.commit()
    conn.close()