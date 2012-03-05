#!/usr/bin/env python

def index_metadata_fields(loader_obj):
    conn = sqlite3.connect(loader_obj.destination + '/toms.db')
    c = conn.cursor()
    for field in metadata_fields:
        query = 'create index %s_index on toms (%s)' % (field, field)
        try:
            c.execute(query)
        except sqlite3.OperationalError:
            pass
    conn.close()
