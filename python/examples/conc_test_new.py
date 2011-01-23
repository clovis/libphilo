#!/usr/bin/env python 
import sys
import re
import time
import philologic.PhiloDB
import philologic.shlaxtree as st

def transform(node):
    if node.tag != "context" and node.attrib.get("class","") != "hilite" : 
        node.tag = None
    if node.text: node.text = re.sub(r"\s+", " ",node.text).decode("utf-8","ignore") # ignore leading/trailing malformed chars.
    if node.tail: node.tail = re.sub(r"\s+", " ",node.tail).decode("utf-8","ignore")
    for child in node:
        transform(child)

if __name__ == "__main__":

    db_path = sys.argv[1]
    db = philologic.PhiloDB.PhiloDB(db_path,7)
    
    print >> sys.stderr, "enter a full-text query.  like 'the stage|player'."
    line = sys.stdin.readline()
    print "<hitlist>"
    q = db.query(line.strip(),limit=10000)
    
    while not q.done:
        time.sleep(.05)
        q.update() # have to check if the query is completed yet.                                                                                                                         
    
    for hit in q:
        doc_id = q.get_doc(hit)
        offsets = q.get_bytes(hit)
        first_offset = offsets[0]
        filename = db.toms[doc_id]["filename"]
        author = db.toms[doc_id]["author"]
        title = db.toms[doc_id]["title"]
        print "<hit>"
        print "<cite>%s : %s, %s : %s@%d</cite>" % (hit,author,title,filename,first_offset)
    
        conc_start = first_offset - 200
        if conc_start < 0: conc_start = 0
        text_path = db_path + "/TEXT/" + filename
        text_file = open(text_path)
        text_file.seek(conc_start)
        text = text_file.read(400)
        p_start = conc_start - len("<context>")
        parser = st.TokenizingParser(p_start,offsets)
        parser.feed("<context>" + text + "</context>")
        tree = parser.close()
        transform(tree)
        print st.ElementTree.tostring(tree,encoding='utf-8')
        print "</hit>"
    
    print >> sys.stderr, "%d hits" % len(q)
    print "</hitlist>"
