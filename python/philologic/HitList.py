#!/usr/bin/env python
import re
import os
import sys
import time
import codecs
import struct
from HitWrapper import HitWrapper, ObjectWrapper

class HitList(object):
    def __init__(self,filename,words,dbh,encoding=None,doc=0,byte=6,method="proxy",methodarg = 3):
        self.filename = filename
        self.words = words
        self.method = method
        self.methodarg = methodarg
        self.dbh = dbh
        self.encoding = encoding
        if method is not "cooc":
            self.has_word_id = 1
            self.length = 7 + 2 * (words)
        else:
            self.has_word_id = 0 #unfortunately.  fix this next time I have 3 months to spare.
            self.length = methodarg + 1 + (words)
        self.fh = open(self.filename) #need a full path here.
        self.format = "=%dI" % self.length #short for object id's, int for byte offset.
        self.hitsize = struct.calcsize(self.format)
        self.doc = doc
        self.byte = byte
        self.position = 0;
        self.done = False
        #self.hitsize = 4 * (6 + self.words) # roughly.  packed 32-bit ints, 4 bytes each.
        self.update()
        
    def __getitem__(self,n):
        self.update()
        if isinstance(n,slice):
            return self.get_slice(n)
        else:            
            return HitWrapper(self.readhit(n),self.dbh)

    def get_slice(self,n):
        self.position = n.start or 0
        while True:
            if self.position is not None and self.position >= n.stop:
                break
            if self.position < len(self):
                try:
                    yield HitWrapper(self.readhit(self.position),self.dbh)
                    self.position += 1
                except IndexError:
                    break
            else:
                self.update()
                if self.done:
                    break
                else:
                    time.sleep(0.1)
                    self.update()
        
    def __len__(self):
        self.update()
        return self.count
        
    def __iter__(self):
        self.update()
        self.position = 0
        while True:
            if self.position < len(self):
                try:
                    yield HitWrapper(self.readhit(self.position),self.dbh)
                    self.position += 1
                except IndexError:
                    break
            else:
                self.update()
                if self.done: 
                    break
                else:
                    time.sleep(0.1)
                    self.update()

    def update(self):
        #Since the file could be growing, we should frequently check size/ if it's finished yet.
        if self.done:
            pass
        else:
            try: 
                os.stat(self.filename + ".done")
                self.done = True
            except OSError:
                pass
            self.size = os.stat(self.filename).st_size # in bytes
            self.count = self.size / self.hitsize 


    def readhit(self,n):
        #reads hitlist into buffer, unpacks
        #should do some work to read k at once, track buffer state.
        self.update()
        if n >= self.count:
            raise IndexError
        if n != self.position:
            offset = self.hitsize * n;
            self.fh.seek(offset)
        buffer = self.fh.read(self.hitsize)
        return(struct.unpack(self.format,buffer))
