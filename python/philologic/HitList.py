#!/usr/bin/env python
import re
import os
import sys
import time
import codecs
import struct

class HitList(object):
    def __init__(self,filename,words,doc=0,byte=6,method="proxy",methodarg = 3):
        self.filename = filename
        self.words = words
        self.method = method
        self.methodarg = methodarg
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
            ret = []
            for index in range(*(n.indices(self.count))):
                ret.append(self.readhit(index))
            return ret
        else:
            return self.readhit(n)
        # We should check if search is finished, or if the item just doesn't exist.
        
    def __len__(self):
        self.update()
        return self.count
        
    def __iter__(self):
        self.update()
        self.position = 0
        return self
        
    def next(self):
        if self.position < self.count:
            oldposition = self.position
            self.position += 1
            return self.readhit(oldposition)
        else:
            raise StopIteration
            
    def update(self):
        #Since the file could be growing, we should frequently check size/ if it's finished yet.
        self.size = os.stat(self.filename).st_size # in bytes
        self.count = self.size / self.hitsize 
        #need to have a reliable way to check if finished.  Ask Mark.
        try: 
            os.stat(self.filename + ".done")
            self.done = True
        except OSError:
            pass
            
    def readhit(self,n):
        #reads hitlist into buffer, unpacks
        #should do some work to read k at once, track buffer state.
        self.update()
        if n >= self.count:
            raise IndexError
        offset = self.hitsize * n;
        self.fh.seek(offset)
        buffer = self.fh.read(self.hitsize)
        return(struct.unpack(self.format,buffer))
    
    def get_doc(self,hit):
        return hit[0]
        
    def get_bytes(self,hit):
        if self.method is not "cooc":
            rest = hit[7:]
            bytes = []
            while rest:
                bytes.append(rest[1])
                rest = rest[2:]
        else:
            bytes = hit[(int(self.methodarg)):]
        bytes.reverse()
        return bytes
        
    def get_words(self,hit):
        pass