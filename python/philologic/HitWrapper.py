#!/usr/bin/python

import time
import sys
from itertools import islice

obj_dict = {'doc': 1, 'div1': 2, 'div2': 3, 'div3': 4,
            'para': 5, 'sent': 6, 'word': 7}

class HitWrapper(object):

    def __init__(self, hit, db, obj_type=False, encoding=None):
        self.db = db
        self.hit = hit
        self.philo_id = hit
        self.encoding = encoding
        if obj_type:
            self.type = obj_type
        else:
            try:
                length = len(hit[:hit.index(0)])
            except ValueError:
                length = len(hit)
            self.type = [k for k in obj_dict if obj_dict[k] == length][0]

    def __getitem__(self, key):
        if key in obj_dict:
            return ObjectWrapper(self.hit, self.db, obj_type=key,encoding=self.encoding)
        else:
            return self.__metadata_lookup(key)
    
    def __getattr__(self, name):
        if name in obj_dict:
            return ObjectWrapper(self.hit, self.db, obj_type=name,encoding=self.encoding)
        else:
            return self.__metadata_lookup(name)
        
    def __metadata_lookup(self, field):
        width = 7
        philo_id = self.philo_id[:width]
        metadata = None
        while width:
            try:
                metadata = self.db.get_id_lowlevel(philo_id[:width])[field]
            except (TypeError,IndexError):
                ## if self.db[philo_id[:width]] returns None
                width -= 1
                continue
            if metadata != None:
                break
            width -= 1
        if metadata == None:
            metadata = ''
        if self.encoding:
            try:
                return metadata.decode(self.encoding, 'ignore')
            except AttributeError:
                ## if the metadata is an integer
                return metadata
        else:
            return metadata
 
           
class ObjectWrapper(object):
    
    def __init__(self, hit, db, obj_type=False, encoding=None):
        self.db = db
        self.hit = hit
        if obj_type:
            self.philo_id = hit[:obj_dict[obj_type]]
            self.type = obj_type
        else:
            self.philo_id = hit
            try:
                length = len(hit[:hit.index(0)])
            except ValueError:
                length = len(hit)
            self.type = [k for k in obj_dict if obj_dict[k] == length][0]
        self.bytes = bytes
        self.encoding = encoding
        

    def __getitem__(self, key):
        if key in obj_dict:
            return ObjectWrapper(self.hit, self.db, key,encoding=self.encoding)
        else:
            return self.__metadata_lookup(key)
        
    def __getattr__(self, name):
        if name in obj_dict:
            return ObjectWrapper(self.hit, self.db, name,encoding=self.encoding)
        else:
            return self.__metadata_lookup(name)
        
    def __metadata_lookup(self, field):
        metadata = None
        try:
            metadata = self.db.get_id_lowlevel(self.philo_id)[field]
        except (TypeError,IndexError):
            ## if self.db[self.philo_id[:width]] returns None]
            metadata = ''
        if metadata == None:
            metadata = ''
        if self.encoding:
            try:
                return metadata.decode(self.encoding, 'ignore')
            except AttributeError:
                ## if the metadata is an integer
                return metadata
        else:
            return metadata
