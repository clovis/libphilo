#!/usr/bin/python
import re
import os
import sys
import codecs


class OHCOVector:
    """OHCOVector manages all the index arithmetic necessary to construct a PhiloLogic index.
    
    OHCOVector is constructed with two arguments: a list of types, 
    and a supplementary list of (parallel_type, dependes_on_type).
    A type list typically takes the form ('doc','div1','div2','div3','para','sent','word'),
    whereas a typical set of parallel types are ( ('byte','doc'),('page','doc'),('line','doc') ).
    The subsequence 'div1','div2','div3' indicates a nested type, which can be addressed simply as
    'div'; in addition, the 'div' type can handle overflow by tesselating deeper structures.
    This isn't ideal, and may cause you to lose metadata on trailing segments--if it happens to you,
    increase the depth of the type.    
    """
    def __init__(self,inner_types,*depends): 
        self.v = []
        self.inner_types = inner_types 
        self.outer_types = [] 
        self.maxdepths = {} #the width of each hier type.  anything beyond is nested. #OUTERMAXDEPTH
        self.currentdepths = {} # for hier types--from 1 up to max #OUTERCURRENTDEPTH
        self.nesteddepths = {} # for hier types--only greater than 0 when current==max OUTERNESTEDDEPTH
        self.types = {} #maps literal levels onto hierarchical types. # TYPEMAP?
        self.depends = depends # needs to map parallel fields onto inner types, not outer.
        self.parallel = [] 
        self.current_objects = []
        self.current_metadata = []
        
        for i,lev in enumerate(inner_types):
            self.v.append(0)
            m = re.match(r"(.+)(\d+)$",lev)
            if m:
                type = m.group(1)
                n = m.group(2)
                self.types[lev] = type
                if type not in self.outer_types:
                    self.outer_types.append(type)
                self.maxdepths[type] = int(n)
                self.currentdepths[type] = 1
                self.nesteddepths[type] = 0
            elif re.match(r"(.+)$",lev):
                type = lev
                self.types[lev] = type
                self.maxdepths[lev] = 1 
                self.currentdepths[type] = 1
                self.nesteddepths[type] = 0
                self.outer_types.append(type)
            else:
                sys.stderr.write("bad vector definition\n")
                
    def push(self,otype):
        """handles the start of an object.  nested types are special."""
        depth = 0
        #if we have a verbatim type listed in self.inner_types,
        #we can calculate it's position directly.
        if otype in self.inner_types:
            depth = self.inner_types.index(otype)
            type = self.types[otype]
            order = self.outer_types.index(type) # oo bad.
            current = depth + 1
            for htype in self.outer_types[:order]:
                for k in range(self.maxdepths[htype]):
                    current -= 1
            self.currentdepths[type] = current
            self.v[depth] += 1
            for j in range(depth + 1,len(self.inner_types)):
                self.v[j]=0
            #should check if we need to reset a parallel type
            return 0
        #if we have a hierarchical type listed in self.hier,
        #we have to walk through the hierarchy, and check the stack,
        #to find the correct position
        elif otype in self.outer_types:
            d = self.outer_types.index(otype)
            for htype in self.outer_types[:d]:
                for i in range (self.maxdepths[htype]):
                    depth += 1
            for j in range(1,self.currentdepths[otype]):
                depth += 1
            if self.currentdepths[otype] < self.maxdepths[otype]:
                self.currentdepths[otype] += 1
            else:
                self.nesteddepths[otype] += 1
            self.v[depth] += 1
            for j in range(depth + 1,len(self.inner_types)):
                self.v[j]=0
            #should check if we need to reset a parallel type
            return 0
        # should check for parallel type push here
        elif otype in [p[0] for p in self.parallel]:
            # need to figure out where in v the parallel object lives, then increment.
            pass 

    def pull(self,otype):
        #can't push to parallel type.
        depth = 0
        if otype in self.inner_types:
            depth = self.inner_types.index(otype)
            r = [i if n <= depth else 0 for n,i in enumerate(self.v)]
        elif otype in self.outer_types:
            d = self.outer_types.index(otype)
            for htype in self.outer_types[:d]:
                for i in range (self.maxdepths[htype]):
                    depth += 1
            for j in range(self.currentdepths[otype]):
                depth += 1
            if self.nesteddepths[otype] > 0:
                self.nesteddepths[otype] -= 1
            else:
                self.currentdepths[otype] -= 1
            r = [i if n <= depth else 0 for n,i in enumerate(self.v)]
            self.v[depth] += 1
            self.v = [j if n <= depth else 0 for n,j in enumerate(self.v)]
        return r

    def currentdepth(self):
        d = len(self.v)
        while d >= 0:
            d -= 1
            if self.v[d] != 0:
                return d

    def typedepth(self,type):
        if type in self.inner_types: #levels should become inner_types
            return self.inner_types.index(type)
        elif type in self.outer_types: #hier should become outer_types
            d = 0
            for t in self.outer_types[:self.outer_types.index(type)]:
                d += self.maxdepths[t]
            d += self.currentdepths[type]
            return d
    
    def __str__(self):
        r = []
        for l, n in zip(self.inner_types,self.v):
            r.append("%s:%d" % (l,n))
        return "(%s)" % ", ".join(r)
