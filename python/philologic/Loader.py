#!/usr/bin/env python
import re
import os
import sys
import codecs
import math
import cPickle
from philologic import OHCOVector,SqlToms,Parser

sort_by_word = "-k 2,2"
sort_by_id = "-k 3,3n -k 4,4n -k 5,5n -k 6,6n -k 7,7n -k 8,8n -k 9,9n"

blocksize = 2048 # index block size.  Don't alter.
index_cutoff = 10 # index frequency cutoff.  Don't. alter.

class Loader(object):

    def __init__(self,workers=4,verbose=True):
        self.max_workers = workers 
        self.omax = [0,0,0,0,0,0,0,0,0]
        self.totalcounts = {}
        self.verbose = verbose
        
    def setup_dir(self,path,files):
        self.destination = path
        self.files = files
        os.mkdir(self.destination)
        self.workdir = self.destination + "/WORK/"
        self.textdir = self.destination + "/TEXT/"
        os.mkdir(self.workdir)
        os.mkdir(self.textdir)

        self.fileinfo =    [{"orig":os.path.abspath(x),
                             "name":os.path.basename(x),
                             "id":n + 1,
                             "newpath":self.textdir + os.path.basename(x),
                             "raw":self.workdir + os.path.basename(x) + ".raw",
                             "words":self.workdir + os.path.basename(x) + ".words.sorted",
                             "toms":self.workdir + os.path.basename(x) + ".toms",
                             "sortedtoms":self.workdir + os.path.basename(x) + ".toms.sorted",
                             "pages":self.workdir + os.path.basename(x) + ".pages",
                             "count":self.workdir + os.path.basename(x) + ".count",
                             "results":self.workdir + os.path.basename(x) + ".results"} for n,x in enumerate(self.files)]

        for t in self.fileinfo:
            os.system("cp %s %s" % (t["orig"],t["newpath"]))
        
        os.chdir(self.workdir) #questionable
        
    def parse_files(self,xpaths=None,metadata_xpaths=None):
        filequeue = self.fileinfo[:]
        print "parsing %d files." % len(filequeue)
        procs = {}
        workers = 0
        done = 0
        total = len(filequeue)
        
        while done < total:
            while filequeue and workers < self.max_workers:
    
                # we want to have up to max_workers processes going at once.
                text = filequeue.pop(0) # parent and child will both know the relevant filenames
                pid = os.fork() # fork returns 0 to the child, the id of the child to the parent.  
                # so pid is true in parent, false in child.
    
                if pid: #the parent process tracks the child 
                    procs[pid] = text["results"] # we need to know where to grab the results from.
                    workers += 1
                    # loops to create up to max_workers children at any one time.
    
                if not pid: # the child process parses then exits.
    
                    i = codecs.open(text["newpath"],"r",)
                    o = codecs.open(text["raw"], "w",) # only print out raw utf-8, so we don't need a codec layer now.
                    print "parsing %d : %s" % (text["id"],text["name"])
                    parser = Parser.Parser({"filename":text["name"]},text["id"],xpaths=xpaths,metadata_xpaths=metadata_xpaths,output=o)
                    r = parser.parse(i)  
                    i.close()
                    o.close()

                    # some unix pipeline filters.
                    wordcommand = "cat %s | egrep \"^word\" | sort %s %s > %s" % (text["raw"],sort_by_word,sort_by_id,text["words"])
                    os.system(wordcommand)            
                    countcommand = "cat %s | wc -l > %s" % (text["words"],text["count"])
                    os.system(countcommand)            
                    tomscommand = "cat %s | egrep \"^doc|^div|^para\" | sort %s > %s" % (text["raw"],sort_by_id,text["sortedtoms"])
                    os.system(tomscommand)
                    pagescommand = "cat %s | egrep \"^page\" > %s" % (text["raw"],text["pages"])
                    os.system(pagescommand)
                    
                    #post filter word processing.
                    max_id = None
                    for line in open(text["words"]):
                        (key,type,id,attr) = line.split("\t")
                        id = [int(i) for i in id.split(" ")]
                        if not max_id:
                            max_id = id
                        else:
                            max_id = [max(new,prev) for new,prev in zip(id,max_id)]
                            
#                    print max_id
                    rf = open(text["results"],"w")
                    cPickle.dump(max_id,rf) # write the result out--really just the resulting omax vector, which the parent will merge in below.
                    rf.close()

                            
                    exit()
    
            #if we are at max_workers children, or we're out of texts, the parent waits for any child to exit.
            pid,status = os.waitpid(0,0) # this hangs until any one child finishes.  should check status for problems.
            done += 1 
            workers -= 1
            vec = cPickle.load(open(procs[pid])) #load in the results from the child's parsework() function.
            #print vec
            self.omax = [max(x,y) for x,y in zip(vec,self.omax)]
            
    def merge_objects(self):
        wordfilearg = " ".join(file["words"] for file in self.fileinfo)
        words_result = self.workdir + "all.words.sorted"
        tomsfilearg = " ".join(file["sortedtoms"] for file in self.fileinfo)
        toms_result = self.workdir + "all.toms.sorted"
        pagesfilearg = " ".join(file["pages"] for file in self.fileinfo)
        pages_result = self.workdir + "all.pages"
        os.system("sort -m %s %s %s > %s" % (wordfilearg,sort_by_word,sort_by_id, words_result) )
        os.system("sort -m %s %s > %s" % (tomsfilearg,sort_by_id, toms_result) )
        os.system("cat %s > %s" % (pagesfilearg,pages_result) )
        
    def analyze(self):
        print self.omax
        vl = [max(int(math.ceil(math.log(float(x),2.0))),1) if x > 0 else 1 for x in self.omax]        
        print vl
        width = sum(x for x in vl)
        print str(width) + " bits wide."
        
        hits_per_block = (blocksize * 8) // width 
        freq1 = index_cutoff
        freq2 = 0
        offset = 0
        
        # unix one-liner for a frequency table
        os.system("cut -f 2 %s | uniq -c | sort -rn -k 1,1> %s" % ( self.workdir + "/all.words.sorted", self.workdir + "/all.frequencies") )
        
        # now scan over the frequency table to figure out how wide (in bits) the frequency fields are, and how large the block file will be.
        for line in open(self.workdir + "/all.frequencies"):    
            f, word = line.rsplit(" ",1) # uniq -c pads output on the left side, so we split on the right.
            f = int(f)    
            if f > freq2:
                freq2 = f
            if f < index_cutoff:
                pass # low-frequency words don't go into the block-mode index.
            else:
                blocks = 1 + f // (hits_per_block + 1) #high frequency words have at least one block.
                offset += blocks * blocksize
        
        # take the log base 2 for the length of the binary representation.
        freq1_l = math.ceil(math.log(float(freq1),2.0))
        freq2_l = math.ceil(math.log(float(freq2),2.0))
        offset_l = math.ceil(math.log(float(offset),2.0))
        
        print "freq1: %d; %d bits" % (freq1,freq1_l)
        print "freq2: %d; %d bits" % (freq2,freq2_l)
        print "offst: %d; %d bits" % (offset,offset_l)
        
        # now write it out in our legacy c-header-like format.  TODO: reasonable format, or ctypes bindings for packer.
        dbs = open(self.workdir + "dbspecs4.h","w")
        print >> dbs, "#define FIELDS 9"
        print >> dbs, "#define TYPE_LENGTH 1"
        print >> dbs, "#define BLK_SIZE " + str(blocksize)
        print >> dbs, "#define FREQ1_LENGTH " + str(freq1_l)
        print >> dbs, "#define FREQ2_LENGTH " + str(freq2_l)
        print >> dbs, "#define OFFST_LENGTH " + str(offset_l)
        print >> dbs, "#define NEGATIVES {0,0,0,0,0,0,0,0,0}"
        print >> dbs, "#define DEPENDENCIES {-1,0,1,2,3,4,5,0,0}"
        print >> dbs, "#define BITLENGTHS {%s}" % ",".join(str(i) for i in vl)
        dbs.close()

        os.system("pack4 " + self.workdir + "dbspecs4.h < " + self.workdir + "/all.words.sorted")
        print "all indices built. moving into place."
        os.system("mv index " + self.destination + "/index")
        os.system("mv index.1 " + self.destination + "/index.1")                

    def make_tables(self):
        toms = SqlToms.SqlToms("../toms.db",7)
        toms.mktoms_sql(self.workdir + "/all.toms.sorted")
        toms.dbh.execute("ALTER TABLE toms ADD COLUMN word_count;")
        for f in self.fileinfo:
            count = int(open(f["count"]).read())
            toms.dbh.execute("UPDATE toms SET word_count = %d WHERE filename = '%s';" % (count,f["name"]))
        toms.dbh.commit()
        pagedb = SqlToms.SqlToms("../pages.db",9)
        pagedb.mktoms_sql(self.workdir + "/all.pages")
        pagedb.dbh.commit()

    def finish(self):
        os.mkdir(self.destination + "/src/")
        os.system("mv dbspecs4.h ../src/dbspecs4.h")

# a quick utility function
def load(path,files,xpaths=None,metadata_xpaths=None,workers=4):
    l = Loader(workers)    
    l.setup_dir(path,files)
    l.parse_files(xpaths,metadata_xpaths)
    l.merge_objects()
    l.analyze()
    l.make_tables()
    l.finish()
   

        
if __name__ == "__main__":
    os.environ["LC_ALL"] = "C" # Exceedingly important to get uniform sort order.
    os.environ["PYTHONIOENCODING"] = "utf-8"
    
    usage = "usage: philoload.py destination_path texts ..."
    
    try :
        destination = sys.argv[1]
    except IndexError:
        print usage
        exit()
        
    texts = sys.argv[2:]
    if len(sys.argv[2:]) == 0:
        print usage
        exit()

    load(destination,texts)
    
    print "done"
