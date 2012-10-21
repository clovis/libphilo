import sys
import os
import errno
import philologic
from optparse import OptionParser
from glob import glob
from philologic.Loader import Loader
from philologic.LoadFilters import *
from philologic.Parser import Parser
from philologic.ParserHelpers import *


#########################
## Command-line parsing #
#########################
usage = "usage: %prog [options] database_name files"
parser = OptionParser(usage=usage)
parser.add_option("-q", "--quiet", action="store_true", dest="quiet", help="suppress all output")
parser.add_option("-c", "--cores", type="int", default="2", dest="workers", help="define the number of cores for parsing")
parser.add_option("-t", "--templates", default=False, dest="template_dir", help="define the path for the templates you want to use")
parser.add_option("-d", "--debug", action="store_true", default=False, dest="debug", help="add debugging to your load")


##########################
## System Configuration **
##########################

# Set the filesytem path to the root web directory for your PhiloLogic install.
database_root = None
# /var/www/html/philologic/ is conventional for linux,
# /Library/WebServer/Documents/philologic for Mac OS.
# Please follow the instructions in INSTALLING before use.

# Set the URL path to the same root directory for your philologic install.
url_root = None 
# http://localhost/philologic is appropriate if you don't have a DNS hostname.

if database_root is None or url_root is None:
    print >> sys.stderr, "Please configure the loader script before use.  See INSTALLING in your PhiloLogic distribution."
    exit()

template_dir = database_root + "_system_dir/_install_dir/"
# The load process will fail if you haven't set up the template_dir at the correct location.


###########################
## Configuration options ##
###########################

## Parse command-line arguments
(options, args) = parser.parse_args(sys.argv[1:])
try:
    dbname = args[0]
    args.pop(0)
    if args[-1].endswith('/') or os.path.isdir(args[-1]):   
        files = glob(args[-1] + '/*')
    else:
        files = args[:]
except IndexError:
    print >> sys.stderr, "\nError: you did not supply a database name or a path for your file(s) to be loaded\n"
    parser.print_help()
    sys.exit()
workers = options.workers or 2
template_dir = options.template_dir or template_dir
quiet = options.quiet or False
debug = options.debug or False

# Define text objects for ranked relevancy: by default it's ['doc']. Disable by supplying empty list
r_r_obj = ['doc'] 

# Data tables to store.
tables = ['toms', 'pages', 'ranked_relevance']

# Define filters as a list of functions to call, either those in Loader or outside
filters = [make_word_counts, generate_words_sorted,make_token_counts,sorted_toms,
                prev_next_obj, word_frequencies_per_obj(*r_r_obj),generate_pages, make_max_id]  

# Define text objects to generate plain text files for various machine learning tasks
plain_text_obj = []
if plain_text_obj:
    filters.extend([store_in_plain_text])

extra_locals = {}
if r_r_obj:
    extra_locals['ranked_relevance_objects'] = r_r_obj


###########################
## Set-up database load ###
###########################

Philo_Types = ["doc","div","para","word"] # every object type you'll be indexing.  pages don't count, yet.

XPaths = {  ".":"doc", # Always fire a doc against the document root.
            ".//front":"div",
            ".//div":"div",
            ".//div1":"div",
            ".//div2":"div",
            ".//div3":"div",
            ".//p":"para",
            ".//sp":"para",
            ".//w":"word",
            #"stage":"para"
            ".//pb":"page",
         } 

Metadata_XPaths = { # metadata per type.  '.' is in this case the base element for the type, as specified in XPaths above.
             "doc" : [(ContentExtractor,"./teiHeader/fileDesc/titleStmt/author","author"),
                      (ContentExtractor,"./teiHeader/fileDesc/titleStmt/title", "title"),
                      (ContentExtractor,"./teiHeader/sourceDesc/biblFull/publicationStmt/date", "date"),
                      (AttributeExtractor,"./text/body/volume@n","volume"),
                      (AttributeExtractor,".@xml:id","id")],
             "div" : [(ContentExtractor,"./head","head"),
                      (ContentExtractor,"./head//*","head"),
                      (AttributeExtractor,".@n","n"),
                      (AttributeExtractor,".@xml:id","id")],
             "para": [(ContentExtractor,"./speaker", "who"),
                      (ContentExtractor,"./head","head")],
             "word": [(AttributeExtractor,".@lemma","lemma"),
#                      (ContentExtractor,".","token"),
                      (AttributeExtractor,".@ana","ana")],
             "page": [(AttributeExtractor,".@n","n"),
                      (AttributeExtractor,".@src","img")],
           }

non_nesting_tags = ["div1","div2","div3","p","P"]
self_closing_tags = ["pb","p","Xdiv","note","span","br","P","BR",]
pseudo_empty_tags = ["milestone"]

word_regex = r"([^ \.,;:?!\"\n\r\t\(\)]+)"
punct_regex = r"([\.;:?!])"

token_regex = word_regex + "|" + punct_regex 

#############################
# Actual work.  Don't edit. #
#############################

os.environ["LC_ALL"] = "C" # Exceedingly important to get uniform sort order.
os.environ["PYTHONIOENCODING"] = "utf-8"
    
template_destination = database_root + dbname
data_destination = template_destination + "/data"
db_url = url_root + "/" + dbname

try:
    os.mkdir(template_destination)
except OSError:
    print "The %s database already exists" % dbname
    print "Do you want to delete this database? Yes/No"
    choice = raw_input().lower()
    if choice.startswith('y'):
        os.system('rm -rf %s' % template_destination)
        os.mkdir(template_destination)
    else:
        sys.exit()
os.system("cp -r %s* %s" % (template_dir,template_destination))
os.system("cp %s.htaccess %s" % (template_dir,template_destination))
if not quiet:
    print "copied templates to %s" % template_destination


####################
## Load the files ##
####################

print "\nIndexing begins... \n"
if quiet:
    verbose = sys.stdout
    sys.stdout = open(os.devnull, 'w')
l = Loader(data_destination,
           Philo_Types,
           XPaths,
           Metadata_XPaths,
           filters, 
           token_regex,
           non_nesting_tags,
           self_closing_tags,
           pseudo_empty_tags,
           debug=debug)
l.add_files(files)
filenames = l.list_files()
load_metadata = [{"filename":f} for f in sorted(filenames,reverse=True)]
l.parse_files(workers,load_metadata)
l.merge_objects()
l.analyze()
l.make_tables(tables, *r_r_obj)
l.finish(**extra_locals)

if quiet:
    sys.stdout = verbose
print "\nDone indexing."
print "Your database is viewable at " + db_url + "\n"