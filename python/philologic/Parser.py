from philologic import OHCOVector, shlaxtree
from philologic.ParserHelpers import *
import re

et = shlaxtree.et  # MAKE SURE you use ElementTree version 1.3.
				   # This is standard in Python 2.7, but an add on in 2.6,
				   # so you have to set the package right at make/configure/install time.
				   # if you did it wrong, you can fix it in shlaxtree or reinstall.

# A list of valid types in the Philo object hierarchy, used to construct an OHCOVector.Stack
# The index is constructed by "push" and "pull" operations to various types, and sending metadata into them.
# Don't try to push or pull byte objects.  we populate it by hand right now.
# Keep your push and pulls matched and nested unless you know exactly what you're doing.

ARTFLVector = ["doc","div1","div2","div3","para","sent","word"]
ARTFLParallels = ["byte","page"]

# The compressor is NOT configurable in this version, so DON'T change this format.
# Feel free to re-purpose the "page" object to store something else: line numbers, for example.

# The Element -> OHCOVector.Record mapping should be unambiguous, and context-free. 
# They're evaluated relative to the document root--note that if the root is closed and discarded, you're in trouble.
# TODO: add xpath support soon, for attribute matching. <milestone unit='x'>, for example.

TEIMapping = {	".":"doc", # Always fire a doc against the document root.
				".//front":"div",
				".//div":"div",
				".//div0":"div",
				".//div1":"div",
				".//div2":"div",
				".//div3":"div",
				".//p":"para",
				".//sp":"para",
				#"stage":"para"
			 } 

# Relative xpaths for metadata extraction.  look at the constructors in TEIHelpers.py for details.
# Make sure they are unambiguous, relative to the parent object.
# Note that we supply the class and its configuration arguments, but don't construct them yet.
# Full construction is carried out when new records are created, supplying the context and destination.

TEIPaths = { "doc" : [(ContentExtractor,"./teiHeader/fileDesc/titleStmt/author","author"),
                      (ContentExtractor,"./teiHeader/fileDesc/titleStmt/title", "title"),
                      (AttributeExtractor,".@xml:id","id")],
             "div" : [(ContentExtractor,"./head","head"),
             		  (AttributeExtractor,".@n","n"),
             		  (AttributeExtractor,".@xml:id","id")],
             "para": [(ContentExtractor,"./speaker", "who")],
           }

class Parser:
	def __init__(self,filename,docid,format=ARTFLVector,parallel=ARTFLParallels,map=TEIMapping,metadata_paths = TEIPaths,output=None):
		self.filename = filename
		self.docid = docid
		self.i = shlaxtree.ShlaxIngestor(target=self)
		self.tree = None
		self.root = None
		self.stack = []
		self.map = map
		self.v = OHCOVector.Stack(format[:],parallel[:]) # copies to make sure they don't get clobbered. next time you parse. 
		if output is None:
			output = open("/dev/null","w")
		self.v.out = output
		# OHCOVector should take an output file handle.
		self.metadata_paths = metadata_paths
		self.extractors = []

	def parse(self,input):
		"""Top level function for reading a file and printing out the output."""
		self.input = input
		for line in input:
			self.i.feed(line)
		return self.i.close()

	def feed(self,*event):
		"""Consumes a single event from the parse stream.
		
		Transforms "start","text", and "end" events into OHCOVector pushes, pulls, and attributes,
		based on the object and metadata Xpaths given to the constructor."""
		
		# I'm leaving some ugly comments in for debugging purposes.
		# TODO: Maybe a debug variable?
#		print >> self.out,event
#		print >> self.out, "current_objects",self.v.current_objects

		e_type, content, offset, name, attrib = event

		self.v.v[7] = offset # HACK for byte offset.

		if e_type == "start":
			# Add every element to the tree and store a pointer to it in the stack.
			# The first element will be the root of our tree.
			if self.root is None: 
				self.root = et.Element(name,attrib)	
				new_element = self.root
			else:
				new_element = et.SubElement(self.stack[-1],name,attrib)
			self.stack.append(new_element)

			# see if this Element should emit a new Record
			for xpath,ohco_type in self.map.items():
				if new_element in self.root.findall(xpath):
					#	print "pushing %s" % ohco_type
					if new_element == self.root:
						new_records = self.v.push(ohco_type,name,self.docid)
						self.v.get_current(ohco_type).attrib["filename"] = self.filename
						# If we have data from a preprocessor, we should use it here.						
					else:
						new_records = self.v.push(ohco_type,name) # set parent here.
						parent = self.v.get_parent(ohco_type)
						if parent:
							self.v.get_current(ohco_type).attrib["parent"] = " ".join(str(i) for i in parent.id)
					self.v.get_current(ohco_type).attrib['byte_start'] = offset
					# Set up metadata extractors for the new Record.
					# These get called for each child node or text event, until you hit a new record.
					# We could keep a stack of extractors for multiple simultaneous 
	
					if ohco_type in self.metadata_paths:
						self.extractors = []
						for extractor,pattern,field in self.metadata_paths[ohco_type]:
							# print "building %s at %s" % (extractor,new_element)
							self.extractors.append(extractor(pattern,field,new_element,new_records[0].attrib))
					break	# Don't check any other paths.
				
			# Attribute extraction done after new Element/Record, 
			for e in self.extractors:
				e(new_element,event)

		if e_type == "text":

			# Extract metadata if necessary.
			if self.stack:
				current_element = self.stack[-1]
				for e in self.extractors:
					e(current_element,event)
					# Should test whether to go on and tokenize or not.
						
			# Tokenize and emit tokens.  Still a bit hackish.
			# TODO: Tokenizer object shared with output formatter. 
			# Should push a sentence by default here, if we're in a new para/div/doc.  sent byte ordering is not quite right.
			tokens = re.finditer(ur"([\w\'\u2019]+)|([\.;:?!])",content,re.U) # should put in a nicer tokenizer.
			for t in tokens:
				if t.group(1):
					# Should push a sentence here if I'm not in one... all words occur in sentences.
					self.v.push("word",t.group(1)) 
					self.v.get_current("word").id[7] = offset + t.start(1) # HACK to set the byte offset.
					self.v.pull("word") 
				elif t.group(2): 
					if self.v.current_objects[5]: # Punctuation is the end of a sentence that began a few words ago.  So we pull first.  That means the byte position of a sentence is where it starts, not where it ends, oddly enough.
						self.v.get_current("sent").name = t.group(2) #HACK to set punctuation mark before pull.
						self.v.get_current("sent").id[7] = offset + t.start(2) # HACK to set byte offset before pull.
						self.v.pull("sent") 
					self.v.push("sent",".") # period by default, change it if we see something else.

		if e_type == "end":
			if self.stack:
				current_element = self.stack[-1]
				for xpath,ohco_type in self.map.items():
					# print "matching stack %s against %s for closure" % (self.stack,xpath)
					if current_element in self.root.findall(xpath):
						# print "found"
						self.v.get_current(ohco_type).attrib["byte_end"] = offset
						self.v.pull(ohco_type)
					break

			if self.stack: # All elements get pulled of the stack..
				if self.stack[-1].tag == name:
					old_element = self.stack.pop()
					# This can go badly out of whack if you're just missing one end tag, 
					# The OHCOVector is resilient enough to handle it a lot of the time.
					# Filter your events BEFORE the parser if you have especially ugly HTML or SGML.

					# prune the tree. saves memory. be careful.
					old_element.clear() # empty old element.
					if self.stack: # if the old element had a parent:
						del self.stack[-1][-1] # delete reference in parent
						
					else: # otherwise, you've cleared out the whole tree
						pass # and should do something clever.
						# might want to create a new root, and maybe even increment docid?



	def close(self):
		"""Finishes parsing a document, and emits any objects still extant.
		
		Returns a max_id vector suitable for building a compression bit-spec in a loader."""
		# pull all extant objects.
		objects = [n for n in enumerate(self.v.current_objects)]
		objects.reverse()
		for i,o in objects:
			if o:
				o.attrib['byte_end'] = self.v.v[7] # HACK
				ohco_type = self.v.types[i]
				self.v.pull(ohco_type)
		# return the maximum value for each field in the vector.
		return self.v.v_max


if __name__ == "__main__":
    import sys
    did = 1
    files = sys.argv[1:]
    for docid, filename in enumerate(files,1):
        f = open(filename)
        print >> sys.stderr, "%d: parsing %s" % (docid,filename)
        p = Parser(filename,docid, output=sys.stdout)
        p.parse(f)
        #print >> sys.stderr, "%s\n%d total tokens in %d unique types." % (spec,sum(counts.values()),len(counts.keys()))
