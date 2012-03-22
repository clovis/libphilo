#!/usr/bin/env python

import os
import cPickle
from philologic.OHCOVector import Record
from ast import literal_eval as eval


## Default filters
def make_word_counts(loader_obj, text, depth=5):
    object_types = ['doc', 'div1', 'div2', 'div3', 'para', 'sent', 'word']
    counts = [0 for i in range(depth)]
    temp_file = text['raw'] + '.tmp'
    output_file = open(temp_file, 'w')
    for line in open(text['raw']):
        type, word, id, attrib = line.split('\t')
        id = id.split()
        record = Record(type, word, id)
        record.attrib = eval(attrib)
        for d,count in enumerate(counts):
            if type == 'word':
                counts[d] += 1
            elif type == object_types[d]:
                record.attrib['word_count'] = counts[d]
                counts[d] = 0
        print >> output_file, record
    output_file.close()
    os.remove(text['raw'])
    os.rename(temp_file, text['raw'])
    
def prev_next_obj(loader_obj, text, depth=5):
    object_types = ['doc', 'div1', 'div2', 'div3', 'para', 'sent', 'word'][:depth]
    record_dict = {}
    temp_file = text['raw'] + '.tmp'
    output_file = open(temp_file, 'w')
    for line in open(text['sortedtoms']):
        type, word, id, attrib = line.split('\t')
        id = id.split()
        record = Record(type, word, id)
        record.attrib = eval(attrib) 
        if type in record_dict:
            record_dict[type].attrib['next'] = ' '.join(id)
            if type in object_types:
                print >> output_file, record_dict[type]
            else:
                del record_dict[type].attrib['next']
                del record_dict[type].attrib['prev']
                print >> output_file, record_dict[type]
            record.attrib['prev'] = ' '.join(record_dict[type].id)
            record_dict[type] = record
        else:
            record.attrib['prev'] = ''
            record_dict[type] = record
    object_types.reverse()
    for obj in object_types:
        record_dict[obj].attrib['next'] = ''
        print >> output_file, record_dict[obj]
    output_file.close()
    os.remove(text['sortedtoms'])
    tomscommand = "cat %s | egrep \"^doc|^div|^para\" | sort %s > %s" % (temp_file,loader_obj.sort_by_id,text["sortedtoms"])
    os.system(tomscommand)
    os.remove(temp_file)

def generate_words_sorted(loader_obj, text):
    wordcommand = "cat %s | egrep \"^word\" | sort %s %s > %s" % (text["raw"],loader_obj.sort_by_word,loader_obj.sort_by_id,text["words"])
    os.system(wordcommand)        
    
def sorted_toms(loader_obj, text):
    tomscommand = "cat %s | egrep \"^doc|^div|^para\" | sort %s > %s" % (text["raw"],loader_obj.sort_by_id,text["sortedtoms"])
    os.system(tomscommand)
    
def generate_pages(loader_obj, text):
    pagescommand = "cat %s | egrep \"^page\" > %s" % (text["raw"],text["pages"])
    os.system(pagescommand)
    
def make_max_id(loader_obj, text):
    max_id = None
    for line in open(text["words"]):
        (key,type,id,attr) = line.split("\t")
        id = [int(i) for i in id.split(" ")]
        if not max_id:
            max_id = id
        else:
            max_id = [max(new,prev) for new,prev in zip(id,max_id)]
    rf = open(text["results"],"w")
    cPickle.dump(max_id,rf) # write the result out--really just the resulting omax vector, which the parent will merge in below.
    rf.close()
    
## Additional Filters
def make_token_counts(loader_obj, text, depth=5):
    old_word = None
    record_list = []
    temp_file = text['words'] + '.tmp'
    output_file = open(temp_file, 'w')
    for line in open(text['words']):
        type, word, id, attrib = line.split('\t')
        id = id.split()
        record = Record(type, word, id)
        record.attrib = eval(attrib)
        if word == old_word or old_word == None:
            record_list.append(record)
        else:
            count_tokens(record_list, depth, output_file)
            record_list = []
            record_list.append(record)
        old_word = word
    if len(record_list) != 0:
        count_tokens(record_list, depth, output_file)
    record_list = []
    os.remove(text['words'])
    os.rename(temp_file, text['words'])
    
## Helper functions
def count_tokens(record_list, depth, output_file):
    object_types = ['doc', 'div1', 'div2', 'div3', 'para', 'sent', 'word']
    new_record_list = []
    record_dict = {}
    for new_record in record_list:
        new_record.attrib['doc_token_count'] = len(record_list)
        for d in range(depth):
            philo_id = tuple(new_record.id[:d+1])
            if philo_id not in record_dict:
                record_dict[philo_id] = 1
            else:
                record_dict[philo_id] += 1
    for new_record in record_list:
        for d in range(depth):
            philo_id = tuple(new_record.id[:d+1])
            token_count = object_types[len(philo_id)-1] + '_token_count'
            new_record.attrib[token_count] = record_dict[philo_id]
        print >> output_file, new_record