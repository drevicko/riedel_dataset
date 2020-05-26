import Document_pb2
import sys
import os
import codecs
import csv
from google.protobuf.internal.decoder import _DecodeVarint32
from pathlib import Path

def create_guid_dict(file_name):
    '''
    Create a dictionary with guids as key and entity name as value
    '''
    tsvin = csv.reader(open(file_name, 'r'), delimiter='\t')
    guid_dict = {}
    for row in tsvin:
        guid_dict[row[0]] = row[1]
    return guid_dict


def base32(num, numerals="0123456789bcdfghjklmnpqrstvwxyz_"):
    '''ref: http://code.activestate.com/recipes/65212/'''
    return ((num == 0) and numerals[0]) or (base32(num // 32, numerals).lstrip(numerals[0]) + numerals[num % 32])


def guid_to_mid(guid):
    '''
    Convert guid to mid. 
    Conversion rules mentioned at http://wiki.freebase.com/wiki/Guid
    '''
    guid = guid.split('/')[-1]
    # remove 9202a8c04000641f8
    guid = guid[17:]
    i = int(guid,16)
    mid = 'm.0' + str(base32(i))
    return mid

# TODO: put input file here instead and open it.
# input_dir = sys.argv[1]
# file_list = os.listdir(input_dir)
input_file = Path(sys.argv[1])

# out_dir = os.sep.join(input_dir.split(os.sep)[:-2]) + os.sep
# out_file = input_dir.split(os.sep)[-2] + '.tsv'
out_file = input_file.with_suffix(".tsv")
# f_write = csv.writer(open(out_dir + out_file, 'w'), delimiter='\t')
f_write = csv.writer(open(out_file, 'w'), delimiter='\t')

print("Starting converting files in ", input_file)
guid_mapping_file = sys.argv[2]
guid_dict = create_guid_dict(guid_mapping_file)


# TODO: function to iterate over "Relation()"s in the input file
def relationIterator(input_file, reuse_msg_object):
    if reuse_msg_object:
        rel = Document_pb2.Relation()
    with open(input_file, "rb") as fin:
        buf = fin.read()
        next_pos, pos = 0, 0
        # num_read = -1
        while pos < len(buf):
        # while True:
            try:
                # num_read += 1
                # if num_read >= 10:
                #     return
                if not reuse_msg_object:
                    rel = Document_pb2.Relation()
                length, bytes_read = _DecodeVarint32(buf[pos:], 0)
                # length, bytes_read = _DecodeVarint32(fin.read(2), 0)
                pos += bytes_read
                # print(f"at {pos} after {bytes_read}, reading {length}")
                bytes_read = rel.ParseFromString(buf[pos : pos + length])
                # bytes_read = rel.ParseFromString(fin.read(length))
                if bytes_read != length:
                    print(f"{bytes_read} != {length}; {type(bytes_read)}, {type(length)}")
                    exit(0)
                pos += bytes_read
                # print(f"at {pos} after {bytes_read}")
                yield rel
            except EOFError:
                return


# for file_name in file_list:
for rel in relationIterator(input_file, False):
    # rel = Document_pb2.Relation()
    # f = open(input_dir + file_name, 'r')
    # rel.ParseFromString(f.read())
    sourceId = rel.sourceGuid
    destId = rel.destGuid
    e1_name = guid_dict[sourceId]
    e2_name = guid_dict[destId]
    relation = rel.relType
    sourceId = guid_to_mid(sourceId)
    destId = guid_to_mid(destId)
    # print(f"got {e1_name}  {relation}  {e2_name}")
    n = 0
    for mention in rel.mention:
        n += 1
        e1_name_new = e1_name.replace(' ', '_')
        e2_name_new = e2_name.replace(' ', '_')

        sentence = mention.sentence
        sentence = sentence.replace(e1_name, e1_name_new)
        sentence = sentence.replace(e2_name, e2_name_new)

        types = mention.feature[0].split('->')
        
        f_write.writerow([sourceId, destId, e1_name_new.lower(), e2_name_new.lower(), 
        	types[0], types[1], relation, sentence.lower()])
    # print(f"    with {n} mentions")
    # f.close()
