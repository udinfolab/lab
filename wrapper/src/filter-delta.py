#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Filter the documents which are new in the 07-19 judgments

'''
import os
import sys
import time
import shutil
import datetime
import subprocess
import streamcorpus

gpg_dir = '/tmp/foo'

stream_list = []
md5_map = {}
hour_map = {}
file_map = {}

def import_gpg():
  gpg_private='~/trec.key'
  if not os.path.exists(gpg_dir):
    ## remove the gpg_dir
    shutil.rmtree(gpg_dir, ignore_errors=True)
    os.makedirs(gpg_dir)

  cmd = 'gpg --homedir %s --no-permission-warning --import' \
      ' %s' % (gpg_dir, gpg_private)
  gpg_child = subprocess.Popen([cmd], shell=True)
  gpg_child.wait()
  print 'Done.'

# generate the reverse path for the given stream_id
def gen_reverse_path():
  # first, load the stream list
  load_stream_list()
  # load chunk to stream_id map file
  load_chunk_to_stream_id_map()

  # do map
  for stream_id in stream_list:
    if not stream_id in md5_map:
      print 'No map found for %s' % stream_id
      continue
    md5 = md5_map[stream_id]
    hour = hour_map[stream_id]

    # load the path file for the specific hour
    hour_file_path = 'hours/%s' % hour
    try:
      with open(hour_file_path) as f:
        content = f.readlines()
        for line in content:
          file_md5 = line.split('/')[2].split('-')[2]
          if md5 == file_md5:
            file_map[stream_id] = line.strip()
    except IOError as e:
      print 'Failed to open file: %s' % hour_file_path

def load_stream_list():
  stream_list_file = 'diff.to_add.list'
  try:
    with open(stream_list_file) as f:
      print 'Loading %s' % stream_list_file
      content = f.readlines()
      for line in content:
        items = line.split(' ')
        stream_id = items[2]
        stream_list.append(stream_id)
  except IOError as e:
    print 'Failed to open file: %s' % stream_list_file

def load_chunk_to_stream_id_map():
  map_file = 'chunk-to-stream_id.txt'
  try:
    with open(map_file) as f:
      print 'Loading %s' % map_file
      content = f.readlines()
      for line in content:
        items = line.split('|')
        path = items[0]
        md5 = path.split('/')[4].split('.')[0].split('-')[2]
        hour = path.split('/')[3]
        stream_id = items[1].split('(')[1].split(',')[0]
        md5_map[stream_id] = md5
        hour_map[stream_id] = hour
  except IOError as e:
    print 'Failed to open file: %s' % map_file

def run():
  save_dir = 'save/'
  stream_id_list = file_map.keys()
  stream_id_list.sort(key=lambda x: int(x.split('-')[0]))
  chunk_count = len(stream_id_list)

  for stream_id in stream_id_list:
    hour = hour_map[stream_id]
    file = file_map[stream_id]
    save_file = save_dir + hour + '.sc'
    print '[%s / %s / %d]' %(hour, stream_id, chunk_count)

    url = 'http://s3.amazonaws.com/aws-publicdatasets/' \
        'trec/kba/kba-streamcorpus-2013-v0_2_0-english-and-unknown-language' \
        + file
    cmd = '(wget -O - %s | gpg --homedir %s --no-permission-warning ' \
        '--trust-model always --output - --decrypt - | xz --decompress | ' \
        './r-bin/select 1>>%s) 2>>log/%s ' % (url, gpg_dir, save_file, hour)
    #print cmd
    #continue

    child = subprocess.Popen(cmd, shell=True)
    # wait for the command to end
    child.wait()

def main():
  import_gpg()
  gen_reverse_path()
  run()

if __name__ == '__main__':
  try:
    main()
  except KeyboardInterrupt:
    print '\nGoodbye!'

