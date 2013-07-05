#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Run the mapping program
'''
import os
import sys
import time
import shutil
import subprocess
import streamcorpus
import argparse

corpus_dir = '/corpus/filtered/2010-10/'
save_dir = 'map/'

def run(hour):
  start_time = time.time()
  chunk_count = 0
  print 'Processing %s' % hour

  corpus_file = corpus_dir + hour + '.sc'
  save_file = save_dir + hour

  cmd = 'cat %s | ./r-bin/map 1>%s 2>log/%s' % (corpus_file, save_file, hour)

  #print cmd
  #return

  child = subprocess.Popen(cmd, shell=True)
  # wait for the command to end
  child.wait()

def main():
  parser = argparse.ArgumentParser(usage=__doc__)
  parser.add_argument('hour')
  args = parser.parse_args()

  run(args.hour)

if __name__ == '__main__':
  try:
    main()
  except KeyboardInterrupt:
    print '\nGoodbye!'

