#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Apply filtering again on first-filtered documents from old alternative names

Usage: re-run-filter.py <hour>

'''
import os
import sys
import time
import shutil
import subprocess
import streamcorpus
import argparse

def run(hour):
  print 'Processing %s' % hour
  file_name = '%s.sc' % hour

  cmd = '(cat input/%s | ./bin/filter 1>output/%s) 2>log/%s ' % (file_name,
      file_name, hour)

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

