#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Generate the file list hierarchy for KBA 2013 corpus by parsing the S3 directory
'''

import sys,os
import re
import urllib
import urllib2
import codecs
import time
import math
from datetime import date, timedelta
from bs4 import BeautifulSoup
import json
import requests
import traceback
reload(sys)
sys.setdefaultencoding('utf-8')

def get_page(url):
  try:
    #time.sleep(0.5)
    response = requests.get(url)
    # force the encoding to utf-8
    response.encoding = 'utf-8'
    return response.text
  except:
    print 'Error: ' + url
    return 'ERROR'

def parse_list_page(content):
  page = BeautifulSoup(content, 'lxml')
  file_list = []

  for url in page.find_all('a'):
    url_href = url['href']
    if url_href.endswith('.xz.gpg'):
      file_list.append(url['href'])

  return file_list

def load_dir_list(dir_file):
  try:
    with open(dir_file) as f:
      print 'Loading %s' % dir_file
      dirs = [line.strip() for line in f]
      return dirs
  except IOError as e:
    print 'Can not open file: %s' % dir_file

def save_file(save_file, file_list):
  try:
    with open(save_file, 'ab') as f:
      print 'Saving %s' % save_file
      for file_path in file_list:
        f.write('%s\n' % file_path)
  except IOError as e:
    print 'Can not save file: %s'

def main():
  dirs = load_dir_list('dir-list.txt')
  hours_base_dir = 'hours/'

  '''
  retrieve all the results in the current date
  '''
  url_base = 'http://s3.amazonaws.com/aws-publicdatasets/trec/kba/'\
      'kba-streamcorpus-2013-v0_2_0-english-and-unknown-language/'
  num = len(dirs)

  for idx, hour_dir in enumerate(dirs):
    print 'Retrieving %s [%5d / %5d]' %(hour_dir, idx, num)
    # construct the URL to the index page of a specific hour slot directory
    url = url_base + hour_dir + '/index.html'
    file_list = parse_list_page(get_page(url))

    file_name_list = []
    for file_name in file_list:
      file_path = '/' + hour_dir + '/' + file_name
      file_name_list.append(file_path)

    # save the hour list file
    hour_list_file = hours_base_dir + hour_dir
    save_file(hour_list_file, file_name_list)
    #break

if __name__ == '__main__':
  try:
    main()
  except KeyboardInterrupt:
    print '\nGoodbye!'

