'''
You can run this script like this using GNU Parallel:

ls paths/xxx | ./parallel -j 32 --eta "cat {} | python filter-streamitems-cpp.py 1> {}.speed.log 2> {}.errors.log  {}" &> parallel.log &

The streamcorpus-counter is a program like the one posted here:
  https://groups.google.com/d/msg/streamcorpus/fi8Y8yseF8o/viJjiFNVLNsJ

'''
import os
import sys
import time
import shutil
import subprocess
import streamcorpus
import argparse

gpg_dir = '/tmp/foo'

def import_gpg():
  gpg_private='~/trec.key'
  if not os.path.exists(gpg_dir):
    ## remove the gpg_dir
    shutil.rmtree(gpg_dir, ignore_errors=True)
    os.makedirs(gpg_dir)

  cmd = 'gpg --homedir %s --no-permission-warning --import' \
      '%s 2> gpg_errors.log' % (gpg_dir, gpg_private)
  gpg_child = Popen([cmd], shell=True)

def run(hour):
  start_time = time.time()
  chunk_count = 0
  print 'Processing %s' % hour

  save_dir = 'save/'
  save_file = save_dir + hour + '.sc'

  for line in sys.stdin:
    chunk_count += 1
    url = 'http://s3.amazonaws.com/aws-publicdatasets/' \
        'trec/kba/kba-streamcorpus-2013-v0_2_0-english-and-unknown-language' \
        + line.strip()
    cmd = '(wget -O - %s | gpg --homedir %s --no-permission-warning ' \
        '--trust-model always --output - --decrypt - | xz --decompress | ' \
        './bin/select 1>%s) 2>> subprocess_errors.log ' % (url, gpg_dir, save_file)
    print cmd
    #child = subprocess.Popen(cmd, shell=True)

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

