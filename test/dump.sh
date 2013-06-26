#/bin/sh

if [ -z  "$1" ]; then
  echo "You should specify the first parameter to the corpus file"
  exit
fi

cat $1 | ./bin/dump
