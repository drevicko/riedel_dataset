#!/bin/bash
convert () {
  python pb_to_tsv.py $1 $2
}

if [ -z "$1"  ]
  then
    echo "No argument supplied"
    echo "./convert.bash 1 means convert data"
    echo "./convert.bash 0 means delete converted data"
    exit 1
fi

if [ $1 -eq 0 ]
then
  read -p "do you want to delete the converted data?" yn
  case $yn in 
	  [yY]* ) 
	  echo "deleting converted data"
	  rm -rf ../../data/kb_manual/trainPositive.tsv
	  rm -rf ../../data/kb_manual/trainNegative.tsv

	  rm -rf ../../data/heldout_relations/trainPositive.tsv
	  rm -rf ../../data/heldout_relations/trainNegative.tsv
	  rm -rf ../../data/heldout_relations/testPositive.tsv
	  rm -rf ../../data/heldout_relations/testNegative.tsv
  esac
else
  echo "converting data"
  #convert all files
  convert ../../data/kb_manual/trainPositive.pb ../../data/filtered-freebase-simple-topic-dump-3cols.tsv 
  convert ../../data/kb_manual/trainNegative.pb ../../data/filtered-freebase-simple-topic-dump-3cols.tsv

  convert ../../data/heldout_relations/trainPositive.pb ../../data/filtered-freebase-simple-topic-dump-3cols.tsv
  convert ../../data/heldout_relations/trainNegative.pb ../../data/filtered-freebase-simple-topic-dump-3cols.tsv
  convert ../../data/heldout_relations/testPositive.pb ../../data/filtered-freebase-simple-topic-dump-3cols.tsv
  convert ../../data/heldout_relations/testNegative.pb ../../data/filtered-freebase-simple-topic-dump-3cols.tsv
fi

