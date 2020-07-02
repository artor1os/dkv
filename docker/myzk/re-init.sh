#!/usr/bin/env bash

rm -f conf/zoo.cfg.dynamic.next

rm -rf data1
rm -rf data2
rm -rf data3

mkdir data1
mkdir data2
mkdir data3

echo 1 > data1/myid
echo 2 > data2/myid
echo 3 > data3/myid

touch data1/initialize
touch data2/initialize
touch data3/initialize
