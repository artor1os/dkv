#!/usr/bin/env bash

rm conf/zoo.cfg.dynamic.next

rm -r data1
rm -r data2
rm -r data3

mkdir data1
mkdir data2
mkdir data3

echo 1 > data1/myid
echo 2 > data2/myid
echo 3 > data3/myid

touch data1/initialize
touch data2/initialize
touch data3/initialize
