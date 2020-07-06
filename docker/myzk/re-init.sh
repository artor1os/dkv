#!/usr/bin/env bash

rm -f conf/zoo.cfg.dynamic.next

rm -f data1/*
rm -f data2/*
rm -f data3/*

echo 1 > data1/myid
echo 2 > data2/myid
echo 3 > data3/myid

touch data1/initialize
touch data2/initialize
touch data3/initialize
