#!/bin/sh

case "$1" in

  'master')
      ARGS="-dataDir=/data"
      exec dkv "$@" $ARGS
      ;;

  'replica')
      ARGS="-dataDir=/data"
      exec dkv "$@" $ARGS
      ;;

  'client')
        ARGS=""
        exec dkv "$@" ARGS
        ;;
   *)
        exec dkv "$@"
        ;;
esac