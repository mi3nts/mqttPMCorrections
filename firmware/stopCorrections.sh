#!/bin/sh


kill $(pgrep -f 'liveDCCorrections.py')
sleep 1

kill $(pgrep -f 'liveLNCorrections.py')
sleep 1


