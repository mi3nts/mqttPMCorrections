#!/bin/sh

kill $(pgrep -f 'python3 liveDCCorrections.py')
sleep 1
python3 liveDCCorrections.py &

kill $(pgrep -f 'python3 liveLNCorrections.py')
sleep 1
python3 liveLNCorrections.py &


