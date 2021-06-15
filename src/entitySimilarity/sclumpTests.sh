#!/bin/bash

for numC in {50,100,500,1000,5000}; do
 screen -dmS "session-trial:sclump" python executeSClump.py -nc=$numC
done
