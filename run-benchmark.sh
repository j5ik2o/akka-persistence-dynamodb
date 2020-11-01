#!/bin/sh

# sbt 'benchmark/jmh:run -prof jmh.extras.JFR:dir=.. -i 5 -wi 3 -f1 -t1'
sbt ";clean ;benchmark/jmh:compile ;benchmark/jmh:run -i 3 -wi 2 -f1 -t1 $@"