#!/bin/sh

LD_LIBRARY_PATH=./:/usr/local/lib java -classpath ./okuyamaFuse-0.0.2.jar:./fuse-j.jar:./commons-logging-1.0.4.jar:./okuyama-0.9.6.jar:./javamail-1.4.1.jar:./log4j-1.2.14.jar \
   -Dorg.apache.commons.logging.Log=fuse.logging.FuseLog \
   -Dfuse.logging.level=ERROR -Xmx1548m -Xms1524m -server -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -XX:+UseParNewGC  \
   fuse.okuyamafs.OkuyamaFuse -f -o direct_io -o allow_other $1 $2 1>> ./okufs.log 2>>./okufs.log
