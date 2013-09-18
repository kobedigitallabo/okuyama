#!/bin/sh
java -classpath ./classes:./lib/log4j-1.2.14.jar:./lib/javamail-1.4.1.jar:./lib/commons-codec-1.4.jar -Xmx1512m -Xms512m -Xmn128m -server -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -XX:+UseParNewGC okuyama.base.JavaMain  /Main.properties /DataNode_small.properties
