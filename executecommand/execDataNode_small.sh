#!/bin/sh
java -classpath ./classes:./lib/log4j-1.2.14.jar:./lib/javamail-1.4.1.jar:./lib/commons-codec-1.4.jar -Xmx512m -Xms256m -Xmn64m -server -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -XX:+UseParNewGC -XX:TargetSurvivorRatio=98 -XX:MaxTenuringThreshold=32 okuyama.base.JavaMain  /Main.properties /DataNode_small.properties -vidf true -svic 2