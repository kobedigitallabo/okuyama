#!/bin/sh
java -classpath ./classes:./lib/log4j-1.2.14.jar -Xmx384m -Xms384m  org.batch.JavaMain  /Main.properties /SlaveDataNode.properties
