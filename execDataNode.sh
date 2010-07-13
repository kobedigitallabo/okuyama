#!/bin/sh
java -classpath ./classes:./lib/log4j-1.2.14.jar:./lib/javamail-1.4.1.jar -Xmx384m -Xms384m  org.batch.JavaMain  /Main.properties /DataNode.properties
