#!/bin/sh
java -classpath ./classes:./lib/log4j-1.2.14.jar -Xmx256m -Xms256m org.batch.JavaMain /Main.properties /MasterNode.properties
