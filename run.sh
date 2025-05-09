#!/bin/bash

# 执行Maven打包并运行Java程序
mvn clean package
time java --add-opens java.base/java.util=ALL-UNNAMED -jar target/flink-stream-processing-1.0-SNAPSHOT.jar > output.txt
