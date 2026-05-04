#! /bin/bash

export JAVA_HOME="${JAVA_HOME:-/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home}"

"$JAVA_HOME/bin/java" -version
./mvnw spring-boot:run
