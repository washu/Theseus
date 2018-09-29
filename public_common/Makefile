
GRADLE ?= ./gradlew --warning-mode=all
NPM    ?= 'npm'
OSTYPE := $(shell uname)
OS_LOWER := $(shell uname -s | tr '[:upper:]' '[:lower:]')

JAVA_FILES  := $(shell find src/main/java -type f -name "*.java" ) $(shell find src/test/java -type f -name "*.java" )


default: compile

compile:
	${GRADLE} compileJava processResources


#
# Run the unit tests
#
check: $(JAVA_FILES)
	${GRADLE} test itest slowtest

test: $(JAVA_FILES)
	${GRADLE} test

itest: $(JAVA_FILES)
	${GRADLE} itest

slowtest: $(JAVA_FILES)
	${GRADLE} slowtest


#
# Clean
#

clean:
	${GRADLE} clean
	if [ -e out ]; then rm -r out; fi
