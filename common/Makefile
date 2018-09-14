
GRADLE ?= ./gradlew --warning-mode=all
NPM    ?= 'npm'
OSTYPE := $(shell uname)
OS_LOWER := $(shell uname -s | tr '[:upper:]' '[:lower:]')


default: compile

compile:
	${GRADLE} compileJava processResources


#
# Run the unit tests
#
check:
	${GRADLE} test itest slowtest

test:
	${GRADLE} -x test

itest:
	${GRADLE} -x itest

slowtest:
	${GRADLE} -x slowtest


#
# Clean
#

clean:
	${GRADLE} clean
	if [ -e out ]; then rm -r out; fi
