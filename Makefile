
GRADLE ?= ./gradlew --warning-mode=all
NPM    ?= 'npm'
OSTYPE := $(shell uname)
OS_LOWER := $(shell uname -s | tr '[:upper:]' '[:lower:]')


default: compile

compile:
	$(MAKE) -C common
	$(MAKE) -C theseus


#
# Run the unit tests
#
check:
	$(MAKE) -C common check
	$(MAKE) -C theseus check


#
# Clean
#
clean:
	$(MAKE) -C common clean
	$(MAKE) -C theseus clean
