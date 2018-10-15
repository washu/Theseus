
VERSION := 0.3.0

GRADLE ?= ./gradlew --warning-mode=all
NPM    ?= 'npm'
OSTYPE := $(shell uname)
OS_LOWER := $(shell uname -s | tr '[:upper:]' '[:lower:]')

COMMON_SOURCES := $(shell find public_common/src/main -name "*.java") $(shell find public_common/src/gen -name "*.java")
THESEUS_SOURCES := $(shell find theseus/src/main -name "*.java") $(shell find theseus/src/gen -name "*.java")

default: compile

compile:
	$(MAKE) -C public_common
	$(MAKE) -C theseus

theseus/build/classpath.txt: theseus/build.gradle public_common/build.gradle
	cd theseus && ./gradlew writeTheseusClasspath

release/theseus-${VERSION}.jar: theseus/build/classpath.txt ${COMMON_SOURCES} ${THESEUS_SOURCES}
	mkdir -p ./build/
	cd public_common && ./gradlew generateProto
	cd theseus && ./gradlew generateProto
	@echo "Compiling Java..."
	@javac -encoding utf8 -cp "`cat theseus/build/classpath.txt`" -d ./build ${COMMON_SOURCES} ${THESEUS_SOURCES}
	@echo "Creating Jar..."
	mkdir -p ./release/
	jar cvf release/theseus-${VERSION}.jar -C build .

release/theseus-${VERSION}-sources.jar: ${COMMON_SOURCES} ${THESEUS_SOURCES}
	mkdir -p ./release/
	@echo "Creating Jar..."
	jar cvf release/theseus-${VERSION}-sources.jar -C theseus/src/main/java .
	jar uvf release/theseus-${VERSION}-sources.jar -C theseus/src/gen/proto/java/main/java .
	jar uvf release/theseus-${VERSION}-sources.jar -C theseus/src/gen/proto/java/main/grpc .
	jar uvf release/theseus-${VERSION}-sources.jar -C public_common/src/main/java .
	jar uvf release/theseus-${VERSION}-sources.jar -C public_common/src/gen/proto/java/main/java .
	jar uvf release/theseus-${VERSION}-sources.jar -C public_common/src/gen/proto/java/main/grpc .

release/theseus-${VERSION}-javadoc.jar: ${COMMON_SOURCES} ${THESEUS_SOURCES}
	mkdir -p ./doc/
	@echo "Compiling Javadoc..."
	javadoc -cp "`cat theseus/build/classpath.txt`" -d ./doc ${COMMON_SOURCES} ${THESEUS_SOURCES}
	@echo "Creating Jar..."
	mkdir -p ./release/
	jar cvf release/theseus-${VERSION}-javadoc.jar -C doc .

release: release/theseus-${VERSION}.jar release/theseus-${VERSION}-sources.jar release/theseus-${VERSION}-javadoc.jar
	@echo "Signing code..."
	cd release; gpg -ab theseus-${VERSION}.jar
	@echo "Signing sources..."
	cd release; gpg -ab theseus-${VERSION}-sources.jar
	@echo "Signing javadoc..."
	cd release; gpg -ab theseus-${VERSION}-javadoc.jar
	@echo "Copying pom.xml..."
	cp pom.xml release/theseus-${VERSION}.pom


#
# Run the unit tests
#
check:
	$(MAKE) -C public_common check
	$(MAKE) -C theseus check


#
# Clean
#
clean:
	$(MAKE) -C public_common clean
	$(MAKE) -C theseus clean
