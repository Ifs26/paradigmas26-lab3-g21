# Variables
JAVA_HOME := /usr/lib/jvm/java-17-openjdk-amd64
PATH      := $(JAVA_HOME)/bin:$(PATH)
SBT_OPTS  := --add-exports=java.base/sun.nio.ch=ALL-UNNAMED
SBT       := sbt

export JAVA_HOME
export PATH
export SBT_OPTS

.DEFAULT_GOAL := run

# --- Comandos previos ---

check-java:
	@echo "Verificando Java 17..."
	@java -version 2>&1 | grep -q "17" || (echo "ERROR: Java 17 no encontrado en $(JAVA_HOME)" && exit 1)

setup: check-java
	@echo "Setup listo."

compile: check-java
	$(SBT) compile

# --- Target principal ---

run: setup compile
	$(SBT) run

mock: setup compile
	REDDIT_BASE_URL=http://localhost:8123 $(SBT) run

.PHONY: check-java setup compile run