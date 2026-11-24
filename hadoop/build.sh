#!/usr/bin/env bash
# Compila as classes MapReduce e gera hadoop/logs-mr.jar (executar no master).
set -euo pipefail
cd "$(dirname "$0")"
rm -rf build logs-mr.jar
mkdir -p build/classes
javac -encoding UTF-8 -cp "$(hadoop classpath)" -d build/classes $(find src -name '*.java')
jar cf logs-mr.jar -C build/classes .
echo "OK: $(pwd)/logs-mr.jar"
jar tf logs-mr.jar | grep '\.class$'
