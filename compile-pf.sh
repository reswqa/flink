cd flink-core-api
mvn clean install -DskipTests -Dfast
cd ../
cd flink-core
mvn clean install -DskipTests -Dfast
cd ../

if [ ! -z "$1" ] && [ "$1" = "-full" ]; then
    cd flink-runtime
    mvn clean install -DskipTests -Dfast
    cd ../
    cd flink-streaming-java
    mvn clean install -DskipTests -Dfast
    cd ../
fi

cd flink-dist
mvn clean install -DskipTests -Dfast
cd ../
cd flink-process-function-parent
mvn clean install -DskipTests -Dfast
