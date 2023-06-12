cd flink-core-api
mvn clean install -DskipTests -Dfast
cd ../
cd flink-core
mvn clean install -DskipTests -Dfast
cd ../
cd flink-dist
mvn clean install -DskipTests -Dfast
cd ../
cd flink-process-function-parent
mvn clean install -DskipTests -Dfast
