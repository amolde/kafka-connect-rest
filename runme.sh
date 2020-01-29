jenv global
# 1.8.0.232
jenv version
# 1.8.0.232 (set by /Users/adeshmukh/.jenv/version)
mvn clean install -DskipTests
cp kafka-connect-rest-plugin/target/kafka-connect-rest-plugin-1.0.3-shaded.jar ~/ckg/cp-kafka-connect/jars/.
cp kafka-connect-transform-from-json/kafka-connect-transform-from-json-plugin/target/kafka-connect-transform-from-json-plugin-1.0.3-shaded.jar ~/ckg/cp-kafka-connect/jars/.
cp kafka-connect-transform-add-headers/target/kafka-connect-transform-add-headers-1.0.3-shaded.jar ~/ckg/cp-kafka-connect/jars/.
cp kafka-connect-transform-velocity-eval/target/kafka-connect-transform-velocity-eval-1.0.3-shaded.jar ~/ckg/cp-kafka-connect/jars/.
cd ~/ckg/cp-kafka-connect/
./runme.sh
