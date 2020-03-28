echo "Compiling and assembling application..."
sbt clean package

# Directory where spark-submit is defined
# Install spark from https://spark.apache.org/downloads.html
SPARK_HOME=/home/dmitry/Installations/spark-2.4.5-bin-hadoop2.7

# JAR containing a simple hello world
JARFILE=`pwd`/target/scala-2.11/placesuggester_2.11-0.1.jar

# Run it locally
${SPARK_HOME}/bin/spark-submit --class HelloWorld --master local $JARFILE