echo "Compiling and assembling application..."
sbt clean package

SPARK_HOME=/home/dmitry/Installations/spark-2.4.5-bin-hadoop2.7

JARFILE=`pwd`/target/scala-2.11/placesuggester_2.11-0.1.jar

${SPARK_HOME}/bin/spark-submit --class HelloWorld --master local $JARFILE