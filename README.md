this has a dependency on log-synth - https://github.com/tdunning/log-synth

You should compile log-synth and install it to your local repository:

```
 mvn clean package install:install-file -Dfile=target/log-synth-0.1-SNAPSHOT.jar -DgroupId=com.mapr.synth -DartifactId=log-synth -Dversion=0.1-SNAPSHOT -Dpackaging=jar
```

Run it as follows (topic is required but can be anything, since it's not used):

```
java -jar target/synth-avro-1.0-SNAPSHOT-jar-with-dependencies.jar -t foo -s /tmp/songstream.json -c 100
```

Usage:

```
$ java -jar target/synth-avro-1.0-SNAPSHOT-jar-with-dependencies.jar
usage: pubsubgen
 -c,--count <arg>    Number of messages to generate.
 -f,--forever        Emit messages forever.
 -r,--realtime       Set this if you want messages emitted in real-time.
 -s,--schema <arg>   Path to schema file.
 -t,--topic <arg>    Pub/sub topic to use (must already exist).
```
