this has a dependency on log-synth - https://github.com/tdunning/log-synth

You should compile log-synth and install it to your local repository:

```
 mvn clean package install:install-file -Dfile=target/log-synth-0.1-SNAPSHOT.jar -DgroupId=com.mapr.synth -DartifactId=log-synth -Dversion=0.1-SNAPSHOT -Dpackaging=jar
```
