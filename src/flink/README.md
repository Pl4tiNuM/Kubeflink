## Install Java, Maven, etc
 ```sh
 curl -s "https://get.sdkman.io" | bash
 source "$HOME/.sdkman/bin/sdkman-init.sh"
 sdk list java
 sdk install java 17.0.11-tem
 sdk use java 17.0.11-tem

 sdk list maven
 sdk install maven 3.8.6
 sdk use maven 3.8.6
```

## Build Flink
```sh
mvn spotless:apply
mvn clean install -DskipTests -pl flink-dist -am
```


## Build Docker Image
```sh
docker build -t <username>/flink:latest -f Dockerfile .
```