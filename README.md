# LevelDB MCPE in Java

This project is a fork of https://github.com/pcmind/leveldb aiming to implement the changes made
in https://github.com/Mojang/leveldb-mcpe/ where relevant to allow the library to read MCPE.

For more information see the original repository on use cases / API usage.

Building
--------

**Requirements**

- Git
- Java 11 or higher
- Maven

**Steps**

1. Clone this repository via `git clone git://github.com/HiveGamesOSS/leveldb-mcpe-java.git`.
2. Build the project via `mvn clean install`.
3. Obtain the library from `target/` folder.

Library Usage
--------

You can use the following in your maven pom.xml:

```xml

<dependency>
    <groupId>com.hivemc.leveldb</groupId>
    <artifactId>leveldb</artifactId>
    <version>1.0.0</version>
</dependency>
```

```xml

<dependency>
    <groupId>com.hivemc.leveldb</groupId>
    <artifactId>leveldb-api</artifactId>
    <version>1.0.0</version>
</dependency>
```

This library is aimed as a drop in replacement to the original fork https://github.com/pcmind/leveldb.

License
--------

Details of the LICENSE can be found in the license.txt, this fork maintains the original license for all code and
modifications.
