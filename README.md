[![Build Status](https://travis-ci.org/afrunt/imdb-client.svg?branch=master)](https://travis-ci.org/afrunt/imdb-client)
# imdb-client
Java client for [IMDb Datasets](http://www.imdb.com/interfaces/)
```xml
<dependency>
  <groupId>com.afrunt.imdb</groupId>
  <artifactId>imdb-client</artifactId>
  <version>1.0.1</version>
</dependency>
```
Example
```java
new IMDbClient()
    .nameStream()
    .forEach(System.out::println);
```        