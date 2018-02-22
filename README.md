# imdb-client
Java client for IMDb open datasets
```xml
<dependency>
  <groupId>com.afrunt.imdb</groupId>
  <artifactId>imdb-client</artifactId>
  <version>1.0</version>
</dependency>
```
Example
```java
new IMDbClient()
    .nameStream()
    .forEach(System.out::println);
```        