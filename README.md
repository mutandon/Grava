# Maven Repository

To use this library, edit your `pom.xml` to match the following

```xml
<project ...>
<repositories>
    <repository>
      <id>java.net</id>
      <url>https://raw.github.com/mutandon/Grava/maven/</url>
    </repository>
 </repositories>
...
</project>
```


Then include the artifact as follows

```xml
<project>
...
 <dependencies>
   <dependency>
     <groupId>eu.unitn.disi.db</groupId>
     <artifactId>Grava</artifactId>
     <version>2.2-alpha</version>
     <scope>system</scope>
   </dependency>
 </dependencies>
 ...
</project>
```

