# user-guide-trigger

## description
开发的trigger测试代码

## dependency
pom.xml
```xml
        <dependency>
            <groupId>org.apache.iotdb</groupId>
            <artifactId>iotdb-server</artifactId>
            <version>1.3.0-SNAPSHOT</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.iotdb</groupId>
            <artifactId>iotdb-session</artifactId>
            <version>1.3.0-SNAPSHOT</version>
            <scope>provided</scope>
        </dependency>

```

## 使用
```shell
# build
mvn clean package

find . -name '*.jar'

```

## 其他
以下工程是不必要的。分别是为了测试mqtt,测试trigger性能辅助代码。
```xml
        <module>client-support</module>
        <module>perf-trigger</module>
        <module>mqtt</module>
```
