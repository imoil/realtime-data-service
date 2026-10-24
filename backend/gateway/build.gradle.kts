plugins {
    java
    id("org.springframework.boot") version "3.4.3"
    id("io.spring.dependency-management") version "1.1.7"
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.apache.arrow:arrow-vector:18.2.0")
    implementation("org.apache.arrow:arrow-memory-netty:18.2.0")
    implementation("com.clickhouse:clickhouse-jdbc:0.7.2")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.apache.arrow:arrow-vector:18.2.0")
}

tasks.test {
    useJUnitPlatform()
}
