plugins {
    java
    id("org.springframework.boot") version "4.1.2"
    id("io.spring.dependency-management") version "1.1.7"
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-websocket")
    implementation("org.springframework.kafka:spring-kafka")
    implementation("com.google.protobuf:protobuf-java:3.25.5")
    implementation("org.apache.arrow:arrow-vector:18.2.0")
    implementation("org.apache.arrow:arrow-memory-unsafe:18.2.0")
    implementation("com.clickhouse:clickhouse-jdbc:0.7.2") {
        exclude(group = "org.lz4", module = "lz4-pure-java")
    }

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.boot:spring-boot-starter-webmvc-test")
    testImplementation("org.springframework.kafka:spring-kafka-test")
    testImplementation("org.apache.arrow:arrow-vector:18.2.0")
}

tasks.test {
    useJUnitPlatform()
}
