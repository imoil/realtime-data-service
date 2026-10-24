plugins {
    java
}

dependencies {
    implementation("com.google.protobuf:protobuf-java:3.25.5")
    implementation("org.apache.flink:flink-streaming-java:1.20.0")
    implementation("org.apache.flink:flink-clients:1.20.0")

    testImplementation("org.apache.flink:flink-test-utils:1.20.0")
    testImplementation("org.junit.jupiter:junit-jupiter:5.11.4")
    testImplementation("org.assertj:assertj-core:3.27.3")
}

tasks.test {
    useJUnitPlatform()
}
