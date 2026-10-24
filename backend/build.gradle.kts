plugins {
    id("java")
}

allprojects {
    repositories {
        mavenCentral()
    }
}

subprojects {
    apply(plugin = "java")

    tasks.withType<JavaCompile> {
        options.release.set(21)
    }

    dependencies {
        add("testRuntimeOnly", "org.junit.platform:junit-platform-launcher")
    }

    tasks.withType<Test> {
        useJUnitPlatform()
        jvmArgs("--add-opens=java.base/java.nio=ALL-UNNAMED")
    }
}
