plugins {
    `java-library`
}

dependencies {
    api("io.github.aviatorscript:aviator:5.9.0")

    testImplementation("org.junit.jupiter:junit-jupiter:5.11.4")
    testImplementation("org.assertj:assertj-core:3.27.3")
}

tasks.test {
    useJUnitPlatform()
}
