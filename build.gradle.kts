plugins {
    kotlin("jvm") version "1.9.23"
}

group = "com.melexis.tema"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

val exposedVersion: String by project

dependencies {
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-cbor:2.17.1")

    implementation("org.eclipse.paho:org.eclipse.paho.mqttv5.client:1.2.5")

    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.1")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core-jvm:1.8.1")

    implementation("org.xerial:sqlite-jdbc:3.46.0.0")

    implementation("org.jetbrains.exposed:exposed-core:$exposedVersion")
    implementation("org.jetbrains.exposed:exposed-dao:$exposedVersion")
    implementation("org.jetbrains.exposed:exposed-jdbc:$exposedVersion")

    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(21)
}
