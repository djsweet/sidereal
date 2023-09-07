@file:Suppress("UnstableApiUsage")

import kotlinx.benchmark.gradle.JvmBenchmarkTarget
import kotlinx.benchmark.gradle.benchmark

plugins {
    id("name.djsweet.thorium.kotlin-application-conventions")
    id("org.jetbrains.kotlinx.benchmark") version "0.4.7"
    id("org.jetbrains.kotlin.plugin.allopen") version "1.8.21"
    kotlin("kapt")
}

allOpen {
    annotation("org.openjdk.jmh.annotations.State")
    annotation("org.openjdk.jmh.annotations.BenchmarkMode")
}

sourceSets {
    this.create("benchmarks").java {
        srcDir("src/benchmarks/kotlin")
    }
}

val operatingSystem = org.gradle.nativeplatform.platform.internal.DefaultNativePlatform.getCurrentOperatingSystem()!!

val picocliVersion = "4.7.3"
val vertxVersion = "4.4.2"
val nettyResolverDnsVersion = "4.1.92.Final"
val netJqwik = "net.jqwik:jqwik:1.7.3"

val benchmarksImplementation: Configuration = configurations.getAt("benchmarksImplementation")

dependencies {
    implementation("org.apache.commons:commons-text:1.10.0")
    implementation("info.picocli:picocli:${picocliVersion}")
    implementation("io.vertx:vertx-core:${vertxVersion}")
    implementation("io.vertx:vertx-lang-kotlin:${vertxVersion}")
    implementation("io.vertx:vertx-lang-kotlin-coroutines:${vertxVersion}")

    if (operatingSystem.isMacOsX) {
        // io.netty.resolver.dns.DnsServerAddressStreamProviders prints a warning on macOS about how it
        // can't resolve this package on its classpath by default.
        val classifier = if (System.getProperty("os.arch") == "aarch64") {
            "osx-aarch_64"
        } else {
            "osx-x86_64"
        }
        implementation(
            "io.netty:netty-resolver-dns-native-macos:$nettyResolverDnsVersion:$classifier"
        )
    }

    implementation(project(":query-tree"))
    annotationProcessor("info.picocli:picocli-codegen:${picocliVersion}")
    kapt("info.picocli:picocli-codegen:${picocliVersion}")

    testImplementation(platform("org.junit:junit-bom:5.9.2"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.9.2")
    testImplementation(netJqwik)

    benchmarksImplementation(project(mapOf("path" to ":thorium")))
    benchmarksImplementation("org.jetbrains.kotlinx:kotlinx-benchmark-runtime:0.4.7")
    benchmarksImplementation(netJqwik)
    benchmarksImplementation(sourceSets.test.get().output + sourceSets.test.get().runtimeClasspath)
}

kapt {
    arguments {
        arg("project", "${project.group}/${project.name}")
    }
}

application {
    // Define the main class for the application.
    mainClass.set("name.djsweet.thorium.AppKt")
}

tasks.test {
    // As silly as this looks, Gradle sometimes gets very confused about
    // whether it needs to run the `test` task, because the artifacts
    // of the last test run get dumped, and it thinks "oh well these artifacts
    // are here so I don't need to do this."
    outputs.upToDateWhen { false }
    useJUnitPlatform {
        includeEngines("jqwik", "junit-jupiter")
    }
    testLogging {
        events("passed", "skipped", "failed")
    }
    minHeapSize = "512m"
    maxHeapSize = "2048m"
}

benchmark {
    configurations {
        create("jsonDecoding") {
            include("JsonToQueryableDataEncoderBenchmark\\..*")
        }
    }
    targets {
        register("benchmarks") {
            this as JvmBenchmarkTarget
            jmhVersion = "1.21"
        }
    }
}
