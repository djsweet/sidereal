import kotlinx.benchmark.gradle.JvmBenchmarkTarget
import kotlinx.benchmark.gradle.benchmark

plugins {
    id("name.djsweet.thorium.kotlin-application-conventions")
    id("org.jetbrains.kotlinx.benchmark") version "0.4.7"
    id("org.jetbrains.kotlin.plugin.allopen") version "1.8.21"
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

val cliktVersion = "4.2.1"
val logbackVersion = "1.3.13"
val micrometerVersion = "1.11.3"
val nettyResolverDnsVersion = "4.1.92.Final"
val slf4jVersion = "2.0.9"
val vertxVersion = "4.4.5"
val netJqwik = "net.jqwik:jqwik:1.7.3"
val kotlinXBenchmarkRuntime = "org.jetbrains.kotlinx:kotlinx-benchmark-runtime:0.4.7"
val kotlinXCollectionsImmutable = "org.jetbrains.kotlinx:kotlinx-collections-immutable:0.3.5"

val benchmarksImplementation: Configuration = configurations.getAt("benchmarksImplementation")

dependencies {
    implementation("com.github.ajalt.clikt:clikt:${cliktVersion}")
    implementation("org.slf4j:slf4j-api:${slf4jVersion}")
    implementation("ch.qos.logback:logback-core:${logbackVersion}")
    implementation("ch.qos.logback:logback-classic:${logbackVersion}")
    implementation("io.vertx:vertx-core:${vertxVersion}")
    implementation("io.vertx:vertx-lang-kotlin:${vertxVersion}")
    implementation("io.vertx:vertx-lang-kotlin-coroutines:${vertxVersion}")
    implementation("io.micrometer:micrometer-registry-prometheus:${micrometerVersion}")
    implementation(kotlinXCollectionsImmutable)

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

    testImplementation(platform("org.junit:junit-bom:5.9.2"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.9.2")
    testImplementation(netJqwik)

    benchmarksImplementation(project(mapOf("path" to ":thorium")))
    benchmarksImplementation(kotlinXBenchmarkRuntime)
    benchmarksImplementation(netJqwik)
    benchmarksImplementation(sourceSets.test.get().output + sourceSets.test.get().runtimeClasspath)
}

application {
    // Define the main class for the application.
    mainClass.set("name.djsweet.thorium.AppKt")
}

tasks.named("compileKotlin", org.jetbrains.kotlin.gradle.tasks.KotlinCompilationTask::class.java) {
    compilerOptions {
        freeCompilerArgs.add("-Xno-call-assertions")
        freeCompilerArgs.add("-Xno-param-assertions")
        freeCompilerArgs.add("-Xno-receiver-assertions")
    }
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

tasks.jar {
    dependsOn(":query-tree:jar")
    manifest.attributes["Main-Class"] = "name.djsweet.thorium.AppKt"
    val dependencies = configurations
        .runtimeClasspath
        .get()
        .map(::zipTree)
    from(dependencies)
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
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
