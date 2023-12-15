import kotlinx.benchmark.gradle.JvmBenchmarkTarget
import kotlinx.benchmark.gradle.benchmark

plugins {
    id("name.djsweet.thorium.kotlin-application-conventions")
    id("org.jetbrains.kotlinx.benchmark") version "0.4.7"
    // allopen is required to properly build the benchmark implementations; JMH assumes that its target classes
    // are open for inheritance, but Kotlin's default is that classes are final.
    // Make sure this stays in sync with the Kotlin plugin version in buildSrc!
    id("org.jetbrains.kotlin.plugin.allopen") version "1.9.21"

    // Native builds are possible with the GraalVM build tools
    id("org.graalvm.buildtools.native") version "0.9.28"
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

val cliktVersion = "4.2.1"
val logbackVersion = "1.3.13"
val micrometerVersion = "1.11.3"
val nettyVersion = "4.1.92.Final"
val slf4jVersion = "2.0.9"
val vertxVersion = "4.4.5"

val netJqwik = "net.jqwik:jqwik:1.7.3"
val nettyBSDTransport = "io.netty:netty-transport-native-kqueue:${nettyVersion}"
val nettyEPollTransport = "io.netty:netty-transport-native-epoll:${nettyVersion}"
val nettyMacOSResolver = "io.netty:netty-resolver-dns-native-macos:${nettyVersion}"
val kotlinXBenchmarkRuntime = "org.jetbrains.kotlinx:kotlinx-benchmark-runtime:0.4.7"
val kotlinXCollectionsImmutable = "org.jetbrains.kotlinx:kotlinx-collections-immutable:0.3.5"

val linuxX86Qualifier = "linux-x86_64"
val linuxArmQualifier = "linux-aarch_64"
val macOSX86Qualifier = "osx-x86_64"
val macOSArmQualifier = "osx-aarch_64"

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

    // Native transports for Netty. Having the "wrong targets" bundled in the build does increase artifact size,
    // but is otherwise harmless, and is portable too!
    // These two specifically suppress an error about Netty not being able to find
    // io.netty.resolver.dns.macos.MacOSDnsServerAddressStreamProvider.
    implementation("${nettyMacOSResolver}:$macOSX86Qualifier")
    implementation("${nettyMacOSResolver}:$macOSArmQualifier")
    // These are native event loop transports.
    implementation("${nettyEPollTransport}:$linuxX86Qualifier")
    implementation("${nettyEPollTransport}:$linuxArmQualifier")
    implementation("${nettyBSDTransport}:$macOSX86Qualifier")
    implementation("${nettyBSDTransport}:$macOSArmQualifier")

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
    // are here, so I don't need to do this."
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

tasks.named("run") {
    // Gradle 8.4 very often considers the "run" task up-to-date, seemingly as a consequence of the GraalVM
    // plugin being in use. "run" is never up-to-date; we always need to run it when requested. So we'll
    // force the issue here.
    outputs.upToDateWhen { false }
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

val operatingSystem = org.gradle.nativeplatform.platform.internal.DefaultNativePlatform.getCurrentOperatingSystem()!!

graalvmNative {
    binaries {
        all {
            buildArgs.addAll(
                "--no-fallback",
                // native-image 21.0.1 2023-10-17 on macOS 13.6 segmentation faults at -O2, but not -O1 or -O3.
                // We'll look into this.
                "-O3",
            )

            // The G1 garbage collector is the garbage collector you want -- it's designed for long-running processes,
            // is multithreaded, and is capable of high throughput with OK latency. But it's also only available on
            // Linux, and only if you're not using GraalVM Community Edition.
            //
            // We can be firm about building with the non-community-edition GraalVM tooling, but can't be so firm
            // that this should only run on Linux. Admittedly, this check only works when building directly on Linux,
            // but as of this writing, cross-compilation isn't supported by the GraalVM Gradle Plugin anyway.
            if (operatingSystem.isLinux) {
                buildArgs.add("--gc=G1")
            }
        }

        named("test") {
            // These are all necessary to get the tests to build natively -- without them, we wouldn't be able
            // to run ./gradlew nativeTest
            buildArgs.addAll(
                "--initialize-at-build-time=net.jqwik.api.AfterFailureMode",
                "--initialize-at-build-time=net.jqwik.api.EdgeCasesMode",
                "--initialize-at-build-time=net.jqwik.api.FixedSeedMode",
                "--initialize-at-build-time=net.jqwik.api.GenerationMode",
                "--initialize-at-build-time=net.jqwik.api.ShrinkingMode",
                "--initialize-at-build-time=net.jqwik.engine.JqwikProperties",
                "--initialize-at-build-time=net.jqwik.engine.JqwikTestEngine",
                "--initialize-at-build-time=net.jqwik.engine.DefaultJqwikConfiguration",
                "--initialize-at-build-time=net.jqwik.engine.DefaultJqwikConfiguration\$2",
                "--initialize-at-build-time=net.jqwik.engine.PropertyAttributesDefaults\$1",
                "--initialize-at-build-time=net.jqwik.engine.descriptor.ContainerClassDescriptor",
                "--initialize-at-build-time=net.jqwik.engine.descriptor.JqwikEngineDescriptor",
                "--initialize-at-build-time=net.jqwik.engine.descriptor.PropertyConfiguration",
                "--initialize-at-build-time=net.jqwik.engine.descriptor.PropertyMethodDescriptor",
                "--initialize-at-build-time=net.jqwik.engine.discovery.DefaultPropertyAttributes",
                "--initialize-at-build-time=net.jqwik.engine.execution.GenerationInfo",
                "--initialize-at-build-time=net.jqwik.engine.execution.lifecycle.LifecycleHooksRegistry",
                "--initialize-at-build-time=net.jqwik.engine.recording.TestRunData",
                "--initialize-at-build-time=net.jqwik.engine.recording.TestRunDatabase"
            )
        }
    }
}
