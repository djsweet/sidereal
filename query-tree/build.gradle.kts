// SPDX-FileCopyrightText: 2023 Dani Sweet <sidereal@djsweet.name>
//
// SPDX-License-Identifier: MIT

@file:Suppress("UnstableApiUsage")

import kotlinx.benchmark.gradle.*

plugins {
    id("name.djsweet.sidereal.kotlin-library-conventions")
    id("org.jetbrains.kotlinx.benchmark") version "0.4.7"
    // allopen is required to properly build the benchmark implementations; JMH assumes that its target classes
    // are open for inheritance, but Kotlin's default is that classes are final.
    // Make sure this stays in sync with the Kotlin plugin version in buildSrc!
    id("org.jetbrains.kotlin.plugin.allopen") version "1.9.21"
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

val benchmarksImplementation: Configuration = configurations.getAt("benchmarksImplementation")

val kotlinXCollectionsImmutable = "org.jetbrains.kotlinx:kotlinx-collections-immutable:0.3.5"
val netJqwik = "net.jqwik:jqwik:1.7.3"

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.9.2"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.9.2")
    testImplementation(netJqwik)
    testImplementation(kotlinXCollectionsImmutable)
    compileOnly("org.jetbrains:annotations:24.0.1")
    benchmarksImplementation(project(mapOf("path" to ":query-tree")))
    benchmarksImplementation("org.jetbrains.kotlinx:kotlinx-benchmark-runtime:0.4.7")
    benchmarksImplementation(netJqwik)
    benchmarksImplementation(kotlinXCollectionsImmutable)
    benchmarksImplementation(sourceSets.test.get().output + sourceSets.test.get().runtimeClasspath)
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

    // jvmArgs = listOf("-XX:MaxPermSize=512m")
}

benchmark {
    configurations {
        create("arrayUtils") {
            include("ArrayUtilsBenchmark\\..*")
        }
        create("trie") {
            include("QPTrieBenchmark\\..*")
        }
        create("triePoint") {
            include("QPTrieBenchmark\\.point.*")
        }
        create("trieIterator") {
            include("QPTrieBenchmark\\.iterator.*")
        }
        create("trieVisit") {
            include("QPTrieBenchmark\\.visit.*")
        }
        create("identitySet") {
            include("IdentitySetBenchmark\\..*")
        }
        create("identitySetPoint") {
            include("IdentitySetBenchmark\\.point.*")
        }
        create("identitySetIterator") {
            include("IdentitySetBenchmark\\.iterator.*")
        }
        create("identitySetVisit") {
            include("IdentitySetBenchmark\\.visit.*")
        }
        create("persistentSet") {
            include("PersistentSetBenchmark\\..*")
        }
        create("persistentSetPoint") {
            include("PersistentSetBenchmark\\.point.*")
        }
        create("persistentSetIterator") {
            include("PersistentSetBenchmark\\.iterator.*")
        }
        create("persistentMap") {
            include("PersistentMapBenchmark\\..*")
        }
        create("persistentMapPoint") {
            include("PersistentMapBenchmark\\.point.*")
        }
        create("persistentMapIterator") {
            include("PersistentMapBenchmark\\.iterator.*")
        }
        create("queryTree") {
            include("QueryTreeBenchmark\\..*")
        }
        create("queryTreeOnly") {
            include("QueryTreeBenchmark\\.point01.*")
        }
        create("queryTreeVisitArrayOnly") {
            include("QueryTreeBenchmark\\.point04VisitUsingTreeIntoArray")
        }
    }
    targets {
        register("benchmarks") {
            this as JvmBenchmarkTarget
            jmhVersion = "1.21"
        }
    }
}