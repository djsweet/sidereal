plugins {
    id("name.djsweet.thorium.kotlin-application-conventions")
    kotlin("kapt")
}

val operatingSystem = org.gradle.nativeplatform.platform.internal.DefaultNativePlatform.getCurrentOperatingSystem()!!
val picocliVersion = "4.7.3"
val vertxVersion = "4.4.2"
val nettyResolverDnsVersion = "4.1.92.Final"

dependencies {
    implementation("org.apache.commons:commons-text:1.10.0")
    implementation("info.picocli:picocli:${picocliVersion}")
    implementation("io.vertx:vertx-core:${vertxVersion}")
    implementation("io.vertx:vertx-lang-kotlin:${vertxVersion}")
    implementation("io.vertx:vertx-lang-kotlin-coroutines:${vertxVersion}")
    implementation("net.openhft:zero-allocation-hashing:0.16")

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