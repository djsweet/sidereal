plugins {
    id("name.djsweet.query.listener.kotlin-library-conventions")
}

val jqwikVersion = "1.7.3"

dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-collections-immutable:0.3.5")
    testImplementation(platform("org.junit:junit-bom:5.9.2"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("net.jqwik:jqwik:${jqwikVersion}")
    compileOnly("org.jetbrains:annotations:23.0.0")
}

tasks.test {
    useJUnitPlatform() {
        includeEngines("jqwik", "junit-jupiter")
    }
    testLogging {
        events("passed", "skipped", "failed")
    }
}