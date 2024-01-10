// SPDX-FileCopyrightText: 2023 Dani Sweet <thorium@djsweet.name>
//
// SPDX-License-Identifier: MIT

package name.djsweet.sidereal.benchmarks

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import name.djsweet.sidereal.*
import org.openjdk.jmh.annotations.*
import net.jqwik.api.*
import java.util.concurrent.TimeUnit

@State(Scope.Benchmark)
class JsonSpec {
    var jsonObject = JsonObject()
    var jsonString: String = ""
    var integerString: String = ""
    var nonIntegerString: String = ""
    var byteBudget = 0

    private val scalarsArbitraries = listOf(
        Arbitraries.just(null),
        Arbitraries.oneOf(listOf(Arbitraries.just(true), Arbitraries.just(false))),
        Arbitraries.doubles(),
        Arbitraries.integers(),
        Arbitraries.strings(),
    )
    private val scalarsAndArraysArbitraries = this.scalarsArbitraries.toMutableList()

    init {
        this.scalarsAndArraysArbitraries.add(
            Arbitraries.oneOf(this.scalarsArbitraries)
                .list().ofMaxSize(10)
                .map {
            JsonArray(it)
        })
    }

    fun jsonObjectWithRemainingRecursion(recursion: Int): Arbitrary<JsonObject> {
        val valuesArbitrary = if (recursion <= 0) {
            Arbitraries.oneOf(this.scalarsAndArraysArbitraries)
        } else {
            val choiceList = this.scalarsAndArraysArbitraries.toMutableList()
            choiceList.add(this.jsonObjectWithRemainingRecursion(recursion - 1))
            Arbitraries.oneOf(choiceList)
        }
        return Arbitraries.strings().list().ofMinSize(4).ofMaxSize(12).flatMap { keys ->
            valuesArbitrary.list().ofSize(keys.size).map { values ->
                val result = JsonObject()
                for (i in keys.indices) {
                    result.put(keys[i], values[i])
                }
                result
            }
        }
    }

    val jsonObjectArbitrary = this.jsonObjectWithRemainingRecursion(2)
    val integerArbitrary: Arbitrary<Int> = Arbitraries.integers().filter { it >= 0 }
    val nonIntegerArbitrary: Arbitrary<String> = Arbitraries.strings().ofMaxLength(16).filter {
        try {
            Integer.parseInt(it)
            false
        } catch (e: NumberFormatException) {
            true
        }
    }

    @Setup(Level.Iteration)
    fun setup() {
        this.byteBudget = maxSafeKeyValueSizeSync()

        this.jsonObject = this.jsonObjectArbitrary.sample()
        this.jsonString = this.jsonObject.encode()
        this.integerString = this.integerArbitrary.sample().toString()
        this.nonIntegerString = this.nonIntegerArbitrary.sample()
    }
}

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations=5)
@Measurement(iterations=30)
class JsonToQueryableDataEncoderBenchmark {
    @Benchmark
    fun validateIntegerString(spec: JsonSpec): Int? {
        return stringAsInt(spec.integerString)
    }

    @Benchmark
    fun validateNonIntegerString(spec: JsonSpec): Int? {
        return stringAsInt(spec.nonIntegerString)
    }

    @Benchmark
    fun convertJsonToQueryableData(spec: JsonSpec): ShareableScalarListQueryableData {
        return encodeJsonToQueryableData(
            spec.jsonObject,
            AcceptAllKeyValueFilterContext(),
            spec.byteBudget,
            128
        )
    }

    @Benchmark
    fun convertJsonToQueryableDataFullParsing(spec: JsonSpec): ShareableScalarListQueryableData {
        val decoded = JsonObject(spec.jsonString)
        return encodeJsonToQueryableData(
            decoded,
            AcceptAllKeyValueFilterContext(),
            spec.byteBudget,
            128
        )
    }
}