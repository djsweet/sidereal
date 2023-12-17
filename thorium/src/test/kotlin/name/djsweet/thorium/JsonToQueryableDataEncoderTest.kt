package name.djsweet.thorium

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import net.jqwik.api.*
import net.jqwik.api.lifecycle.BeforeContainer
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.*

class JsonToQueryableDataEncoderTest {
    companion object {
        private var byteBudget = 0

        // Note that jqwik uses a different lifecycle than junit:
        // https://jqwik.net/docs/current/user-guide.html#simple-property-lifecycle
        @JvmStatic
        @BeforeContainer
        fun setup() {
            this.byteBudget = maxSafeKeyValueSizeSync()
        }
    }

    private val keyLength = 12
    private val stringLength = 64
    private val scalarArbitraries = listOf(
        Arbitraries.just(null),
        Arbitraries.oneOf(listOf(Arbitraries.just(true), Arbitraries.just(false))),
        Arbitraries.doubles(),
        // It's unclear from the Vertx documentation whether all unspecified number types come back
        // as a Double with `getValue`, so we're going to test the hypothesis that `getValue` will give
        // you back a Double even when given a string integer.
        Arbitraries.integers(),
        Arbitraries.strings().ofMaxLength(this.stringLength)
    )
    private val scalarAndArrayArbitraries = this.scalarArbitraries.toMutableList()

    init {
        this.scalarAndArrayArbitraries.add(Arbitraries.oneOf(this.scalarArbitraries).list().map {
            JsonArray(it)
        })
    }

    private fun generateJsonObject(subObjectDepth: Int): Arbitrary<JsonObject> {
        val values = if (subObjectDepth > 0) {
            val choiceList = this.scalarAndArrayArbitraries.toMutableList()
            choiceList.add(this.generateJsonObject(subObjectDepth - 1))
            Arbitraries.oneOf(choiceList)
        } else {
            Arbitraries.oneOf(this.scalarAndArrayArbitraries)
        }
        val keyValuePairs = Arbitraries.strings().ofMaxLength(this.keyLength)
            .flatMap { key -> values.map { value -> key to value } }
            .list().ofMinSize(0).ofMaxSize(4)
        return keyValuePairs.map { pairs ->
            val result = JsonObject()
            for ((key, value) in pairs) {
                result.put(key, value)
            }
            result
        }
    }

    @Provide
    fun generateJsonObjectDepth3(): Arbitrary<JsonObject> {
        return this.generateJsonObject(3)
    }

    @Provide
    fun generateJsonObjectDepth5(): Arbitrary<JsonObject> {
        return this.generateJsonObject(5)
    }

    private fun ensureKeyValueEncodesJsonScalarKeyValue(key: ByteArray, value: ByteArray, obj: JsonObject) {
        val keyDecoder = Radix64LowLevelDecoder(key)
        var curObj = obj
        var lastKey: String? = null
        var encodedKeyDepth = 0
        var jsonKeyDepth = 0
        while (keyDecoder.withByteArray {
            val curKey = convertByteArrayToString(it)
            lastKey = curKey
            encodedKeyDepth++
            when (val resultingValue = curObj.getValue(curKey)) {
                is JsonObject -> {
                    jsonKeyDepth++
                    curObj = resultingValue
                }
            }
        }) {
            // Do nothing, the receiver above does all the work
        }
        assertNotNull(lastKey)
        assertEquals(encodedKeyDepth - 1, jsonKeyDepth)

        val testValue = curObj.getValue(lastKey)
        val valueDecoder = Radix64JsonDecoder(value)
        do {
            if (valueDecoder.withNull {
                assertNull(testValue)
            }) {
                break
            }
            if (valueDecoder.withBoolean {
                assertEquals(it, testValue)
            }) {
                break
            }
            if (valueDecoder.withNumber {
                assertTrue(testValue is Number)
                testValue as Number
                assertEquals(it, testValue.toDouble())
            }) {
                break
            }
            if (valueDecoder.withString { s, full ->
                assertTrue(full)
                assertEquals(s, testValue)
            }) {
                break
            }
            fail<Unit>("Could not decode value for testing")
        } while (false)
    }

    private fun ensureKeyValueEncodesJsonArrayKeyValue(key: ByteArray, values: List<ByteArray>, obj: JsonObject) {
        val keyDecoder = Radix64LowLevelDecoder(key)
        var curObj = obj
        var lastKey: String? = null
        var encodedKeyDepth = 0
        var jsonKeyDepth = 0
        while (keyDecoder.withByteArray {
                val curKey = convertByteArrayToString(it)
                lastKey = curKey
                encodedKeyDepth++
                when (val resultingValue = curObj.getValue(curKey)) {
                    is JsonObject -> {
                        jsonKeyDepth++
                        curObj = resultingValue
                    }
                }
            }) {
            // Do nothing, the receiver above does all the work
        }
        assertNotNull(lastKey)
        assertEquals(encodedKeyDepth - 1, jsonKeyDepth)

        val testValues = curObj.getValue(lastKey)
        if (testValues !is JsonArray) {
            fail<String>("JSON value is not array $testValues")
            return
        }
        assertEquals(values.size, testValues.size())
        for (i in values.indices) {
            val value = values[i]
            val testValue = testValues.getValue(i)
            val valueDecoder = Radix64JsonDecoder(value)

            do {
                if (valueDecoder.withNull {
                        assertNull(testValue)
                    }) {
                    break
                }
                if (valueDecoder.withBoolean {
                        assertEquals(it, testValue)
                    }) {
                    break
                }
                if (valueDecoder.withNumber {
                        assertTrue(testValue is Number)
                        testValue as Number
                        assertEquals(it, testValue.toDouble())
                    }) {
                    break
                }
                if (valueDecoder.withString { s, full ->
                        assertTrue(full)
                        assertEquals(s, testValue)
                    }) {
                    break
                }
                fail<Unit>("Could not decode value for testing")
            } while (false)
        }
    }

    private fun ensureTriesEncodeJsonObject(tries: ShareableScalarListQueryableData, obj: JsonObject) {
        val (scalars, arrays) = tries
        scalars.trie.visitUnsafeSharedKey { (key, value) ->
            ensureKeyValueEncodesJsonScalarKeyValue(key, value, obj)
        }
        arrays.trie.visitAscendingUnsafeSharedKey { (key, value) ->
            ensureKeyValueEncodesJsonArrayKeyValue(key, value, obj)
        }
    }

    @Property
    fun recursiveOnlyJsonEncoding(
        @ForAll @From("generateJsonObjectDepth3") obj: JsonObject
    ) {
        val tries = encodeJsonToQueryableData(obj, AcceptAllKeyValueFilterContext(), byteBudget, 6)
        this.ensureTriesEncodeJsonObject(tries, obj)
    }

    @Property
    fun iterativeOnlyJsonEncoding(
        @ForAll @From("generateJsonObjectDepth3") obj: JsonObject
    ) {
        val tries = encodeJsonToQueryableData(obj, AcceptAllKeyValueFilterContext(), byteBudget, 0)
        this.ensureTriesEncodeJsonObject(tries, obj)
    }

    @Property
    fun hybridJsonEncoding(
        @ForAll @From("generateJsonObjectDepth5") obj: JsonObject
    ) {
        val tries = encodeJsonToQueryableData(obj, AcceptAllKeyValueFilterContext(), byteBudget, 3)
        this.ensureTriesEncodeJsonObject(tries, obj)
    }
}