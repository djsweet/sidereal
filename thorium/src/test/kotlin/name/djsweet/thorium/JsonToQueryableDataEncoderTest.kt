package name.djsweet.thorium

import io.vertx.core.json.JsonObject
import name.djsweet.query.tree.QPTrie
import net.jqwik.api.*
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.*
import java.nio.charset.Charset

class JsonToQueryableDataEncoderTest {
    companion object {
        private var maxKeySize = 0
        @JvmStatic
        @BeforeAll
        fun setup() {
            this.maxKeySize = maxSafeKeyValueSizeSync()
        }
    }

    private val keyLength = 16
    private val stringLength = 128
    private val byteBudget = this.stringLength * 8

    private fun generateJsonObject(subObjectDepth: Int): Arbitrary<JsonObject> {
        val strings = Arbitraries.strings().ofMaxLength(this.stringLength)
        val values = if (subObjectDepth > 0) {
            Arbitraries.oneOf(listOf(
                Arbitraries.just(null),
                Arbitraries.oneOf(listOf(Arbitraries.just(true), Arbitraries.just(false))),
                Arbitraries.doubles(),
                strings,
                this.generateJsonObject(subObjectDepth - 1)
            ))
        } else {
            Arbitraries.oneOf(listOf(
                Arbitraries.just(null),
                Arbitraries.oneOf(listOf(Arbitraries.just(true), Arbitraries.just(false))),
                Arbitraries.doubles(),
                strings
            ))
        }
        val keyValuePairs = Arbitraries.strings().ofMaxLength(this.keyLength)
            .flatMap { key -> values.map { value -> key to value } }
            .list().ofMinSize(0).ofMaxSize(8)
        return keyValuePairs.map { pairs ->
            val result = JsonObject()
            for ((key, value) in pairs) {
                result.put(key, value)
            }
            result
        }
    }

    @Provide
    fun generateJsonObjectDepth5(): Arbitrary<JsonObject> {
        return this.generateJsonObject(5)
    }

    @Provide
    fun generateJsonObjectDepth8(): Arbitrary<JsonObject> {
        return this.generateJsonObject(8)
    }

    private fun ensureKeyValueEncodesJsonKeyValue(key: ByteArray, value: ByteArray, obj: JsonObject) {
        val keyDecoder = Radix64LowLevelDecoder(key)
        var curObj = obj
        var lastKey: String? = null
        var encodedKeyDepth = 0
        var jsonKeyDepth = 0
        while (keyDecoder.withByteArray {
            val curKey = it.toString(Charset.forName("utf-8"))
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
                assertEquals(it, testValue)
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

    private fun ensureTrieEncodesJsonObject(trie: QPTrie<ByteArray>, obj: JsonObject) {
        trie.visitUnsafeSharedKey { (key, value) ->
            ensureKeyValueEncodesJsonKeyValue(key, value, obj)
        }
    }

    @Property
    fun recursiveOnlyJsonEncoding(
        @ForAll @From("generateJsonObjectDepth5") obj: JsonObject
    ) {
        val (trie) = encodeJsonToQueryableData(obj, this.byteBudget, 6)
        this.ensureTrieEncodesJsonObject(trie, obj)
    }

    @Property
    fun iterativeOnlyJsonEncoding(
        @ForAll @From("generateJsonObjectDepth5") obj: JsonObject
    ) {
        val (trie) = encodeJsonToQueryableData(obj, this.byteBudget, 0)
        this.ensureTrieEncodesJsonObject(trie, obj)
    }

    @Property
    fun hybridJsonEncoding(
        @ForAll @From("generateJsonObjectDepth8") obj: JsonObject
    ) {
        val (trie) = encodeJsonToQueryableData(obj, this.byteBudget, 3)
        this.ensureTrieEncodesJsonObject(trie, obj)
    }
}