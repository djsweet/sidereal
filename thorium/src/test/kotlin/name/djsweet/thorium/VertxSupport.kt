// SPDX-FileCopyrightText: 2023 Dani Sweet <thorium@djsweet.name>
//
// SPDX-License-Identifier: MIT

package name.djsweet.thorium

import io.vertx.core.Vertx
import kotlinx.coroutines.runBlocking

fun<T> withVertx(consumer: (Vertx) -> T): T {
    val vertx = Vertx.vertx()
    registerMessageCodecs(vertx)
    try {
        return consumer(vertx)
    } finally {
        runBlocking { vertx.close() }
    }
}

fun<T> withVertxAsync(consumer: suspend (Vertx) -> T): T {
    return withVertx { vertx ->
        runBlocking { consumer(vertx) }
    }
}