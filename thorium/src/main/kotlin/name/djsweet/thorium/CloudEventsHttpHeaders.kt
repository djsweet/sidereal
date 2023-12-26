// SPDX-FileCopyrightText: 2023 Dani Sweet <thorium@djsweet.name>
//
// SPDX-License-Identifier: MIT

package name.djsweet.thorium

import io.vertx.core.http.HttpServerRequest
import io.vertx.core.json.JsonObject
import java.lang.NumberFormatException
import java.util.*

/**
 * Extracts HTTP Headers prefixed with "ce-" and places them in the body of [basis]
 *
 * Note that [basis] is mutated directly, and is only returned as a convenience for
 * operation chaining.
 */
fun fillDataWithCloudEventsHeaders(req: HttpServerRequest, basis: JsonObject): JsonObject {
    val headers = req.headers()
    for (header in headers) {
        // See CloudEvents - Version 1.0.3-wip, Context Attributes, Naming Conventions
        // > CloudEvents attribute names MUST consist of lower-case letters ('a' to 'z') or digits ('0' to '9') from the ASCII character set
        // https://github.com/cloudevents/spec/blob/12336bb/cloudevents/spec.md#naming-conventions
        // Trying to lower-case any characters should thus be a no-op, but we're doing this
        // to especially ensure that the resulting attributes are in compliance with the
        // specification.
        val lowered = header.key.lowercase(Locale.ROOT)
        if (!lowered.startsWith("ce-")) {
            continue
        }
        // ce- is 3 characters; the rest will thus start at offset 3
        val attribute = lowered.substring(3)
        // Despite the existence of Int and Boolean types in the CloudEvents specification,
        // we can't safely try to convert this header into anything other than String.
        // Consider the simple case of
        //
        //    ce-extensionattribute: true
        //
        // This could either be "true": String or true: Boolean. This ambiguity was going
        // to exist no matter what, because HTTP Headers only support String values. So
        // we're going to not attempt any conversion, because it's not possible for us
        // to know, without extra configuration, what type this should be.
        basis.put(attribute, header.value)
    }
    // The HTTP Protocol Binding for CloudEvents - Version 1.0.3-wip, section 3.1.1 states
    // > Note that a ce-datacontenttype HTTP header MUST NOT be present in the message
    // (https://github.com/cloudevents/spec/blob/a64cb14/cloudevents/bindings/http-protocol-binding.md#311-http-content-type)
    // because it is taken from Content-Type instead.
    basis.put("datacontenttype", headers.get("Content-Type"))
    return basis
}