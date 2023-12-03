package name.djsweet.thorium

import java.net.URLEncoder

internal const val buildOnlyKvpSafetyFactorPropertyName = "name.djsweet.thorium.compiletime.kvp.safetyfactor"

fun urlEncode(s: String): String {
    return URLEncoder.encode(s, "UTF-8")
}