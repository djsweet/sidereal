package name.djsweet.thorium

import java.net.URLEncoder

fun urlEncode(s: String): String {
    return URLEncoder.encode(s, "UTF-8")
}