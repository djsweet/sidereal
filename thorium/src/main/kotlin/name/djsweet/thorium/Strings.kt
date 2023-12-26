// SPDX-FileCopyrightText: 2023 Dani Sweet <thorium@djsweet.name>
//
// SPDX-License-Identifier: MIT

package name.djsweet.thorium

import java.net.URLEncoder

fun urlEncode(s: String): String {
    return URLEncoder.encode(s, "UTF-8")
}