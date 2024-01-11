// SPDX-FileCopyrightText: 2023 Dani Sweet <sidereal@djsweet.name>
//
// SPDX-License-Identifier: MIT

package name.djsweet.sidereal

const val translationTimerName = "thorium.json.translation"
const val translationTimerDescription = "Time spent converting JSON into Thorium's internal queryable representation"

const val routerTimerName = "thorium.event.routing"
const val routerTimerDescription = "Time spent routing data to active queries"

const val idempotencyKeyCacheSizeName = "thorium.idempotency.key.cache.size"
const val idempotencyKeyCacheSizeDescription = "Number of entries in the idempotency key cache"

const val queryCountName = "thorium.active.queries"
const val queryCountDescription = "Number of queries being serviced by this query router"

const val outstandingEventsCountName = "thorium.outstanding.events"
const val outstandingEventsCountDescription = "Number of events not yet acknowledged by all interested clients"

const val byteBudgetGaugeName = "thorium.data.byte.budget"
const val byteBudgetGaugeDescription = "Maximum length of the combined bytes of any key/value pair ingested"