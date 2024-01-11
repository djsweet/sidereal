// SPDX-FileCopyrightText: 2023 Dani Sweet <sidereal@djsweet.name>
//
// SPDX-License-Identifier: MIT

package name.djsweet.sidereal

const val translationTimerName = "sidereal.json.translation"
const val translationTimerDescription = "Time spent converting JSON into Sidereal Event's internal queryable representation"

const val routerTimerName = "sidereal.event.routing"
const val routerTimerDescription = "Time spent routing data to active queries"

const val idempotencyKeyCacheSizeName = "sidereal.idempotency.key.cache.size"
const val idempotencyKeyCacheSizeDescription = "Number of entries in the idempotency key cache"

const val queryCountName = "sidereal.active.queries"
const val queryCountDescription = "Number of queries being serviced by this query router"

const val outstandingEventsCountName = "sidereal.outstanding.events"
const val outstandingEventsCountDescription = "Number of events not yet acknowledged by all interested clients"

const val byteBudgetGaugeName = "sidereal.data.byte.budget"
const val byteBudgetGaugeDescription = "Maximum length of the combined bytes of any key/value pair ingested"