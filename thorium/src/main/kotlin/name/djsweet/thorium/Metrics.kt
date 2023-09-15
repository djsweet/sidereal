package name.djsweet.thorium

const val translationTimerName = "thorium.json.translation"
const val translationTimerDescription = "Time spent converting JSON into Thorium's internal queryable representation"

const val routerTimerName = "thorium.data.response.routing"
const val routerTimerDescription = "Time spent routing data to active queries"

const val idempotencyKeyCacheSizeName = "thorium.idempotency.key.cache.size"
const val idempotencyKeyCacheSizeDescription = "Number of entries in the idempotency key cache"

const val queryCountName = "thorium.active.query.count"
const val queryCountDescription = "Number of queries being serviced by this query router"