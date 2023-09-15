package name.djsweet.thorium

const val translationMetricName = "thorium.json.translation"
const val translationMetricDescription = "Time spent converting JSON into Thorium's internal queryable representation"

const val routerMetricName = "thorium.data.response.routing"
const val routerMetricDescription = "Time spent routing data to active queries"

const val idempotencyKeyCacheSizeName = "thorium.idempotency.key.cache.size"
const val idempotencyKeyCacheSizeDescription = "Number of entries in the idempotency key cache"