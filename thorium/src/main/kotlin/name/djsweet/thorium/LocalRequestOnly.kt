package name.djsweet.thorium

import io.vertx.core.eventbus.DeliveryOptions

val localRequestOptions: DeliveryOptions = DeliveryOptions().setLocalOnly(true)