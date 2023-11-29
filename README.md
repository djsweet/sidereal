# Thorium - Reactive queries over streaming CloudEvents

Thorium is a standalone HTTP server providing reactive queries over
streaming [CloudEvents](https://cloudevents.io). When used in conjunction with
Change Data Capture, it can turn any database into a real-time database.

A query in Thorium is a logical conjunction (`x AND y AND z` ...) of key/value
terms (`key1=value1 AND key2=value2 AND key3=value3` ...). Queries are
internally indexed by their terms (e.g. `key2=value2` is an index entry), as
an inversion to the usual practice of data being indexed by their fields. This
query indexing allows Thorium to scale efficiently to thousands of concurrent
queries while still ingesting tens of thousands of events per second.

## Running from source

You'll need a Java Development Kit (JDK) supporting Java 17 or greater.
Thorium builds and runs correctly using any of

- [Amazon Corretto 17](https://aws.amazon.com/corretto/)
- [OpenJDK 18](https://jdk.java.net/archive/)
- [GraalVM 21](https://www.graalvm.org/downloads/)

Thorium is compiled using [Gradle](https://gradle.org), and this repository
includes the
[Gradle Wrapper](https://docs.gradle.org/current/userguide/gradle_wrapper.html),
so only a suitable JDK needs to be explicitly installed to build this project.

Run the server with the command

```shell
./gradlew run --args="serve"
```

This will start the Thorium Server listening for HTTP connections over TCP
port 8232. Additional command-line flags can be passed in the string
argument to Gradle's `--args` argument. Run

```shell
./gradlew run --args="serve --help"
```

for a list of these flags, or see the
[Server Configuration](#server-configuration) section for more details.

## Building a runnable JAR

Gradle can build directly-runnable [JAR Files](https://docs.oracle.com/javase/tutorial/deployment/jar/basicsindex.html)
by running

```shell
./gradlew jar
```

The resulting JAR will be saved to `./thorium/build/libs/thorium.jar`,
and run directly using

```shell
java -jar ./thorium/build/libs/thorium.jar serve
```

## Sending events

Events are sent to named "channels" by requesting an HTTP POST containing
[JSON](https://www.json.org/json-en.html) against the path

```
/channels/CHANNEL-NAME
```

where `CHANNEL-NAME` is a
[percent-encoded](https://datatracker.ietf.org/doc/html/rfc3986#section-2.1)
string containing at least 1 character. For example, a `CHANNEL-NAME` of
"with/slash" would be encoded as

```
/channels/with%2fslash
```

The name `meta` is used internally to report query registration, and does not
receive events from external sources. Attempts to send an event to the `meta`
channel will result in an HTTP 403 response.

The `Content-Type` of the data sent to this endpoint can be one of
- `application/json`
- `application/cloudevents+json`
- `application/cloudevents-batch+json`

with the contents interpreted according to the
[HTTP Protocol Binding](https://github.com/cloudevents/spec/blob/a64cb14/cloudevents/bindings/http-protocol-binding.md)
for CloudEvents. Of note is how `Content-Type: application/json` implicitly
describes the CloudEvent `datacontenttype` as `application/json`.

The CloudEvent specification expects each combination of "source" and "id"
[to be globally unique](https://github.com/cloudevents/spec/blob/4af4b7/cloudevents/spec.md#source-1).
Thorium internally keeps track of these "source" and "id" combinations it has
received, over a configurable time horizon with a configurable number of
remembered events. If a producer tries to send a "source", "id" combination to
a channel that has already received this combination, Thorium will report the
publication as having been successful, but will not deliver the event to
consumers. Within the constraints of the time horizon and maximum remembered
"source", "id" combinations, this makes event publishing an idempotent
operation.

Note that the same "source", "id" combination can be published to multiple
channels. Each channel will send the data for the same "source", "id"
combination at least once.

## Receiving events

Consumers receive an event stream by requesting an HTTP GET against the same
path used to send events. The content body of this HTTP GET behaves according
to the
[Server-sent event](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events)
specification, with the event name being "data".

For example, if an event is sent with

```
POST /channels/events HTTP/1.1
Content-Type: application/json
Ce-Source: //somewhere
Ce-Id: some-id
Ce-Type: some.type

...
```

Then the event will be present in a GET to the same path:

```
GET /channels/events HTTP/1.1
...

HTTP/1.1 200 OK
Connection: keep-alive
transfer-encoding: chunked
Content-Type: text/event-stream; charset=utf-8

event: connect
data: {"timestamp":"...","clientID":"..."}

... After the event is sent
event: data
id: %2F%2Fsomewhere+some-id
data: {"source":"//somewhere","id":"some-id","type":"some.type","data":{...}}
```

The contents of the Server-sent event are formatted according to the
[JSON Event Format](https://github.com/cloudevents/spec/blob/4af4b73/cloudevents/formats/json-format.md)
for CloudEvents.

### Filtering received events

Thorium is designed to efficiently support deep-content filtering of its JSON
input across thousands of connected clients, with multiple query terms as part
of a logical conjunction (x AND y AND z ...). This filtering is enabled by
passing the terms of the filter as a query string. For example, if a consumer
were to connect to a channel using

```
GET /channels/example?one="one"&two="two"&three=3 HTTP/1.1
...
```

And the producer were to send the events

```
POST /channels/example HTTP/1.1
Content-Type: application/cloudevents-batch+json

[
{
    "source": "somewhere",
    "id": 1,
    "type": "com.example.thorium",
    "specversion": "1.0",
    "data": {
        "one": "one",
        "two": "two",
        "three": 3,
        "four": "four"
    }
}, {
    "source": "somewhere",
    "id": 2,
    "type": "com.example.thorium",
    "specversion": "1.0",
    "data": {
        "one": "one",
        "two": "two",
        "three": 3,
        "four": "five"
    }
}, {
    "source": "somewhere",
    "id": 3,
    "type": "com.example.thorium",
    "specversion": "1.0",
    "data": {
        "two": "two",
        "three": 3,
        "four": "five"
    }
}, {
    "source": "somewhere",
    "id": 4,
    "type": "com.example.thorium",
    "specversion": "1.0",
    "data": {
        "one": "two",
        "two": "two",
        "three": 3
    }
}, {
    "source": "somewhere",
    "id": 5,
    "type": "com.example.thorium",
    "specversion": "1.0",
    "data": {
        "one": "one",
        "two": "one",
        "three": 3
    }
}, {
    "source": "somewhere",
    "id": 6,
    "type": "com.example.thorium",
    "specversion": "1.0",
    "data": {
        "one": "one",
        "two": 2,
        "three": 3
    }
}, {
    "source": "somewhere",
    "id": 7,
    "type": "com.example.thorium",
    "specversion": "1.0",
    "data": {
        "one": "one",
        "two": "two",
        "three": 4
    }
}, {
    "source": "somewhere",
    "id": 8,
    "type": "com.example.thorium",
    "specversion": "1.0",
    "data": {
        "one": "one",
        "two": "two",
        "three": "three"
    }
}
]
```

then a client receiving events for the "example" channel would see

```
GET /channels/example HTTP/1.1
...

HTTP/1.1 200 OK
Connection: keep-alive
transfer-encoding: chunked
Content-Type: text/event-stream; charset=utf-8

event: connect
data: {"timestamp":"...","clientID":"..."}

event: data
id: somewhere+1
data: {"source":"somewhere","id":"1","type":"com.example.thorium","specversion":"1.0","data":
data: {"one":"one","two":"two","three":3,"four":"four"}}

event: data
id: somewhere+2
data: {"source":"somewhere","id":"2","type":"com.example.thorium","specversion":"1.0","data":
data: {"one":"one","two":"two","three":3,"four":"five"}}
```

for the following reasons:
- 1 would match because `data["one"] == "one"`, `data["two"] == "two"`,
  `data["three"] == 3`. The contents, or even presence, of `data["four"]`
  has no effect on the given filter.
- 2 would match because `data["one"] == "one"`, `data["two"] == "two"`,
  `data["three"] == 3`. Similar to 1, the contents, or even presence, of
  `data["four"]` has no effect.
- 3 would not match because `data["one"]` is not present.
- 4 would not match because `data["one"] == "two"` when we expected
  `data["one"] == "one"`.
- 5 would not match because `data["two"] == "one"` when we expected
  `data["two"] == "two"`.
- 6 would not match because `data["two"] == 2` when we expected
  `data["two"] == "two"`.
- 7 would not match because `data["three"] == 4` when we expected
  `data["three"] == 3`.
- 8 would not match because `data["three"] == "three"` when we expected
  `data["three"] == 3`.

By default, if a key does not start with `/` or `../`, it is assumed to be a
literal key within the "data" object of the event. For example, a query string
of the form

```
some.key="value"
```

is interpreted to match
```
{
    "source": "...",
    "id": "...",
    "type": "..."
    "specversion: "1.0",
    "data": {
        "some.key": "value"
    }
}
```

Access to keys within JSON documents is made possible by using
[JSON Pointers](https://datatracker.ietf.org/doc/html/rfc6901).
As an example, to match "some", then "key" in

```
{
    "source": "...",
    "id": "...",
    "type": "com.example.thorium"
    "specversion: "1.0",
    "data": {
        "some": {
            "key": "value"
        }
    }
}
```

you would use a query string

```
?/some/key="value"
```

Note that `~` in a valid key path component must be replaced with `~0`, and
`/` in a valid key path must be replaced with `~1`. The replacement of `~`
with `~0` should occur before replacing `/` with `~1` so that the encoding
`~1` is not accidentally rewritten as `~~1`. For a key path of

```javascript
data["with/slash"]["with~tilde"]
```

the JSON Pointer encoding would be

```
?/with~1slash/with~0tilde
```

Keys are matched starting from the "data" key in the resulting CloudEvent by
default. As an extension to JSON Pointers, if the query string starts with
`..` and the remainder is a JSON Pointer, the key is matched starting from
the object root. As an example, to match the CloudEvent type in

```
{
    "source": "...",
    "id": "...",
    "type": "com.example.thorium"
    "specversion: "1.0",
    "data": {
        "some": {
            "key": "value"
        }
    }
}
```

you would use a query string

```
?../type="com.example.thorium"
```

Values are encoded according to their JSON representation. Only `null`,
booleans, numbers, and strings are supported as match values. If a value
cannot be decoded as `null`, a boolean, or a number, and does not start
with a filter operator prefix, it is assumed to be a string.

### Filter Operators

Thorium supports more filters than just field equality. The following
additional operators are available, but many with caveats on the number of
operators per query.

- ***Logical Not***, with a value prefix of `!`. This can be used multiple
  times in a single query. As an example, `?../type=!"com.example.thorium"`, or
  `..%2Ftype=%21%22com.example.thorium%22` if using strict percent-encoding.
- ***Array Contains***, with a value prefix of `[`. This can be used multiple
  times in a single query. As an example, `?../type=["com.example.thorium"`, or
  `?..%2Ftype=%5B%22com.example.thorium%22` if using strict percent-encoding.
- ***Less Than***, with a value prefix of `<`. This can only be used once in a
  single query, and precludes the use of ***Less Than or Equal*** and ***Starts
  With*** operators. It may be used in conjunction with ***Greater Than or
  Equal*** and ***Greater Than*** only if these operators are used with the
  same key. As an example, `?../type=<"com.example.thorium"`, or
  `?..%2Ftype=%3C%22com.example.thorium%22` if using strict percent-encoding.
- ***Less Than or Equal***, with a value prefix of `<=`. This can only be used
  once in a single query, and precludes the use of the ***Less Than*** and
  ***Starts With*** operators. It may be used in conjunction with ***Greater
  Than or Equal*** and ***Greater Than*** only if these operators are used with
  the same key. As an example, `?../type=<="com.example.thorium"`, or
  `?..%2Ftype=%3C%3D%22com.example.thorium%22` if using strict
  percent-encoding.
- ***Greater Than or Equal***, with a value prefix of `>=`. This can only be
  used once in a single query, and precludes the use of the ***Greater Than***
  and ***Starts With*** operators. It may be used in conjunction with ***Less
  Than*** and ***Less Than or Equal*** only if these operators are used with
  the same key. As an example, `?../type=>="com.example.thorium"`, or
  `?..%2Ftype=%3E%3D%22com.example.thorium%22` if using strict
  percent-encoding.
- ***Greater Than***, with a value prefix of `>`. This can only be used once
  in a single query, and precludes the use of the ***Greater Than or Equal***
  and ***Starts With*** operators. It may be used in conjunction with the
  ***Less Than*** and ***Less Than or Equal*** operators only if these
  operators are used with the same key. As an example,
  `?../type=>"com.example.thorium"`, or
  `?..%2Ftype=%3E%22com.example.thorium%22` if using strict percent-encoding.
- ***Starts With***, with a value prefix of `~`. This can only be used once in
  a single query, can only be used with string values, and precludes the use
  of the ***Less Than***, ***Less Than or Equal***, ***Greater Than or
  Equal***, and ***Greater Than*** operators. As an example,
  `?../type=~"com.example.thorium"`, or
  `?..%2Ftype=%7E%22com.example.thorium%22` if using strict percent-encoding.

## Server Configuration

Thorium accepts configuration through command-line flags and environment
variables.

- **Flag:** `--server-port`
  <br/>
  **Environment Variable:** `THORIUM_SERVER_PORT`
  <br/>
  **Type:** Integer
  <br/>
  **Default Value:** 8232

  Thorium will listen for HTTP connections over this TCP port.

- **Flag:** `--source-name`
  <br/>
  **Environment Variable:** `THORIUM_SOURCE_NAME`
  <br/>
  **Type:** String
  <br/>
  **Default Value:** //name.djsweet.thorium

  CloudEvents emitted by Thorium will use this string as the "source"
  metadata.

- **Flag:** `--query-threads`
  <br/>
  **Environment Variable:** `THORIUM_QUERY_THREADS`
  <br/>
  **Type:** Integer
  <br/>
  **Default Value:** Number of logical CPU threads reported by the operating
  system.

  Thorium will spawn this many operating system threads to service consumer
  connections.

- **Flag:** `--translator-threads`
  <br/>
  **Environment Variable:** `THORIUM_TRANSLATOR_THREADS`
  <br/>
  **Type:** Integer
  <br/>
  **Default Value:** Number of logical CPU threads reported by the operating
  system.

  Thorium will spawn this many operating system threads to translate CloudEvents
  into its internal indexing representation.

- **Flag:** `--web-server-threads`
  <br/>
  **Environment Variable:** `THORIUM_WEB_SERVER_THREADS`
  <br/>
  **Type:** Integer
  <br/>
  **Default Value:** Twice the number of logical CPU threads reported by the
  operating system.

  Thorium will spawn this many operating system threads to service HTTP
  requests.

- **Flag:** `--max-body-size-bytes`
  <br/>
  **Environment Variable:** `THORIUM_MAX_BODY_SIZE_BYTES`
  <br/>
  **Type:** Integer
  <br/>
  **Default Value:** 10,485,760 (10MB)

  Thorium will reject HTTP bodies with a content length greater than this
  value, sending an HTTP 413 when the request body is too large according to
  this value.

- **Flag:** `--max-idempotency-keys`
  <br/>
  **Environment Variable:** `THORIUM_MAX_IDEMPOTENCY_KEYS`
  <br/>
  **Type:** Integer
  <br/>
  **Default Value:** 1,048,576
  
  Thorium will retain this many "source", "id" combinations in a set before
  discarding the oldest values. Setting this value too low may cause duplicate
  publishes of events to become non-idempotent, but setting this value too
  high will result in excess memory usage.

- **Flag:** `--max-json-parsing-recursion`
  <br/>
  **Environment Variable:** `THORIUM_MAX_JSON_PARSING_RECURSION`
  <br/>
  **Type:** Integer
  <br/>
  **Default Value:** 64

  Thorium will recurse this deep when translating JSON into its internal
  indexed representation. At nested objects deeper than the configured value,
  Thorium will use a stack-iterative algorithm that requires heap allocation.
  This value is chosen to trade off performance with StackOverflowError
  exceptions. While Thorium dynamically configures itself to avoid
  StackOverflowErrors in other areas, it is not expected for JSON documents to
  contain thousands of levels of nesting, and thus it is left as a
  configurable value.

- **Flag:** `--max-outstanding-events-per-query-thread`
  <br/>
  **Environment Variable:** `THORIUM_MAX_OUTSTANDING_EVENTS_PER_QUERY_THREAD`
  <br/>
  **Type:** Integer
  <br/>
  **Default Value:** 131,072

  Thorium keeps track of the number of events present "within" the system. An
  event must be delivered to all interested consumers before it is no longer
  tracked as being outstanding. If the number of outstanding events exceeds
  this number, Thorium will respond to producers with an HTTP 429,
  establishing backpressure within the event routing path. The producers are
  expected to re-attempt the publication of their events after a brief period
  of waiting when encountering this HTTP 429.

- **Flag:** `--max-query-terms`
  <br/>
  **Environment Variable:** `THORIUM_MAX_QUERY_TERMS`
  <br/>
  **Type:** Integer
  <br/>
  **Default Value:** 32

  Thorium limits the number of filter terms available to consumers to prevent
  excessively large query indices. If a client attempts to use more filters
  than this configured value, Thorium will reply with an HTTP 400.

- **Flag:** `--body-timeout-ms`
  <br/>
  **Environment Variable:** `THORIUM_BODY_TIMEOUT_MS`
  <br/>
  **Type:** Integer
  <br/>
  **Default Value:** 60,000

  After all HTTP headers are received, Thorium will wait up to this many
  milliseconds to receive an entire response body. If the response body is not
  fully received within this time, Thorium will respond with an HTTP 408.

- **Flag:** `--idempotency-expiration-ms`
  <br/>
  **Environment Variable:** `THORIUM_IDEMPOTENCY_EXPIRATION_MS`
  <br/>
  **Type:** Integer
  <br/>
  **Default Value:** 180,000

  Thorium will remove its record of a "source", "id" combination from its
  internal tracking set after this many milliseconds. Any transmission of the
  same "source", "id" combination after the combination is removed from the
  tracking set will result in the data being sent to consumers.

- **Flag:** `--tcp-idle-timeout-ms`
  <br/>
  **Environment Variable:** `THORIUM_TCP_IDLE_TIMEOUT_MS`
  <br/>
  **Type:** Integer
  <br/>
  **Default Value:** 180,000

  Thorium will close a TCP connection after this many milliseconds of no
  activity. This prevents connections dropped without a TCP FIN or TCP RST
  from consuming resources.
