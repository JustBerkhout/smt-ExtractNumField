# General
This is a demo of a custom transformation.

Read more about Kafka Connect transformations (or _"single message transforms"_ or _SMTs_ )
in the Confluent documentation [here](https://docs.confluent.io/platform/current/connect/transforms/overview.html
"Connect Transformations | Confluent documentation") or specifically about creating custom transformations
[here](https://docs.confluent.io/platform/current/connect/transforms/custom.html "Custom Transformations |
Confluent documentation")

# This thing
* extracts the numeric portion at the end of a string field. E.g. `{"userid": "user_6"}` becomes `{"userid": "6"}`.
* loops through all fields given in the configuration `Fields`
* ignores `Fields` it cannot find.
* crashes the hosting connector if you pass it a non-string field
* returns `null` when input is `null`.
* returns `null` when no numeric found at the end of the string
* outputs a String-representation of an integer. _Datatype is String not Integer_ (If you need Int, daisy-chain the `org.apache.kafka.connect.transforms.Cast` transformation)

The code of this transformation is based on the code of the SMT `MaskField` (see [here](https://github.com/apache/kafka/blob/trunk/connect/transforms/src/main/java/org/apache/kafka/connect/transforms/MaskField.java "MaskField transformation | Apache Kafka project"))


# Remarks

* This demo does not contain unit tests
* This demo only implements a `Value`-class (no `Key`-class)
* Nested fields have not been considered. At all.
