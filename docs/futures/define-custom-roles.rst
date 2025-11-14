Define custom roles for JulieOps
*******************************

While JulieOps offer you as a user the possibility to manage the ACLs for most common
applications deployments such as Consumers, Producers, Kafka Streams, Connectors and ksqlDB, it would be for some cases amazing to be
be able to keep using the powerful abstractions of JulieOps but provide your own set of ACLs.

For example:

* If you are deploying a custom App and aim to give application specific roles
* Deploying applications that might not fit our of the box with generic permissions provided by JulieOps
* Or just if you are building your own roles based on Simple ACLs

and more.

But, how can you get this with JulieOps.

Defining the your roles
-----------

First thing is to define your roles in a configuration file, this file should look like this:

.. code-block:: YAML

    roles:
      - name: "app"
        acls:
          - resourceType: "Topic"
            resourceName: "{{topic}}"
            patternType: "PREFIXED"
            host: "*"
            operation: "ALL"
            permissionType: "ALLOW"
          - resourceType: "Topic"
            resourceName: "sourceTopic"
            patternType: "LITERAL"
            host: "*"
            operation: "ALL"
            permissionType: "ALLOW"
          - resourceType: "Topic"
            resourceName: "targetTopic"
            patternType: "LITERAL"
            host: "*"
            operation: "ALL"
            permissionType: "ALLOW"
          - resourceType: "Group"
            resourceName: "{{group}}"
            patternType: "PREFIXED"
            host: "*"
            operation: "READ"
            permissionType: "ALLOW"

Plug this into JulieOps
-----------

Once the roles are define, the only thing you need to do is to configure your deployment to use it. This can be done using this
configuration variable in your property file:


.. code-block:: JAVA

    julie.roles=/path/to/the/roles/file


How would my new topology file look like
-----------

Once the new roles are setup, your topology can start using them just as the previous "hardcoded" roles.
Your topology file could look like this:


.. code-block:: YAML

    context: "contextOrg"
    source: "source"
    projects:
      - name: "foo"
        foo:
          - principal: "User:banana"
            group: "foo"
        bar:
          - principal: "User:bandana"
            group: "bar"


Role fields Types
-----------

Some role fields map directly to Kafka enums and other are names of resources. In table below

+----------------+-----------------------------------------------+
| Role field     | Type                                          |
+================+===============================================+
| permissionType | org.apache.kafka.common.acl.AclPermissionType |
+----------------+-----------------------------------------------+
| host           | java.lang.String                              |
+----------------+-----------------------------------------------+
| patternType    | org.apache.kafka.common.resource.PatternType  |
+----------------+-----------------------------------------------+
| resourceName   | java.lang.String                              |
+----------------+-----------------------------------------------+
| resourceType   | org.apache.kafka.common.resource.ResourceType |
+----------------+-----------------------------------------------+
| operation      | org.apache.kafka.common.acl.AclOperation      |
+----------------+-----------------------------------------------+

For enum values configuration must match names of values of those enum fields (`.fromString()` gets called).
Note that there are additional limitations set by Kafka. Because the same enumerations are used for querying
in kafka API they include values that can not be set. For example you can't set ACL which has `operation` of
`UNKNOWN` or `ANY`.


Shortening roles
-----------

Writing roles can be quite repetitive. Therefore `host`, `permissionType` and `patternType` have default values.

+----------------+---------------+
| Role field     | Default value |
+================+===============+
| permissionType | ALLOW         |
+----------------+---------------+
| host           | *             |
+----------------+---------------+
| patternType    | LITERAL       |
+----------------+---------------+


More generic usage of roles
-----------

Lets assume you need to deploy multiple instances of an application that is not willing to use JulieOps topic hierarchy,
requires multiple topics and custom ACLs. You could define a role for each instance, or for each group/topic,
but that would clutter the roles file quickly. You would however like to have these topics in JulieOps, for ACL management
and for documentation. For example Kafka MirrorMaker needs many topics which would be nice to group together in JulieOps
config.

For brevity example below gives too much permissions (ALL), but works as example how feature works.

.. code-block:: YAML

---
roles:
  - name: "mirrorMaker"
    acls:
      - resourceType: "Topic"
        resourceName: "{{statusTopic}}"
        operation: "ALL"
      - resourceType: "Topic"
        resourceName: "{{offsetTopic}}"
        operation: "ALL"
      - resourceType: "Topic"
        resourceName: "{{configTopic}}"
        operation: "ALL"
      - resourceType: "Topic"
        resourceName: "{{targetPrefix}}"
        patternType: "PREFIXED"
        operation: "ALL"
      - resourceType: "Topic"
        resourceName: "{{offsetSyncTopic}}"
        operation: "ALL"
      - resourceType: "Topic"
        resourceName: "{{checkpointsTopic}}"
        operation: "ALL"
      - resourceType: "Topic"
        # using jinja2 syntax to provide a default value
        resourceName: "{{heartbeatTopic or 'mirrormaker2-heartbeat'}}"
        operation: "ALL"
      - resourceType: "Cluster"
        resourceName: "kafka-cluster"
        operation: "DESCRIBE"
      - resourceType: "Cluster"
        resourceName: "kafka-cluster"
        operation: "DESCRIBE_CONFIGS"
      - resourceType: "Group"
        resourceName: "{{group}}"
        operation: "ALL"

With previous role MirrorMaker can be defined in a clutter free manner in a project.

.. code-block:: YAML

    context: "contextOrg"
    source: "source"
    projects:
      - name: "foo"
        mirrorMaker:
          - principal: "User:banana"
            group: "foo"
            statusTopic: "test-cluster-status"
            offsetTopic: "test-cluster-offsets"
            configTopic: "test-cluster-configs"
            targetPrefix: "target-prefix."
            offsetSyncTopic: "mm2-offset-syncs.test-mm.internal"
            checkpointsTopic: "test-mm.checkpoints.internal"

Somewhat viable alternative to this would be to use special_topics, but they limit to topic and producer/consumer ACLs
and don't allow PREFIX type permissions.
