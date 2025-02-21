# Changelog

## [4.7.3](https://github.com/statnett/kafka-ops-julie/compare/v4.7.2...v4.7.3) (2025-02-21)


### Bug Fixes

* **deps:** update confluent-ce.version to v7.9.0-ce ([#198](https://github.com/statnett/kafka-ops-julie/issues/198)) ([a2b15d0](https://github.com/statnett/kafka-ops-julie/commit/a2b15d0d5a4d00a4c7470752f089c7febd4bfc61))
* **deps:** update confluent.version to v7.9.0 ([#199](https://github.com/statnett/kafka-ops-julie/issues/199)) ([1061f6b](https://github.com/statnett/kafka-ops-julie/commit/1061f6bee086deaa715b8263c154b16307d66077))
* **deps:** update dependency io.confluent.ksql:ksqldb-api-client to v7.8.0 ([#176](https://github.com/statnett/kafka-ops-julie/issues/176)) ([710629e](https://github.com/statnett/kafka-ops-julie/commit/710629ef0de53bd67bde853c618d9e95d35fc56b))

## [4.7.2](https://github.com/statnett/kafka-ops-julie/compare/v4.7.1...v4.7.2) (2025-02-10)


### Bug Fixes

* add write permission for packages ([#193](https://github.com/statnett/kafka-ops-julie/issues/193)) ([63cfe80](https://github.com/statnett/kafka-ops-julie/commit/63cfe804d13efe0f68d4ea10b923b42e92a5a05a))

## [4.7.1](https://github.com/statnett/kafka-ops-julie/compare/v4.7.0...v4.7.1) (2025-02-10)


### Bug Fixes

* **deploy:** add actor ([#190](https://github.com/statnett/kafka-ops-julie/issues/190)) ([a8d4d34](https://github.com/statnett/kafka-ops-julie/commit/a8d4d34ebfdc7e1f0cb89a39b040ba6e2e03bf3d))

## [4.7.0](https://github.com/statnett/kafka-ops-julie/compare/v4.6.0...v4.7.0) (2025-02-10)


### Features

* publish to GitHub Packages ([#185](https://github.com/statnett/kafka-ops-julie/issues/185)) ([8371b7d](https://github.com/statnett/kafka-ops-julie/commit/8371b7d129bb36373e57d7aab3a15095d1fc7cf3))


### Bug Fixes

* **deps:** update confluent-ce.version to v7.8.1-ce ([#187](https://github.com/statnett/kafka-ops-julie/issues/187)) ([b09cb63](https://github.com/statnett/kafka-ops-julie/commit/b09cb63d76011893cf30f4680d5d8e4fc752dc55))
* **deps:** update confluent.version to v7.8.1 ([#188](https://github.com/statnett/kafka-ops-julie/issues/188)) ([400b45f](https://github.com/statnett/kafka-ops-julie/commit/400b45f039312a28bd9d3e6e637c05ef9255d89a))

## [4.6.0](https://github.com/statnett/kafka-ops-julie/compare/v4.5.2...v4.6.0) (2025-01-28)


### Features

* include version in final jar ([#181](https://github.com/statnett/kafka-ops-julie/issues/181)) ([c9db3b5](https://github.com/statnett/kafka-ops-julie/commit/c9db3b5db4a01c25e2813abe949852d4eb503447))

## [4.5.2](https://github.com/statnett/kafka-ops-julie/compare/v4.5.1...v4.5.2) (2025-01-25)


### Bug Fixes

* **deps:** update confluent-ce.version to v7.8.0-ce ([#173](https://github.com/statnett/kafka-ops-julie/issues/173)) ([fcce704](https://github.com/statnett/kafka-ops-julie/commit/fcce7049792cb7ced9eee9bcb63b56e59262cad5))
* **deps:** update confluent.version to v7.8.0 ([#174](https://github.com/statnett/kafka-ops-julie/issues/174)) ([a99e7f1](https://github.com/statnett/kafka-ops-julie/commit/a99e7f1d7b07b0de472279bb36f62ffbfac9291a))
* **deps:** update dependency com.fasterxml.jackson:jackson-bom to v2.18.2 ([#162](https://github.com/statnett/kafka-ops-julie/issues/162)) ([5f35340](https://github.com/statnett/kafka-ops-julie/commit/5f35340c94be736db9e0ed1332885a9760403d96))
* **deps:** update dependency com.hubspot.jinjava:jinjava to v2.7.4 ([#163](https://github.com/statnett/kafka-ops-julie/issues/163)) ([eee1bb9](https://github.com/statnett/kafka-ops-julie/commit/eee1bb929fa463f7f21eadb80347b8062678d224))
* **deps:** update dependency org.apache.logging.log4j:log4j-bom to v2.24.3 ([#165](https://github.com/statnett/kafka-ops-julie/issues/165)) ([8a7003d](https://github.com/statnett/kafka-ops-julie/commit/8a7003d0987a5fda105235299aa0fc197acfd019))
* **deps:** update dependency org.projectlombok:lombok to v1.18.36 ([#166](https://github.com/statnett/kafka-ops-julie/issues/166)) ([c8c634b](https://github.com/statnett/kafka-ops-julie/commit/c8c634bcd7135f70a1866a9278f9a494bfe5a29d))

## [4.5.1](https://github.com/statnett/kafka-ops-julie/compare/v4.5.0...v4.5.1) (2025-01-23)


### Bug Fixes

* remove gpg signing of jar files ([#149](https://github.com/statnett/kafka-ops-julie/issues/149)) ([71a6c91](https://github.com/statnett/kafka-ops-julie/commit/71a6c912d7af07dd9f7316a3ab299da8d3297994))
* rename old changelog.md to avoid character case collisions ([#151](https://github.com/statnett/kafka-ops-julie/issues/151)) ([e118165](https://github.com/statnett/kafka-ops-julie/commit/e1181659dd047dae6fe8311d8fd841bfe988bbb1))

## [4.5.0](https://github.com/statnett/kafka-ops-julie/compare/v4.4.1...v4.5.0) (2025-01-23)


### ⚠ BREAKING CHANGES

* rename __julieops_commands topic to __julieops_state ([#118](https://github.com/statnett/kafka-ops-julie/issues/118))
* rename KafkaBackend config variables to better reflect what they do ([#117](https://github.com/statnett/kafka-ops-julie/issues/117))

### Features

* add developer manage rbac ([#351](https://github.com/statnett/kafka-ops-julie/issues/351)) ([a2e79d0](https://github.com/statnett/kafka-ops-julie/commit/a2e79d0fe160443d84b2dd1ff32c6f4e4714d2f0))
* add exactly-once to kstreams and create transactionalid ACLs ([#388](https://github.com/statnett/kafka-ops-julie/issues/388)) ([2db3160](https://github.com/statnett/kafka-ops-julie/commit/2db3160922fa71445e22d7486522e8451e2d361e))
* make KafkaBackend create state topic if missing ([#111](https://github.com/statnett/kafka-ops-julie/issues/111)) ([cab85dc](https://github.com/statnett/kafka-ops-julie/commit/cab85dc0150b8b763dbf0791980cbea4ab62877a))
* rename KafkaBackend config variables to better reflect what they do ([#117](https://github.com/statnett/kafka-ops-julie/issues/117)) ([f17c290](https://github.com/statnett/kafka-ops-julie/commit/f17c2905d6e5af60f19e0a4cee5762e67cee1203))


### Bug Fixes

* add missing release-please state ([#144](https://github.com/statnett/kafka-ops-julie/issues/144)) ([b37a9cb](https://github.com/statnett/kafka-ops-julie/commit/b37a9cb2f46359daffb62f93fdafc1299d58377a))
* delete problem on resource used from the rbac api (wrong resource type) ([#398](https://github.com/statnett/kafka-ops-julie/issues/398)) ([ced93c7](https://github.com/statnett/kafka-ops-julie/commit/ced93c74af63d24791677910085f2c47512b3bce))
* **deps:** update dependency com.fasterxml.jackson:jackson-bom to v2.18.1 ([#85](https://github.com/statnett/kafka-ops-julie/issues/85)) ([9ad8be7](https://github.com/statnett/kafka-ops-julie/commit/9ad8be74b5095366c035dcb29ee4905c731fa65b))
* **deps:** update dependency com.google.cloud:libraries-bom to v26.46.0 ([#68](https://github.com/statnett/kafka-ops-julie/issues/68)) ([4ad1e42](https://github.com/statnett/kafka-ops-julie/commit/4ad1e420a3cc3e0c9e9b1677223eec0983bbb77a))
* **deps:** update dependency com.google.cloud:libraries-bom to v26.47.0 ([#72](https://github.com/statnett/kafka-ops-julie/issues/72)) ([ba06593](https://github.com/statnett/kafka-ops-julie/commit/ba065932393153c3859a8f6d9f6d064fb837f83c))
* **deps:** update dependency com.google.cloud:libraries-bom to v26.49.0 ([#97](https://github.com/statnett/kafka-ops-julie/issues/97)) ([33cc884](https://github.com/statnett/kafka-ops-julie/commit/33cc8849a13ba26c8cf150251b9e0ac152a445bf))
* **deps:** update dependency com.google.cloud:libraries-bom to v26.50.0 ([#113](https://github.com/statnett/kafka-ops-julie/issues/113)) ([097c570](https://github.com/statnett/kafka-ops-julie/commit/097c570c8bbfc67cc813e1119c42bd6c1268c8ea))
* **deps:** update dependency com.hubspot.jinjava:jinjava to v2.7.3 ([#41](https://github.com/statnett/kafka-ops-julie/issues/41)) ([fe5fc58](https://github.com/statnett/kafka-ops-julie/commit/fe5fc583729d1b84ff6cd857eedf55cd972a1cb7))
* **deps:** update dependency redis.clients:jedis to v5.2.0 ([#88](https://github.com/statnett/kafka-ops-julie/issues/88)) ([84e85cd](https://github.com/statnett/kafka-ops-julie/commit/84e85cd676507a7bbec46a24ef6a391186e78359))
* **deps:** update dependency software.amazon.awssdk:bom to v2.28.1 ([#45](https://github.com/statnett/kafka-ops-julie/issues/45)) ([d5880f6](https://github.com/statnett/kafka-ops-julie/commit/d5880f6c1f6f6f1656690d72aa0006ee5be9f2e5))
* **deps:** update dependency software.amazon.awssdk:bom to v2.28.2 ([#62](https://github.com/statnett/kafka-ops-julie/issues/62)) ([cc02373](https://github.com/statnett/kafka-ops-julie/commit/cc02373bc52dadaa2ea3f8c2aac8588c22849995))
* **deps:** update dependency software.amazon.awssdk:bom to v2.28.3 ([#67](https://github.com/statnett/kafka-ops-julie/issues/67)) ([68f0412](https://github.com/statnett/kafka-ops-julie/commit/68f0412af04736354e34fe6636e174f25d8c47ee))
* **deps:** update dependency software.amazon.awssdk:bom to v2.28.4 ([#71](https://github.com/statnett/kafka-ops-julie/issues/71)) ([c304fc4](https://github.com/statnett/kafka-ops-julie/commit/c304fc4d61c2349321621bd83ea2673c7dc923d2))
* **deps:** update dependency software.amazon.awssdk:bom to v2.28.5 ([#76](https://github.com/statnett/kafka-ops-julie/issues/76)) ([d0f8879](https://github.com/statnett/kafka-ops-julie/commit/d0f8879a8ab0fbfd9ce3748b00fabea5dcb96155))
* **deps:** update dependency software.amazon.awssdk:bom to v2.28.6 ([#79](https://github.com/statnett/kafka-ops-julie/issues/79)) ([c766c2e](https://github.com/statnett/kafka-ops-julie/commit/c766c2e2db5324b9c8db2a8f98f33013fecb5ff9))
* **deps:** update dependency software.amazon.awssdk:bom to v2.28.7 ([#81](https://github.com/statnett/kafka-ops-julie/issues/81)) ([8845591](https://github.com/statnett/kafka-ops-julie/commit/8845591d046b9094ddd7aabd13b9c7fc5aa9544a))
* **deps:** update dependency software.amazon.awssdk:bom to v2.28.8 ([#82](https://github.com/statnett/kafka-ops-julie/issues/82)) ([0780043](https://github.com/statnett/kafka-ops-julie/commit/0780043c182245a7eedcc69090be0e47677438b6))
* **deps:** update dependency software.amazon.awssdk:bom to v2.29.10 ([#129](https://github.com/statnett/kafka-ops-julie/issues/129)) ([f7399af](https://github.com/statnett/kafka-ops-julie/commit/f7399afaf31949c44ee6862bc4564911fe6a2cdf))
* **deps:** update dependency software.amazon.awssdk:bom to v2.29.4 ([#84](https://github.com/statnett/kafka-ops-julie/issues/84)) ([64b1da0](https://github.com/statnett/kafka-ops-julie/commit/64b1da00b2c1ccb6895ea1719bf60d0e6edf1b2a))
* **deps:** update dependency software.amazon.awssdk:bom to v2.29.6 ([#112](https://github.com/statnett/kafka-ops-julie/issues/112)) ([ea1a7cf](https://github.com/statnett/kafka-ops-julie/commit/ea1a7cf0de9d66ab6fcf624a4864f3985c436983))
* **deps:** update dependency software.amazon.awssdk:bom to v2.29.9 ([#126](https://github.com/statnett/kafka-ops-julie/issues/126)) ([14fa53f](https://github.com/statnett/kafka-ops-julie/commit/14fa53f43db8e62493811e8d2480ed06acd0f9d6))
* **deps:** update misc dependencies ([#110](https://github.com/statnett/kafka-ops-julie/issues/110)) ([a90d67b](https://github.com/statnett/kafka-ops-julie/commit/a90d67bd201b9cfe2177031209403939384a7d9f))
* do not attempt to create state topic if dry-run ([#125](https://github.com/statnett/kafka-ops-julie/issues/125)) ([4aa03b6](https://github.com/statnett/kafka-ops-julie/commit/4aa03b6f1aa69807fe400026feb1df2986db2f81))
* issue with connector and subject permissions being ignored when more than one ([#415](https://github.com/statnett/kafka-ops-julie/issues/415)) ([67c2ce7](https://github.com/statnett/kafka-ops-julie/commit/67c2ce7c0c53c3452197619eccda9790cfa71126))
* Kafka as a Backend for JulieOps resiliency. Ensure state is not corrupted. ([#396](https://github.com/statnett/kafka-ops-julie/issues/396)) ([07a33ea](https://github.com/statnett/kafka-ops-julie/commit/07a33ea4014fca86c8bd588739565567461acfe8))
* ksqldb delele artefacts delete order, first tables then streams, the reverse of the creation ([#392](https://github.com/statnett/kafka-ops-julie/issues/392)) ([50eff14](https://github.com/statnett/kafka-ops-julie/commit/50eff14cdf1c3bc75f6427263a1670e17a38acf5))
* make sure state topic is created, or exception is thrown ([#121](https://github.com/statnett/kafka-ops-julie/issues/121)) ([3ed3c7c](https://github.com/statnett/kafka-ops-julie/commit/3ed3c7c4402c4adb6695dbce066ee9dd2001d78c))
* RBAC Subject and Cluster level binding ([#301](https://github.com/statnett/kafka-ops-julie/issues/301)) ([011b0d0](https://github.com/statnett/kafka-ops-julie/commit/011b0d0893f319677e6157e3b726870f504f78a6))
* ssl context contruction when no ssl configuration is provided. ([#327](https://github.com/statnett/kafka-ops-julie/issues/327)) ([eb0beb4](https://github.com/statnett/kafka-ops-julie/commit/eb0beb4f814df6a8dec353b7fb4584a93d71f700))
* stupid error in checking server labels when creating a connector ([#325](https://github.com/statnett/kafka-ops-julie/issues/325)) ([480b3ee](https://github.com/statnett/kafka-ops-julie/commit/480b3ee276ba69e60f179757f7c6c6f2f32336c1))
* Topic config values should take precedence over plan config values ([#410](https://github.com/statnett/kafka-ops-julie/issues/410)) ([3d82c5b](https://github.com/statnett/kafka-ops-julie/commit/3d82c5b655d8369c860ccc4e4f282c69cc0e7481))
* Unrecognized field "resource_id" ([#307](https://github.com/statnett/kafka-ops-julie/issues/307)) ([#309](https://github.com/statnett/kafka-ops-julie/issues/309)) ([b5307e2](https://github.com/statnett/kafka-ops-julie/commit/b5307e2e6a3f4c031044e8e2ff5285a36da4bd15))
* Use QuotaManager mock in cli unit tests ([#582](https://github.com/statnett/kafka-ops-julie/issues/582)) ([40d70a6](https://github.com/statnett/kafka-ops-julie/commit/40d70a6263437bf569c0867da3d5ea6bf0837171))


### Documentation

* add producer example with transactionID ([#311](https://github.com/statnett/kafka-ops-julie/issues/311)) ([9dc2ec0](https://github.com/statnett/kafka-ops-julie/commit/9dc2ec05c368912c26ea8a9d2b4b198021b522c6))
* replace original README with a disclaimer/explanation ([#141](https://github.com/statnett/kafka-ops-julie/issues/141)) ([d8a9fed](https://github.com/statnett/kafka-ops-julie/commit/d8a9feda60ac61fa24753f920b85ac1692c0b862))


### Miscellaneous Chores

* release 4.4.1 ([bbbb61f](https://github.com/statnett/kafka-ops-julie/commit/bbbb61f7d630572d38621b23064004617012a4da))
* release 4.5.0 ([2cf9e88](https://github.com/statnett/kafka-ops-julie/commit/2cf9e8881da142ae338964f7e4beb7db0ddb9164))
* rename __julieops_commands topic to __julieops_state ([#118](https://github.com/statnett/kafka-ops-julie/issues/118)) ([d6f0ac9](https://github.com/statnett/kafka-ops-julie/commit/d6f0ac9b78f1af19c7b9f2b2bf4394184958cdbb))
