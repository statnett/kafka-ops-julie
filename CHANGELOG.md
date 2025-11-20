# Changelog

## [6.1.0](https://github.com/statnett/kafka-ops-julie/compare/v6.0.0...v6.1.0) (2025-11-20)


### Features

* remove support for Confluent Control Center ([#537](https://github.com/statnett/kafka-ops-julie/issues/537)) ([b95caae](https://github.com/statnett/kafka-ops-julie/commit/b95caae7e58dfde936425557a1ce9ff1e7996bb1))


### Bug Fixes

* **deps:** update dependency com.google.cloud:libraries-bom to v26.72.0 ([#529](https://github.com/statnett/kafka-ops-julie/issues/529)) ([019fb53](https://github.com/statnett/kafka-ops-julie/commit/019fb53f1b466d47312b4ccdd0dbf757c0a972a5))
* **deps:** update dependency software.amazon.awssdk:bom to v2.38.7 ([#525](https://github.com/statnett/kafka-ops-julie/issues/525)) ([f1422ee](https://github.com/statnett/kafka-ops-julie/commit/f1422eede360550db5190f811961d52876a11d99))
* **deps:** update dependency software.amazon.awssdk:bom to v2.38.8 ([#528](https://github.com/statnett/kafka-ops-julie/issues/528)) ([5634f24](https://github.com/statnett/kafka-ops-julie/commit/5634f240797da3db4e5bfb97bf5a956b84e41b58))
* **deps:** update dependency software.amazon.awssdk:bom to v2.39.0 ([#539](https://github.com/statnett/kafka-ops-julie/issues/539)) ([190aa6f](https://github.com/statnett/kafka-ops-julie/commit/190aa6f76850037ef5af9f6196630ac2d96929e3))

## [6.0.0](https://github.com/statnett/kafka-ops-julie/compare/v5.3.0...v6.0.0) (2025-11-14)


### ⚠ BREAKING CHANGES

* remove RBAC/MDS functionality that has not been tested for years ([#520](https://github.com/statnett/kafka-ops-julie/issues/520))

### Features

* remove RBAC/MDS functionality that has not been tested for years ([#520](https://github.com/statnett/kafka-ops-julie/issues/520)) ([e8ffec0](https://github.com/statnett/kafka-ops-julie/commit/e8ffec0dcdded67f531fe5904d6c27ab3d1b5567))

## [5.3.0](https://github.com/statnett/kafka-ops-julie/compare/v5.2.3...v5.3.0) (2025-11-14)


### Features

* update Java to 25 ([#517](https://github.com/statnett/kafka-ops-julie/issues/517)) ([e342360](https://github.com/statnett/kafka-ops-julie/commit/e34236053189ad45bd0bce5776e477065317f554))


### Bug Fixes

* **deps:** update confluent.version to v8.1.0 ([#501](https://github.com/statnett/kafka-ops-julie/issues/501)) ([e5b2718](https://github.com/statnett/kafka-ops-julie/commit/e5b2718d2dd5c42ed408532bcceba05b724bd6d1))
* **deps:** update dependency software.amazon.awssdk:bom to v2.38.6 ([#518](https://github.com/statnett/kafka-ops-julie/issues/518)) ([07f1c96](https://github.com/statnett/kafka-ops-julie/commit/07f1c963d2839d46b6f581ac9c7da6a6752e6d11))

## [5.2.3](https://github.com/statnett/kafka-ops-julie/compare/v5.2.2...v5.2.3) (2025-11-13)


### Bug Fixes

* clear all topics between tests on a reused container ([#499](https://github.com/statnett/kafka-ops-julie/issues/499)) ([15503c7](https://github.com/statnett/kafka-ops-julie/commit/15503c735c89de62d91ba2a4f899f32ed69c9abc))
* **deps:** update confluent.version to v8.0.1 ([#459](https://github.com/statnett/kafka-ops-julie/issues/459)) ([24028a7](https://github.com/statnett/kafka-ops-julie/commit/24028a799197ca5ae3274beb596efe937ae8306e))
* **deps:** update dependency com.fasterxml.jackson:jackson-bom to v2.20.1 ([#492](https://github.com/statnett/kafka-ops-julie/issues/492)) ([50bb59b](https://github.com/statnett/kafka-ops-julie/commit/50bb59b7d9fe0bc081d7fb86c1092a5d32606aaf))
* **deps:** update dependency com.google.cloud:libraries-bom to v26.70.0 ([#477](https://github.com/statnett/kafka-ops-julie/issues/477)) ([f3a4a72](https://github.com/statnett/kafka-ops-julie/commit/f3a4a72d26ebb10b13f29d7da061a3d1b1a14140))
* **deps:** update dependency com.google.cloud:libraries-bom to v26.71.0 ([#490](https://github.com/statnett/kafka-ops-julie/issues/490)) ([d988d3a](https://github.com/statnett/kafka-ops-julie/commit/d988d3a924537cff867cfb37d92f91f5feede8ac))
* **deps:** update dependency com.hubspot.jinjava:jinjava to v2.8.2 ([#488](https://github.com/statnett/kafka-ops-julie/issues/488)) ([ee6c3bd](https://github.com/statnett/kafka-ops-julie/commit/ee6c3bd8d024bd683ab2c00a3d14c6ec15553892))
* **deps:** update dependency commons-cli:commons-cli to v1.11.0 ([#505](https://github.com/statnett/kafka-ops-julie/issues/505)) ([829fc45](https://github.com/statnett/kafka-ops-julie/commit/829fc4500676d8e035985160875557e559e74141))
* **deps:** update dependency io.confluent.ksql:ksqldb-api-client to v8.0.2 ([#479](https://github.com/statnett/kafka-ops-julie/issues/479)) ([ed5e71a](https://github.com/statnett/kafka-ops-julie/commit/ed5e71a752a13f1d3bf4749a8e6f4e417e930422))
* **deps:** update dependency io.confluent.ksql:ksqldb-api-client to v8.1.0 ([#484](https://github.com/statnett/kafka-ops-julie/issues/484)) ([cea7899](https://github.com/statnett/kafka-ops-julie/commit/cea7899c392897f3f5d53e26e487b383d25962c2))
* **deps:** update dependency redis.clients:jedis to v7 ([#478](https://github.com/statnett/kafka-ops-julie/issues/478)) ([b27ec3f](https://github.com/statnett/kafka-ops-julie/commit/b27ec3f97589078842e08a680cb32c7fdc152be0))
* **deps:** update dependency software.amazon.awssdk:bom to v2.35.10 ([#486](https://github.com/statnett/kafka-ops-julie/issues/486)) ([d81debe](https://github.com/statnett/kafka-ops-julie/commit/d81debe96643a638128e21ba628f311452291561))
* **deps:** update dependency software.amazon.awssdk:bom to v2.35.2 ([#473](https://github.com/statnett/kafka-ops-julie/issues/473)) ([a352bb8](https://github.com/statnett/kafka-ops-julie/commit/a352bb8f33a1fcc3712a5ada51e2a5bcf14be30b))
* **deps:** update dependency software.amazon.awssdk:bom to v2.35.5 ([#476](https://github.com/statnett/kafka-ops-julie/issues/476)) ([31dbcce](https://github.com/statnett/kafka-ops-julie/commit/31dbcce4ee466300473fe502a083babf67ddc4ff))
* **deps:** update dependency software.amazon.awssdk:bom to v2.35.6 ([#481](https://github.com/statnett/kafka-ops-julie/issues/481)) ([1c0f65c](https://github.com/statnett/kafka-ops-julie/commit/1c0f65cb9623c7abc63c8f6a26a72449de9cc942))
* **deps:** update dependency software.amazon.awssdk:bom to v2.35.7 ([#482](https://github.com/statnett/kafka-ops-julie/issues/482)) ([385a8db](https://github.com/statnett/kafka-ops-julie/commit/385a8db3099783afe168da33bce77242090566bc))
* **deps:** update dependency software.amazon.awssdk:bom to v2.35.8 ([#485](https://github.com/statnett/kafka-ops-julie/issues/485)) ([4b5afb6](https://github.com/statnett/kafka-ops-julie/commit/4b5afb6050542e7c87f89c1130702802e4ca5e71))
* **deps:** update dependency software.amazon.awssdk:bom to v2.36.1 ([#487](https://github.com/statnett/kafka-ops-julie/issues/487)) ([e9f6127](https://github.com/statnett/kafka-ops-julie/commit/e9f6127edb23d411ad379c9b595a23604d0f9b7e))
* **deps:** update dependency software.amazon.awssdk:bom to v2.36.3 ([#489](https://github.com/statnett/kafka-ops-julie/issues/489)) ([697acd4](https://github.com/statnett/kafka-ops-julie/commit/697acd4cd6a3d9c8f1a491b3926c574228424f2c))
* **deps:** update dependency software.amazon.awssdk:bom to v2.37.3 ([#491](https://github.com/statnett/kafka-ops-julie/issues/491)) ([d6a87af](https://github.com/statnett/kafka-ops-julie/commit/d6a87af1d31b84dbad34a866b78df52a18ad6b4b))
* **deps:** update dependency software.amazon.awssdk:bom to v2.37.5 ([#493](https://github.com/statnett/kafka-ops-julie/issues/493)) ([62eb904](https://github.com/statnett/kafka-ops-julie/commit/62eb9044922c9ce772af1b97a665fd3aeb3ca8dc))
* **deps:** update dependency software.amazon.awssdk:bom to v2.38.2 ([#494](https://github.com/statnett/kafka-ops-julie/issues/494)) ([86b04e6](https://github.com/statnett/kafka-ops-julie/commit/86b04e613e9126ce0392ec6d1b7edd1875aba10c))
* **deps:** update dependency software.amazon.awssdk:bom to v2.38.5 ([#500](https://github.com/statnett/kafka-ops-julie/issues/500)) ([0de872c](https://github.com/statnett/kafka-ops-julie/commit/0de872cb4ea44dcfbce795429396562e8547fd5d))
* remove Apache Kafka, use Confluent Platform only ([#498](https://github.com/statnett/kafka-ops-julie/issues/498)) ([f27af44](https://github.com/statnett/kafka-ops-julie/commit/f27af44df06eec305ae713e53671505a4e6469bd))

## [5.2.2](https://github.com/statnett/kafka-ops-julie/compare/v5.2.1...v5.2.2) (2025-10-05)


### Bug Fixes

* **deps:** update dependency software.amazon.awssdk:bom to v2.35.0 ([#468](https://github.com/statnett/kafka-ops-julie/issues/468)) ([5205762](https://github.com/statnett/kafka-ops-julie/commit/5205762f68bb8304f2712968d55d4794b19f50cb))
* make permission type ALLOW by default ([#471](https://github.com/statnett/kafka-ops-julie/issues/471)) ([7f49f12](https://github.com/statnett/kafka-ops-julie/commit/7f49f127fa67353b750a287e074c4c476ed94d62))

## [5.2.1](https://github.com/statnett/kafka-ops-julie/compare/v5.2.0...v5.2.1) (2025-10-03)


### Bug Fixes

* **deps:** update dependency com.google.cloud:libraries-bom to v26.68.0 ([#442](https://github.com/statnett/kafka-ops-julie/issues/442)) ([9cdebf8](https://github.com/statnett/kafka-ops-julie/commit/9cdebf8c11d3f9bc19a75d778f5ca54a1f04ea61))
* **deps:** update dependency com.google.cloud:libraries-bom to v26.69.0 ([#461](https://github.com/statnett/kafka-ops-julie/issues/461)) ([5887203](https://github.com/statnett/kafka-ops-julie/commit/5887203a731aa3e9a712b7656635e5a6fecfed7e))
* **deps:** update dependency com.hubspot.jinjava:jinjava to v2.8.1 ([#444](https://github.com/statnett/kafka-ops-julie/issues/444)) ([8ce03aa](https://github.com/statnett/kafka-ops-julie/commit/8ce03aa76ef82922172a7d50e69925c14f24b389))
* **deps:** update dependency com.typesafe:config to v1.4.5 ([#434](https://github.com/statnett/kafka-ops-julie/issues/434)) ([fd23004](https://github.com/statnett/kafka-ops-julie/commit/fd23004a15238d7cab20ac867bda514893cee7e3))
* **deps:** update dependency io.confluent.ksql:ksqldb-api-client to v8.0.1 ([#460](https://github.com/statnett/kafka-ops-julie/issues/460)) ([c0148da](https://github.com/statnett/kafka-ops-julie/commit/c0148daef465c1331f7f41c07e1a6440810778d6))
* **deps:** update dependency org.apache.logging.log4j:log4j-bom to v2.25.2 ([#454](https://github.com/statnett/kafka-ops-julie/issues/454)) ([1cd90a2](https://github.com/statnett/kafka-ops-julie/commit/1cd90a2488461621c4ada4817cff386a2adbda41))
* **deps:** update dependency org.projectlombok:lombok to v1.18.40 ([#425](https://github.com/statnett/kafka-ops-julie/issues/425)) ([bc2144c](https://github.com/statnett/kafka-ops-julie/commit/bc2144c720fb49526918f9c8072e6f5aa720c679))
* **deps:** update dependency org.projectlombok:lombok to v1.18.42 ([#447](https://github.com/statnett/kafka-ops-julie/issues/447)) ([9463b6e](https://github.com/statnett/kafka-ops-julie/commit/9463b6e7effdf1dac94453a3e9248a5b32927f01))
* **deps:** update dependency software.amazon.awssdk:bom to v2.33.10 ([#443](https://github.com/statnett/kafka-ops-julie/issues/443)) ([c678d20](https://github.com/statnett/kafka-ops-julie/commit/c678d20b152f2a9cb520e1c75dbf58d6c628dd3f))
* **deps:** update dependency software.amazon.awssdk:bom to v2.33.11 ([#445](https://github.com/statnett/kafka-ops-julie/issues/445)) ([9e6a099](https://github.com/statnett/kafka-ops-julie/commit/9e6a099a529fc4a2551f46e581e4abdc193863bb))
* **deps:** update dependency software.amazon.awssdk:bom to v2.33.12 ([#448](https://github.com/statnett/kafka-ops-julie/issues/448)) ([5584c9f](https://github.com/statnett/kafka-ops-julie/commit/5584c9fab532fb75eeffc80d2da595e9562a62f2))
* **deps:** update dependency software.amazon.awssdk:bom to v2.33.3 ([#423](https://github.com/statnett/kafka-ops-julie/issues/423)) ([d48d406](https://github.com/statnett/kafka-ops-julie/commit/d48d40624969b810cc5ae604127f2e1b5bc2855f))
* **deps:** update dependency software.amazon.awssdk:bom to v2.33.4 ([#427](https://github.com/statnett/kafka-ops-julie/issues/427)) ([11c6210](https://github.com/statnett/kafka-ops-julie/commit/11c621083e269d531e1c6cc6e0ab07d48646d5f8))
* **deps:** update dependency software.amazon.awssdk:bom to v2.33.5 ([#429](https://github.com/statnett/kafka-ops-julie/issues/429)) ([d1d0358](https://github.com/statnett/kafka-ops-julie/commit/d1d03589915102a08a592a4e59f6e5b6787c43f2))
* **deps:** update dependency software.amazon.awssdk:bom to v2.33.6 ([#433](https://github.com/statnett/kafka-ops-julie/issues/433)) ([bba6faf](https://github.com/statnett/kafka-ops-julie/commit/bba6faff12dead87813955535e2196c105d1918b))
* **deps:** update dependency software.amazon.awssdk:bom to v2.33.9 ([#438](https://github.com/statnett/kafka-ops-julie/issues/438)) ([d3c529d](https://github.com/statnett/kafka-ops-julie/commit/d3c529d088255ee767437ce6a360f786158ec9b2))
* **deps:** update dependency software.amazon.awssdk:bom to v2.34.0 ([#450](https://github.com/statnett/kafka-ops-julie/issues/450)) ([9ae773d](https://github.com/statnett/kafka-ops-julie/commit/9ae773dab0e9739be0a33e6c06a093916862ad56))
* **deps:** update dependency software.amazon.awssdk:bom to v2.34.3 ([#456](https://github.com/statnett/kafka-ops-julie/issues/456)) ([e3bd0a4](https://github.com/statnett/kafka-ops-julie/commit/e3bd0a43d40732ebd6fd36ebde27226facceb65b))
* **deps:** update dependency software.amazon.awssdk:bom to v2.34.5 ([#458](https://github.com/statnett/kafka-ops-julie/issues/458)) ([c48a201](https://github.com/statnett/kafka-ops-julie/commit/c48a201336563e3e99ee1d76aa6f3a4bf463d7a3))
* **deps:** update dependency software.amazon.awssdk:bom to v2.34.6 ([#462](https://github.com/statnett/kafka-ops-julie/issues/462)) ([e538a15](https://github.com/statnett/kafka-ops-julie/commit/e538a15dcf8cf6e388defd65ec8762939eafd879))
* **deps:** update dependency software.amazon.awssdk:bom to v2.34.7 ([#463](https://github.com/statnett/kafka-ops-julie/issues/463)) ([51bcf0e](https://github.com/statnett/kafka-ops-julie/commit/51bcf0eaf3c6530672ad8abd17154d9f6b92a508))
* **deps:** update dependency software.amazon.awssdk:bom to v2.34.9 ([#465](https://github.com/statnett/kafka-ops-julie/issues/465)) ([27c6b07](https://github.com/statnett/kafka-ops-julie/commit/27c6b07ad3628187e9720c902083b96b3545106d))


### Documentation

* specify how to run integration tests on Apple Silicon ([#437](https://github.com/statnett/kafka-ops-julie/issues/437)) ([0b1f091](https://github.com/statnett/kafka-ops-julie/commit/0b1f0915628f25f37eaa33107d039de080d26aba))

## [5.2.0](https://github.com/statnett/kafka-ops-julie/compare/v5.1.12...v5.2.0) (2025-09-04)


### Features

* make the half-baked roles concept actually work ([#412](https://github.com/statnett/kafka-ops-julie/issues/412)) ([b99658e](https://github.com/statnett/kafka-ops-julie/commit/b99658e49db33dd048fe17bcb23e63a95778e671))


### Bug Fixes

* **deps:** update dependency com.fasterxml.jackson:jackson-bom to v2.20.0 ([#415](https://github.com/statnett/kafka-ops-julie/issues/415)) ([5540bab](https://github.com/statnett/kafka-ops-julie/commit/5540bab8d6e590f33feebe505af2dd1d91d0ddf5))
* **deps:** update dependency com.google.cloud:libraries-bom to v26.65.0 ([#391](https://github.com/statnett/kafka-ops-julie/issues/391)) ([906fb2b](https://github.com/statnett/kafka-ops-julie/commit/906fb2bed6b5d2568c449305090adde8c1516b49))
* **deps:** update dependency com.google.cloud:libraries-bom to v26.66.0 ([#403](https://github.com/statnett/kafka-ops-julie/issues/403)) ([9378e4e](https://github.com/statnett/kafka-ops-julie/commit/9378e4e3b90eb66d790e9849780f8627f874f4b0))
* **deps:** update dependency com.google.cloud:libraries-bom to v26.67.0 ([#416](https://github.com/statnett/kafka-ops-julie/issues/416)) ([55af83e](https://github.com/statnett/kafka-ops-julie/commit/55af83e1953a2c7ee33b7b9dbd3043061e5a83bc))
* **deps:** update dependency commons-cli:commons-cli to v1.10.0 ([#392](https://github.com/statnett/kafka-ops-julie/issues/392)) ([f5b05b7](https://github.com/statnett/kafka-ops-julie/commit/f5b05b761522c860590697169b824eacd4423246))
* **deps:** update dependency redis.clients:jedis to v6.1.0 ([#393](https://github.com/statnett/kafka-ops-julie/issues/393)) ([db85a4a](https://github.com/statnett/kafka-ops-julie/commit/db85a4a5211747ec7307791d6b088127870ca035))
* **deps:** update dependency redis.clients:jedis to v6.2.0 ([#414](https://github.com/statnett/kafka-ops-julie/issues/414)) ([998deae](https://github.com/statnett/kafka-ops-julie/commit/998deae0e03c595c004c352b8fd46d08925be147))
* **deps:** update dependency software.amazon.awssdk:bom to v2.32.11 ([#387](https://github.com/statnett/kafka-ops-julie/issues/387)) ([778fedc](https://github.com/statnett/kafka-ops-julie/commit/778fedc4cb927f00327351903e9b52bbec380bf5))
* **deps:** update dependency software.amazon.awssdk:bom to v2.32.12 ([#389](https://github.com/statnett/kafka-ops-julie/issues/389)) ([6f62544](https://github.com/statnett/kafka-ops-julie/commit/6f625442a238f43de0bd67e6ff960c33dd207a04))
* **deps:** update dependency software.amazon.awssdk:bom to v2.32.15 ([#390](https://github.com/statnett/kafka-ops-julie/issues/390)) ([e447ba6](https://github.com/statnett/kafka-ops-julie/commit/e447ba6041150cb64f19f0bb0f96cca5c7ef7eb9))
* **deps:** update dependency software.amazon.awssdk:bom to v2.32.16 ([#394](https://github.com/statnett/kafka-ops-julie/issues/394)) ([18abed2](https://github.com/statnett/kafka-ops-julie/commit/18abed2c73d7fea78bf435058abed0eba070714b))
* **deps:** update dependency software.amazon.awssdk:bom to v2.32.19 ([#396](https://github.com/statnett/kafka-ops-julie/issues/396)) ([1cbb179](https://github.com/statnett/kafka-ops-julie/commit/1cbb179f478a02e8aafc6f834cbd8282e61042ff))
* **deps:** update dependency software.amazon.awssdk:bom to v2.32.22 ([#399](https://github.com/statnett/kafka-ops-julie/issues/399)) ([ad90a76](https://github.com/statnett/kafka-ops-julie/commit/ad90a76ba54690060da9c93a3057771eeba66b22))
* **deps:** update dependency software.amazon.awssdk:bom to v2.32.23 ([#400](https://github.com/statnett/kafka-ops-julie/issues/400)) ([1fff8ed](https://github.com/statnett/kafka-ops-julie/commit/1fff8ed8f5eb4ede5e781363868df74e9448159e))
* **deps:** update dependency software.amazon.awssdk:bom to v2.32.24 ([#402](https://github.com/statnett/kafka-ops-julie/issues/402)) ([ee69a6f](https://github.com/statnett/kafka-ops-julie/commit/ee69a6f3a54cb17d6aa4ccfec1032ed10a99b832))
* **deps:** update dependency software.amazon.awssdk:bom to v2.32.25 ([#405](https://github.com/statnett/kafka-ops-julie/issues/405)) ([0e8b916](https://github.com/statnett/kafka-ops-julie/commit/0e8b916330b391335e7885cc5dafd4573037bb7d))
* **deps:** update dependency software.amazon.awssdk:bom to v2.32.26 ([#406](https://github.com/statnett/kafka-ops-julie/issues/406)) ([2d27926](https://github.com/statnett/kafka-ops-julie/commit/2d27926bcb93c9251b675061494972c603fc0500))
* **deps:** update dependency software.amazon.awssdk:bom to v2.32.27 ([#407](https://github.com/statnett/kafka-ops-julie/issues/407)) ([7647315](https://github.com/statnett/kafka-ops-julie/commit/7647315d16beba0201549974667002f0d25b967d))
* **deps:** update dependency software.amazon.awssdk:bom to v2.32.28 ([#409](https://github.com/statnett/kafka-ops-julie/issues/409)) ([8ed18ff](https://github.com/statnett/kafka-ops-julie/commit/8ed18ff79b772405b9936eeab5c056c9e428b552))
* **deps:** update dependency software.amazon.awssdk:bom to v2.32.29 ([#410](https://github.com/statnett/kafka-ops-julie/issues/410)) ([61025b0](https://github.com/statnett/kafka-ops-julie/commit/61025b00f7189c12709ecbcff4e65a8ae67d0250))
* **deps:** update dependency software.amazon.awssdk:bom to v2.32.30 ([#411](https://github.com/statnett/kafka-ops-julie/issues/411)) ([690ab7a](https://github.com/statnett/kafka-ops-julie/commit/690ab7a1af2f40ffe5a514d8171383746b8c7fcc))
* **deps:** update dependency software.amazon.awssdk:bom to v2.32.31 ([#413](https://github.com/statnett/kafka-ops-julie/issues/413)) ([8aaf25b](https://github.com/statnett/kafka-ops-julie/commit/8aaf25b4a2e3c2419e42986dac7977ec117f18e0))
* **deps:** update dependency software.amazon.awssdk:bom to v2.33.0 ([#417](https://github.com/statnett/kafka-ops-julie/issues/417)) ([01cd349](https://github.com/statnett/kafka-ops-julie/commit/01cd34908cf3f25b9abc8ee7ad6e878f234fc372))
* **deps:** update dependency software.amazon.awssdk:bom to v2.33.1 ([#421](https://github.com/statnett/kafka-ops-julie/issues/421)) ([0bc619a](https://github.com/statnett/kafka-ops-julie/commit/0bc619a3ebd6e261fbb1c8dec8d273cfb252553f))
* **deps:** update dependency software.amazon.awssdk:bom to v2.33.2 ([#422](https://github.com/statnett/kafka-ops-julie/issues/422)) ([8415332](https://github.com/statnett/kafka-ops-julie/commit/841533234260d3ea863bad6ae2eb69590f532fcb))

## [5.1.12](https://github.com/statnett/kafka-ops-julie/compare/v5.1.11...v5.1.12) (2025-07-29)


### Bug Fixes

* **deps:** update dependency com.fasterxml.jackson:jackson-bom to v2.19.2 ([#382](https://github.com/statnett/kafka-ops-julie/issues/382)) ([bfbc54b](https://github.com/statnett/kafka-ops-julie/commit/bfbc54b8714509422885891b8e51ffded9cd479c))
* **deps:** update dependency com.google.cloud:libraries-bom to v26.63.0 ([#373](https://github.com/statnett/kafka-ops-julie/issues/373)) ([01ad5ee](https://github.com/statnett/kafka-ops-julie/commit/01ad5ee1cd0abf09944dc9769c3c813f1350b9c2))
* **deps:** update dependency com.google.cloud:libraries-bom to v26.64.0 ([#380](https://github.com/statnett/kafka-ops-julie/issues/380)) ([4a58a10](https://github.com/statnett/kafka-ops-julie/commit/4a58a1015d48011520b4733c5c740871b07a5702))
* **deps:** update dependency com.typesafe:config to v1.4.4 ([#378](https://github.com/statnett/kafka-ops-julie/issues/378)) ([f23be5c](https://github.com/statnett/kafka-ops-julie/commit/f23be5c85258440ea3d3ae892c55c02258c58829))
* **deps:** update dependency org.apache.logging.log4j:log4j-bom to v2.25.1 ([#379](https://github.com/statnett/kafka-ops-julie/issues/379)) ([7bc06fb](https://github.com/statnett/kafka-ops-julie/commit/7bc06fb60b2e6e7d56eee6321d7fb55e55d4184e))
* **deps:** update dependency software.amazon.awssdk:bom to v2.31.71 ([#368](https://github.com/statnett/kafka-ops-julie/issues/368)) ([a9a5a11](https://github.com/statnett/kafka-ops-julie/commit/a9a5a113e3ed883bf2504dd462379a05724e1ca9))
* **deps:** update dependency software.amazon.awssdk:bom to v2.31.73 ([#370](https://github.com/statnett/kafka-ops-julie/issues/370)) ([ec6bab9](https://github.com/statnett/kafka-ops-julie/commit/ec6bab98427179ba51d4702874ea3d6c67198481))
* **deps:** update dependency software.amazon.awssdk:bom to v2.31.75 ([#372](https://github.com/statnett/kafka-ops-julie/issues/372)) ([b2de526](https://github.com/statnett/kafka-ops-julie/commit/b2de5266d8c09ed3e5f5bb475d01562bd1311a47))
* **deps:** update dependency software.amazon.awssdk:bom to v2.31.76 ([#374](https://github.com/statnett/kafka-ops-julie/issues/374)) ([5011214](https://github.com/statnett/kafka-ops-julie/commit/5011214fd7972e6d7ac96f30c8a94f2509675315))
* **deps:** update dependency software.amazon.awssdk:bom to v2.31.77 ([#375](https://github.com/statnett/kafka-ops-julie/issues/375)) ([223a8a2](https://github.com/statnett/kafka-ops-julie/commit/223a8a2869f23dc7ad6f4a1c1207b947bc62d4b8))
* **deps:** update dependency software.amazon.awssdk:bom to v2.32.10 ([#385](https://github.com/statnett/kafka-ops-julie/issues/385)) ([1e61bc5](https://github.com/statnett/kafka-ops-julie/commit/1e61bc5714e48ae317c9078f9a85c09507a5377d))
* **deps:** update dependency software.amazon.awssdk:bom to v2.32.2 ([#377](https://github.com/statnett/kafka-ops-julie/issues/377)) ([4b5a0e8](https://github.com/statnett/kafka-ops-julie/commit/4b5a0e8f3351b3e37209e53b621dd112ea7687e8))
* **deps:** update dependency software.amazon.awssdk:bom to v2.32.6 ([#381](https://github.com/statnett/kafka-ops-julie/issues/381)) ([d19f1dc](https://github.com/statnett/kafka-ops-julie/commit/d19f1dc39571d9b78cc901e3c5057ea728a889c9))
* **deps:** update dependency software.amazon.awssdk:bom to v2.32.9 ([#384](https://github.com/statnett/kafka-ops-julie/issues/384)) ([f9fce24](https://github.com/statnett/kafka-ops-julie/commit/f9fce2419b5d298ba31fa98d9e27aeb934a50c81))

## [5.1.11](https://github.com/statnett/kafka-ops-julie/compare/v5.1.10...v5.1.11) (2025-06-25)


### Bug Fixes

* **deps:** update dependency software.amazon.awssdk:bom to v2.31.65 ([#360](https://github.com/statnett/kafka-ops-julie/issues/360)) ([09fefd9](https://github.com/statnett/kafka-ops-julie/commit/09fefd99da5d392c85d69208f8bd16352b38c775))
* **deps:** update dependency software.amazon.awssdk:bom to v2.31.68 ([#363](https://github.com/statnett/kafka-ops-julie/issues/363)) ([a7d6874](https://github.com/statnett/kafka-ops-julie/commit/a7d68745b7f1840ced50bf0f62fea63458928e19))
* **deps:** update dependency software.amazon.awssdk:bom to v2.31.70 ([#364](https://github.com/statnett/kafka-ops-julie/issues/364)) ([0a61569](https://github.com/statnett/kafka-ops-julie/commit/0a61569946d2db6cd63280699e37e71f7d9157aa))
* upgrade Confluent Platform to version 8 ([#365](https://github.com/statnett/kafka-ops-julie/issues/365)) ([696b89e](https://github.com/statnett/kafka-ops-julie/commit/696b89e465ffc622304a3d6b8d17a2bbaad61c01))
* use CP v8 with KRaft, not ZK, for testcontainers ([#366](https://github.com/statnett/kafka-ops-julie/issues/366)) ([eabc351](https://github.com/statnett/kafka-ops-julie/commit/eabc351a9e81213db8494416ada2bd5bd25365b9))

## [5.1.10](https://github.com/statnett/kafka-ops-julie/compare/v5.1.9...v5.1.10) (2025-06-17)


### Bug Fixes

* **deps:** update confluent.version to v7.9.2 ([#355](https://github.com/statnett/kafka-ops-julie/issues/355)) ([1f416fa](https://github.com/statnett/kafka-ops-julie/commit/1f416fa00beacd3ec3b264f171e2d93f32a4a109))
* **deps:** update dependency com.fasterxml.jackson:jackson-bom to v2.19.1 ([#357](https://github.com/statnett/kafka-ops-julie/issues/357)) ([f54782a](https://github.com/statnett/kafka-ops-julie/commit/f54782a278a29294dd2eed8e7d9540c6c2e9accc))
* **deps:** update dependency io.confluent.ksql:ksqldb-api-client to v7.9.2 ([#356](https://github.com/statnett/kafka-ops-julie/issues/356)) ([cf052cd](https://github.com/statnett/kafka-ops-julie/commit/cf052cd9c9148b86eb8a591663ac99475c2fd36a))
* **deps:** update dependency org.apache.logging.log4j:log4j-bom to v2.25.0 ([#358](https://github.com/statnett/kafka-ops-julie/issues/358)) ([19972b6](https://github.com/statnett/kafka-ops-julie/commit/19972b6a7a29251202342328f12f3b40f2c34f90))
* **deps:** update dependency software.amazon.awssdk:bom to v2.31.54 ([#344](https://github.com/statnett/kafka-ops-julie/issues/344)) ([4b8e55f](https://github.com/statnett/kafka-ops-julie/commit/4b8e55f7229890f28def7751188ed81c428f1e4b))
* **deps:** update dependency software.amazon.awssdk:bom to v2.31.59 ([#347](https://github.com/statnett/kafka-ops-julie/issues/347)) ([b89bbf2](https://github.com/statnett/kafka-ops-julie/commit/b89bbf258547744643b14b34570376753ee08843))
* **deps:** update dependency software.amazon.awssdk:bom to v2.31.61 ([#349](https://github.com/statnett/kafka-ops-julie/issues/349)) ([5f4a3e7](https://github.com/statnett/kafka-ops-julie/commit/5f4a3e7a5b41b22ae9e22f6817a27d0e5a14cd99))
* **deps:** update dependency software.amazon.awssdk:bom to v2.31.64 ([#354](https://github.com/statnett/kafka-ops-julie/issues/354)) ([e3e6419](https://github.com/statnett/kafka-ops-julie/commit/e3e6419f75a474aaea2d7c2aeaf72b58025000b1))

## [5.1.9](https://github.com/statnett/kafka-ops-julie/compare/v5.1.8...v5.1.9) (2025-05-25)


### Bug Fixes

* **deps:** update confluent.version to v7.9.1 ([#333](https://github.com/statnett/kafka-ops-julie/issues/333)) ([ddf724f](https://github.com/statnett/kafka-ops-julie/commit/ddf724ff88b3ce79f369925dbb05d5ebf1859047))
* **deps:** update dependency com.hubspot.jinjava:jinjava to v2.8.0 ([#335](https://github.com/statnett/kafka-ops-julie/issues/335)) ([cd9b639](https://github.com/statnett/kafka-ops-julie/commit/cd9b639ed5660d88d43653ec26de46d761146e2b))
* **deps:** update dependency io.confluent.ksql:ksqldb-api-client to v7.9.1 ([#334](https://github.com/statnett/kafka-ops-julie/issues/334)) ([51d0869](https://github.com/statnett/kafka-ops-julie/commit/51d08696d2efefcaea3592b8a73eddd053cfa347))
* **deps:** update dependency redis.clients:jedis to v6 ([#328](https://github.com/statnett/kafka-ops-julie/issues/328)) ([7d0f55a](https://github.com/statnett/kafka-ops-julie/commit/7d0f55afaf5d73f14732e79c50bd0d32f9262e57))
* **deps:** update dependency software.amazon.awssdk:bom to v2.31.30 ([#325](https://github.com/statnett/kafka-ops-julie/issues/325)) ([278360e](https://github.com/statnett/kafka-ops-julie/commit/278360efce8e3880e4bee0d3033aad3ba4aa0e70))
* **deps:** update dependency software.amazon.awssdk:bom to v2.31.32 ([#327](https://github.com/statnett/kafka-ops-julie/issues/327)) ([99f352b](https://github.com/statnett/kafka-ops-julie/commit/99f352bd6422878c86e400a552aac5e3b0f33288))
* **deps:** update dependency software.amazon.awssdk:bom to v2.31.37 ([#329](https://github.com/statnett/kafka-ops-julie/issues/329)) ([d6fde35](https://github.com/statnett/kafka-ops-julie/commit/d6fde35220b6c7e471bc3082f77d7ca9f39a5e70))
* **deps:** update dependency software.amazon.awssdk:bom to v2.31.40 ([#332](https://github.com/statnett/kafka-ops-julie/issues/332)) ([da139af](https://github.com/statnett/kafka-ops-julie/commit/da139af4254cb98e4eee54487959180d67ca6492))
* **deps:** update dependency software.amazon.awssdk:bom to v2.31.41 ([#336](https://github.com/statnett/kafka-ops-julie/issues/336)) ([7417619](https://github.com/statnett/kafka-ops-julie/commit/7417619bb13bcc512456bdd5345d0ae7ec4d3335))
* **deps:** update dependency software.amazon.awssdk:bom to v2.31.42 ([#337](https://github.com/statnett/kafka-ops-julie/issues/337)) ([dc0959a](https://github.com/statnett/kafka-ops-julie/commit/dc0959a767427a2fd676f58aee12ad4baa44bcf0))
* **deps:** update dependency software.amazon.awssdk:bom to v2.31.45 ([#338](https://github.com/statnett/kafka-ops-julie/issues/338)) ([677b17a](https://github.com/statnett/kafka-ops-julie/commit/677b17abb4dd5e12187ada98a2c9faf1dc01ac38))
* **deps:** update dependency software.amazon.awssdk:bom to v2.31.46 ([#339](https://github.com/statnett/kafka-ops-julie/issues/339)) ([97fc647](https://github.com/statnett/kafka-ops-julie/commit/97fc647a56926b5a94367d707683e54dab4a12fe))
* **deps:** update dependency software.amazon.awssdk:bom to v2.31.48 ([#341](https://github.com/statnett/kafka-ops-julie/issues/341)) ([8dc1580](https://github.com/statnett/kafka-ops-julie/commit/8dc1580a960d9f7235ccffc81106b7e269f5566e))
* **deps:** update dependency software.amazon.awssdk:bom to v2.31.50 ([#342](https://github.com/statnett/kafka-ops-julie/issues/342)) ([188d607](https://github.com/statnett/kafka-ops-julie/commit/188d6076861466b18e578291a21e1a76c191acc1))
* **deps:** update kafka.version to v4 (major) ([#331](https://github.com/statnett/kafka-ops-julie/issues/331)) ([bd89099](https://github.com/statnett/kafka-ops-julie/commit/bd89099d3e35c3785283993e593b76eb4dc2b2d1))

## [5.1.8](https://github.com/statnett/kafka-ops-julie/compare/v5.1.7...v5.1.8) (2025-04-25)


### Bug Fixes

* **deps:** update dependency com.fasterxml.jackson:jackson-bom to v2.19.0 ([#322](https://github.com/statnett/kafka-ops-julie/issues/322)) ([1f192fc](https://github.com/statnett/kafka-ops-julie/commit/1f192fcb82c12b78bcb68c4dcc4c3bb628a0b778))
* **deps:** update dependency software.amazon.awssdk:bom to v2.31.29 ([#321](https://github.com/statnett/kafka-ops-julie/issues/321)) ([d58ccea](https://github.com/statnett/kafka-ops-julie/commit/d58ccea2b19ad536b26184d8235830329080b63b))

## [5.1.7](https://github.com/statnett/kafka-ops-julie/compare/v5.1.6...v5.1.7) (2025-04-24)


### Bug Fixes

* **deps:** update dependency org.projectlombok:lombok to v1.18.38 ([#304](https://github.com/statnett/kafka-ops-julie/issues/304)) ([0659020](https://github.com/statnett/kafka-ops-julie/commit/0659020bf5165c2e717a4a9ff8224c39f1fc9024))
* **deps:** update dependency software.amazon.awssdk:bom to v2.31.10 ([#300](https://github.com/statnett/kafka-ops-julie/issues/300)) ([84f96a2](https://github.com/statnett/kafka-ops-julie/commit/84f96a2d325c202a19c03eca7599fd20c76505fe))
* **deps:** update dependency software.amazon.awssdk:bom to v2.31.11 ([#301](https://github.com/statnett/kafka-ops-julie/issues/301)) ([d699ed2](https://github.com/statnett/kafka-ops-julie/commit/d699ed2edc6d75cb50b305ccbed0bcb3bcf6dfde))
* **deps:** update dependency software.amazon.awssdk:bom to v2.31.12 ([#305](https://github.com/statnett/kafka-ops-julie/issues/305)) ([90c1598](https://github.com/statnett/kafka-ops-julie/commit/90c1598a76b176af2517abadb4e924bb0374e86d))
* **deps:** update dependency software.amazon.awssdk:bom to v2.31.13 ([#306](https://github.com/statnett/kafka-ops-julie/issues/306)) ([1f6d9e7](https://github.com/statnett/kafka-ops-julie/commit/1f6d9e7c532a3044021f8f664ec7d7a1b6e9d3af))
* **deps:** update dependency software.amazon.awssdk:bom to v2.31.14 ([#307](https://github.com/statnett/kafka-ops-julie/issues/307)) ([fa6a27e](https://github.com/statnett/kafka-ops-julie/commit/fa6a27ed5803f45a7c807d7e8d3f52687e956ff0))
* **deps:** update dependency software.amazon.awssdk:bom to v2.31.16 ([#308](https://github.com/statnett/kafka-ops-julie/issues/308)) ([d4b7e99](https://github.com/statnett/kafka-ops-julie/commit/d4b7e99384aab81e33fc71f4b56e8b46277b56cc))
* **deps:** update dependency software.amazon.awssdk:bom to v2.31.17 ([#310](https://github.com/statnett/kafka-ops-julie/issues/310)) ([7112413](https://github.com/statnett/kafka-ops-julie/commit/711241340624247b111ba31bf7c0b4e5f406bcfb))
* **deps:** update dependency software.amazon.awssdk:bom to v2.31.18 ([#311](https://github.com/statnett/kafka-ops-julie/issues/311)) ([d466b31](https://github.com/statnett/kafka-ops-julie/commit/d466b31db918cd5ea6975c2cfa209ed36419ce61))
* **deps:** update dependency software.amazon.awssdk:bom to v2.31.21 ([#312](https://github.com/statnett/kafka-ops-julie/issues/312)) ([426ba21](https://github.com/statnett/kafka-ops-julie/commit/426ba21c849c050376fd108129d349de3e1b49ca))
* **deps:** update dependency software.amazon.awssdk:bom to v2.31.22 ([#313](https://github.com/statnett/kafka-ops-julie/issues/313)) ([f896f9f](https://github.com/statnett/kafka-ops-julie/commit/f896f9fa39c5a7cce6a45acb7faef7cc7c797b02))
* **deps:** update dependency software.amazon.awssdk:bom to v2.31.26 ([#315](https://github.com/statnett/kafka-ops-julie/issues/315)) ([288b452](https://github.com/statnett/kafka-ops-julie/commit/288b452366d4b884dad9210c7c7e4f8b526ecd9b))
* **deps:** update dependency software.amazon.awssdk:bom to v2.31.27 ([#317](https://github.com/statnett/kafka-ops-julie/issues/317)) ([f408bc4](https://github.com/statnett/kafka-ops-julie/commit/f408bc4c7735368e249746c9c73f17ba43e4781f))
* **deps:** update dependency software.amazon.awssdk:bom to v2.31.7 ([#296](https://github.com/statnett/kafka-ops-julie/issues/296)) ([f6248c8](https://github.com/statnett/kafka-ops-julie/commit/f6248c8ab1b27428b38cbfc9e15d9f31e0e3e79d))
* **deps:** update dependency software.amazon.awssdk:bom to v2.31.8 ([#298](https://github.com/statnett/kafka-ops-julie/issues/298)) ([5b37226](https://github.com/statnett/kafka-ops-julie/commit/5b37226c642897e5c3cbdecc83c8b89e67b8fc9a))
* **deps:** update dependency software.amazon.awssdk:bom to v2.31.9 ([#299](https://github.com/statnett/kafka-ops-julie/issues/299)) ([ac4a9f8](https://github.com/statnett/kafka-ops-julie/commit/ac4a9f8213806cf9f459b452e3bf21a100bc76f4))

## [5.1.6](https://github.com/statnett/kafka-ops-julie/compare/v5.1.5...v5.1.6) (2025-03-24)


### Bug Fixes

* avoid array overflow ([#291](https://github.com/statnett/kafka-ops-julie/issues/291)) ([4c31600](https://github.com/statnett/kafka-ops-julie/commit/4c31600692e1d24bd8265de2c1219087d886bd8e))
* **deps:** update dependency software.amazon.awssdk:bom to v2.31.5 ([#288](https://github.com/statnett/kafka-ops-julie/issues/288)) ([4fed8fd](https://github.com/statnett/kafka-ops-julie/commit/4fed8fda87c1956c9db79c0355a0bf5c1dd97149))
* **deps:** update dependency software.amazon.awssdk:bom to v2.31.6 ([#290](https://github.com/statnett/kafka-ops-julie/issues/290)) ([8c73c3d](https://github.com/statnett/kafka-ops-julie/commit/8c73c3dfe0c3686f9dfca6182b6b38344e5d9d97))
* make sure to read to the end of the state topic stream ([#294](https://github.com/statnett/kafka-ops-julie/issues/294)) ([f6f1926](https://github.com/statnett/kafka-ops-julie/commit/f6f19269fdd8d20730751a1a0a7299b3c4cad466))

## [5.1.5](https://github.com/statnett/kafka-ops-julie/compare/v5.1.4...v5.1.5) (2025-03-20)


### Bug Fixes

* **kafka-backend:** delay flushing until all messages are sent ([#285](https://github.com/statnett/kafka-ops-julie/issues/285)) ([6f773bb](https://github.com/statnett/kafka-ops-julie/commit/6f773bb3d75e5866d3814281ed343cea1fe0d552))

## [5.1.4](https://github.com/statnett/kafka-ops-julie/compare/v5.1.3...v5.1.4) (2025-03-20)


### Bug Fixes

* change commitAsync to commitSync ([#283](https://github.com/statnett/kafka-ops-julie/issues/283)) ([635cb64](https://github.com/statnett/kafka-ops-julie/commit/635cb6458bf9fa0059e25c101259469bdc4678ad))
* **deps:** update dependency software.amazon.awssdk:bom to v2.31.3 ([#280](https://github.com/statnett/kafka-ops-julie/issues/280)) ([4fe8f95](https://github.com/statnett/kafka-ops-julie/commit/4fe8f9566d77be1fa92cb3917772f0024eec8590))
* **deps:** update dependency software.amazon.awssdk:bom to v2.31.4 ([#282](https://github.com/statnett/kafka-ops-julie/issues/282)) ([0f632cd](https://github.com/statnett/kafka-ops-julie/commit/0f632cd1d359c1648b7bd596c39ece07a97a3c84))

## [5.1.3](https://github.com/statnett/kafka-ops-julie/compare/v5.1.2...v5.1.3) (2025-03-18)


### Bug Fixes

* **deps:** update dependency software.amazon.awssdk:bom to v2.30.37 ([#273](https://github.com/statnett/kafka-ops-julie/issues/273)) ([d65f62f](https://github.com/statnett/kafka-ops-julie/commit/d65f62f1d4392f31c228bcbb8071ac6dee1a7740))
* **deps:** update dependency software.amazon.awssdk:bom to v2.30.38 ([#275](https://github.com/statnett/kafka-ops-julie/issues/275)) ([375c079](https://github.com/statnett/kafka-ops-julie/commit/375c07932e1875874a90ffbe3eca63e55892ee77))
* **deps:** update dependency software.amazon.awssdk:bom to v2.31.1 ([#276](https://github.com/statnett/kafka-ops-julie/issues/276)) ([0cdb4d8](https://github.com/statnett/kafka-ops-julie/commit/0cdb4d8a10a47a9053c2f73b6644de9d85fd5260))
* **deps:** update dependency software.amazon.awssdk:bom to v2.31.2 ([#278](https://github.com/statnett/kafka-ops-julie/issues/278)) ([e4ae025](https://github.com/statnett/kafka-ops-julie/commit/e4ae025f7c5d42f10405e28e24b1e5ac1f7e094a))

## [5.1.2](https://github.com/statnett/kafka-ops-julie/compare/v5.1.1...v5.1.2) (2025-03-09)


### Bug Fixes

* **deps:** update dependency software.amazon.awssdk:bom to v2.30.33 ([#259](https://github.com/statnett/kafka-ops-julie/issues/259)) ([9fde3f7](https://github.com/statnett/kafka-ops-julie/commit/9fde3f73773311ee72737f652130d43799f5f1c8))
* **deps:** update dependency software.amazon.awssdk:bom to v2.30.34 ([#262](https://github.com/statnett/kafka-ops-julie/issues/262)) ([6105426](https://github.com/statnett/kafka-ops-julie/commit/6105426ea831ffdd12fa8c2bdee66bc6745bfac8))
* **deps:** update dependency software.amazon.awssdk:bom to v2.30.36 ([#270](https://github.com/statnett/kafka-ops-julie/issues/270)) ([dffe4e2](https://github.com/statnett/kafka-ops-julie/commit/dffe4e27de353c6e54a9dd84a7e9bfd411840d27))

## [5.1.1](https://github.com/statnett/kafka-ops-julie/compare/v5.1.0...v5.1.1) (2025-03-04)


### Bug Fixes

* **deps:** update dependency software.amazon.awssdk:bom to v2.30.32 ([#255](https://github.com/statnett/kafka-ops-julie/issues/255)) ([9651876](https://github.com/statnett/kafka-ops-julie/commit/96518760362d9bd3e48eb2f977ab21692b119820))

## [5.1.0](https://github.com/statnett/kafka-ops-julie/compare/v5.0.1...v5.1.0) (2025-03-03)


### Features

* use Apache instead of Confluent, where possible ([#250](https://github.com/statnett/kafka-ops-julie/issues/250)) ([5c348b1](https://github.com/statnett/kafka-ops-julie/commit/5c348b17edb2b83129dc5373b7c3e300c9426de1))


### Bug Fixes

* exclude troublesome dependency io.confluent:broker-plugins ([#248](https://github.com/statnett/kafka-ops-julie/issues/248)) ([2305e0b](https://github.com/statnett/kafka-ops-julie/commit/2305e0b4d1074e75e497fcc41f559249e96321a9))


### Documentation

* remove heavily outdated CONTRIBUTING.md ([#245](https://github.com/statnett/kafka-ops-julie/issues/245)) ([394f7f4](https://github.com/statnett/kafka-ops-julie/commit/394f7f4ddd0120bf515ada4f58ee15961b24e4e4))
* remove no longer applicable RELEASE.md ([#241](https://github.com/statnett/kafka-ops-julie/issues/241)) ([0ec79d1](https://github.com/statnett/kafka-ops-julie/commit/0ec79d1487fc737804379263e76eb5dbec750f99))
* remove non-informative file troubleshooting.md ([#242](https://github.com/statnett/kafka-ops-julie/issues/242)) ([730f4a8](https://github.com/statnett/kafka-ops-julie/commit/730f4a8cd5f7594f385a0f9b6110b74ba463ec40))
* remove redundant CODE_OF_CONDUCT.md ([#244](https://github.com/statnett/kafka-ops-julie/issues/244)) ([422d235](https://github.com/statnett/kafka-ops-julie/commit/422d2358d6ca70d90ac1631730363e15d6fa6a9c))

## [5.0.1](https://github.com/statnett/kafka-ops-julie/compare/v5.0.0...v5.0.1) (2025-03-02)


### Bug Fixes

* **deps:** update dependency com.fasterxml.jackson:jackson-bom to v2.18.3 ([#236](https://github.com/statnett/kafka-ops-julie/issues/236)) ([cd3f4d1](https://github.com/statnett/kafka-ops-julie/commit/cd3f4d191ee7fb87d4c14ac3e4da86556116bc39))
* **deps:** update dependency software.amazon.awssdk:bom to v2.30.31 ([#237](https://github.com/statnett/kafka-ops-julie/issues/237)) ([1e1b9fe](https://github.com/statnett/kafka-ops-julie/commit/1e1b9fe37598fb460e7826ecab7129b15e57135f))
* remove unused code/files ([#239](https://github.com/statnett/kafka-ops-julie/issues/239)) ([4d4ae1e](https://github.com/statnett/kafka-ops-julie/commit/4d4ae1ea55720f484113afb77b16ebbbd43cb5f7))

## [5.0.0](https://github.com/statnett/kafka-ops-julie/compare/v4.8.5...v5.0.0) (2025-02-28)


### ⚠ BREAKING CHANGES

* build for Java 21 instead of 11 ([#232](https://github.com/statnett/kafka-ops-julie/issues/232))

### Features

* build for Java 21 instead of 11 ([#232](https://github.com/statnett/kafka-ops-julie/issues/232)) ([88b3057](https://github.com/statnett/kafka-ops-julie/commit/88b3057af4007cf265d90e3bc00384a9fb0101e4))

## [4.8.5](https://github.com/statnett/kafka-ops-julie/compare/v4.8.4...v4.8.5) (2025-02-28)


### Bug Fixes

* **deps:** update dependency software.amazon.awssdk:bom to v2.30.30 ([#230](https://github.com/statnett/kafka-ops-julie/issues/230)) ([c05a59b](https://github.com/statnett/kafka-ops-julie/commit/c05a59b802f07495e64192a51009690fd5e28feb))
* **deps:** update dependency software.amazon.awssdk:bom to v2.30.6 ([#177](https://github.com/statnett/kafka-ops-julie/issues/177)) ([0498226](https://github.com/statnett/kafka-ops-julie/commit/04982269772fee27cefc0e1836660f4c7f23b3bc))

## [4.8.4](https://github.com/statnett/kafka-ops-julie/compare/v4.8.3...v4.8.4) (2025-02-27)


### Bug Fixes

* allow totally empty state topic ([#225](https://github.com/statnett/kafka-ops-julie/issues/225)) ([4c93378](https://github.com/statnett/kafka-ops-julie/commit/4c93378f70eab3de341cf9a070d538a16a0f4fdf))

## [4.8.3](https://github.com/statnett/kafka-ops-julie/compare/v4.8.2...v4.8.3) (2025-02-27)


### Bug Fixes

* allow dry-run with non-existing state topic ([#221](https://github.com/statnett/kafka-ops-julie/issues/221)) ([4f7e6f5](https://github.com/statnett/kafka-ops-julie/commit/4f7e6f53979ddf5cae5595a520153ac3b3c607ba))

## [4.8.2](https://github.com/statnett/kafka-ops-julie/compare/v4.8.1...v4.8.2) (2025-02-27)


### Bug Fixes

* exclude jackson-module-afterburner ([#218](https://github.com/statnett/kafka-ops-julie/issues/218)) ([a05fc54](https://github.com/statnett/kafka-ops-julie/commit/a05fc5490382a2f72608bff7ce35ba6ef3f226fb))

## [4.8.1](https://github.com/statnett/kafka-ops-julie/compare/v4.8.0...v4.8.1) (2025-02-26)


### Bug Fixes

* exclude logger implementation to get logs again ([#215](https://github.com/statnett/kafka-ops-julie/issues/215)) ([aba3f41](https://github.com/statnett/kafka-ops-julie/commit/aba3f4133ea0ae761c9d04712ca5b4596c9c6c54))

## [4.8.0](https://github.com/statnett/kafka-ops-julie/compare/v4.7.3...v4.8.0) (2025-02-26)


### Features

* add chunking to Kafka-based state backend ([#208](https://github.com/statnett/kafka-ops-julie/issues/208)) ([d74b8ce](https://github.com/statnett/kafka-ops-julie/commit/d74b8ce3561380fd149c266815750c05f09b90e9))
* ditch the zookeeper dependency ([#205](https://github.com/statnett/kafka-ops-julie/issues/205)) ([4c25f7a](https://github.com/statnett/kafka-ops-julie/commit/4c25f7af7d661a062aa3c5f729307e369058207a))


### Bug Fixes

* add transform to fix log4j shading confusion ([#212](https://github.com/statnett/kafka-ops-julie/issues/212)) ([025cb8b](https://github.com/statnett/kafka-ops-julie/commit/025cb8bce76a422602d7bb6b6bf2d8136194f281))
* correct order of partitions/replicas for NewTopic ([#211](https://github.com/statnett/kafka-ops-julie/issues/211)) ([d1806b4](https://github.com/statnett/kafka-ops-julie/commit/d1806b415b2a34f071b5f1df23e4076582772e2d))
* **deps:** update dependency org.slf4j:slf4j-api to v2.0.17 ([#210](https://github.com/statnett/kafka-ops-julie/issues/210)) ([f240d96](https://github.com/statnett/kafka-ops-julie/commit/f240d9640feab36201321589b24eb6582d814884))
* ensure Kafka state topic is compact ([#209](https://github.com/statnett/kafka-ops-julie/issues/209)) ([12b05ab](https://github.com/statnett/kafka-ops-julie/commit/12b05ab79f18826c2f01c5e401bba382c1dbee36))
* exclude dependencies pulled from Confluent snapshot repo ([#206](https://github.com/statnett/kafka-ops-julie/issues/206)) ([8761e53](https://github.com/statnett/kafka-ops-julie/commit/8761e535c160a9d25151bd86df16ab7596e1f5c1))
* exclude overlapping dependencies ([#213](https://github.com/statnett/kafka-ops-julie/issues/213)) ([efb43f2](https://github.com/statnett/kafka-ops-julie/commit/efb43f2956d8d7fa89b723e9cf1c42be7a594a88))

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
