# Statnett's fork of kafka-ops/julie

This is our fork of the [kafka-ops/julie](https://github.com/kafka-ops/julie) project.
For the original README, go to that project.

We used to contribute heavily to the Julie project while it was still being maintained.
After the maintainers stopped reacting to PRs, we continued doing updates and bug fixes in this fork.
The purpose of the fork is for us to be able to continue using Julie, although it is no longer being maintained.

The output from our fork is a single, versioned jar-file, published on GitHub.
No container images, no back-porting of fixes to older major versions, no publishing to Maven Central.
Just the stuff we need.

## Tech Notes

### Running Integration Tests on Apple Silicon

Unfortunately, Confluent does not provide an ARM64 build of the image `confluentinc/ksqldb-server`.
On modern Macs without Intel, this container will be emulated.
You must instruct your Docker Desktop for Mac to "Use Rosetta for x86_64/amd64 emulation on Apple Silicon", otherwise KSQL tests will time out.
You find this option under "Settings->General" in Docker Desktop.
