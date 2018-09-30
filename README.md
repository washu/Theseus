# Theseus · [![Build Status](https://travis-ci.com/eloquentlabs/Theseus.svg?branch=master)](https://travis-ci.com/eloquentlabs/Theseus)

[Eloquent Labs](https://www.eloquent.ai)' implementation of the 
[Raft distributed consensus algorithm](https://raft.github.io/).

Raft is an algorithm to ensure that every node (computer) in a cluster agrees 
on a given state value at all times.
For example, Raft can be used to implement a distributed key-value store,
 distributed locks, or sharding work across a number of worker nodes.

Theseus is written in Java, with a special emphasis on being robust
to modern cloud infrastructure (e.g., [Kubernetes](https://kubernetes.io/))
and general ease-of-use of the API.

## Table of Contents
* [Features](#features)
* [Installation](#installation)
    * [From Maven](#from-maven)
    * [From Gradle](#from-gradle)
    * [From Source](#from-source)
* [Usage](#usage)
    * [Create a node](#create-a-node)
    * [The key-value store](#the-key-value-store)
    * [Distributed locks](#distributed-locks)
    * [Change listeners](#change-listeners)
    * [Error handling](#error-handling)
* [Caveats](#caveats)
* [Contributing](#contributing)
* [Versioning](#versioning)
* [Authors](#authors)
* [License](#license)
* [References](#references)


## Features

There are a [huge number](https://raft.github.io/#implementations) of Raft
implementations out there, and at Eloquent we looked through a good number of
them before deciding to build our own.
There are a number of things we wanted from Raft that we could not find in
any implementation -- either because it was not a priority for the project
at the time, or in some cases because the change necessitates weakening
some of Raft's core guarantees.

This section outlines some of these main features, and also serves as a
list of ways in which Theseus is different from other Raft implementations:

1. **Kubernetes-aware Raft**. Theseus handles dynamically changing membership
   on Kubernetes. That is, when code is redeployed, Kubernetes takes
   the current set of nodes offline, and brings online a new set of nodes
   with different IP addresses. While this has a lot of nice properties in
   general, it does break one of Raft's core requirements: if a box goes
   down, it is either removed manually from the cluster, or it will at some
   point return with the same IP.

   Theseus -- in fact, the motivation behind the name itself -- is robust
   to these sorts of redeploys by dynamically reconfiguring the cluster
   as old nodes die and new nodes come online. This is, in practice,
   extremely useful; though it does lend itself to a rare new failure type 
   (see the [caveats](#caveats) section) in the case of a prolonged and 
   carefully crafted set of network partitions. In the general case, it's
   impossible to distinguish a killed node from a very long network
   partition.

2. **Distributed Locks**. Theseus implements distributed locks.
   In addition, Eloquent implements distributed locks _that unlock
   themselves in case of node failure_.
   For example, on an unpleasant `kill -9`, where the node
   does not have time to clean up after itself at all, much less update Raft's
   distributed state reliably.

3. **Change Listeners**. Theseus allows for arbitrary user code to listern to
   changes to its underlying key-value store. This allows Theseus to act as a
   sort of low-bandwidth pseudo-messaging service, where listening nodes are 
   informed of changes to the value of a given key.

4. **Simple, Safe APIS**. We constructed our top-level API to be as simple and
   safe as possible. Mutating elements in the key-value store ensures locks are
   taken on that element before the mutation happens, and are guaranteed to 
   release safely afterwards. Distributed locks have an expiration date by 
   default after which they are automatically released (in addition to being
   released if the holding node goes down). Raft threads are sandboxed from
   user-space threads, to ensure that the core Raft algorithm won't falter from
   misbehaving user code. And so on.


## Installation

TODO Talk about the different installation methods.

### From Maven

TODO Push to Maven and describe installation

### From Gradle

TODO Push to Gradle and describe installation

### From Source

TODO how to download + install

Theseus requires a number of dependencies in order to run. These are:

* [Gradle](https://gradle.org/) - Dependency management.
* [SLF4J](https://www.slf4j.org/) - For unified logging.
* [gRPC](https://grpc.io/) - RPC library.
* [protobuf](https://developers.google.com/protocol-buffers/) - Interchange and data storage format.

For running the tests, Theseus additionally depends on:

* [JUnit 4](https://junit.org/junit4/) - Unit testing.
* [Guava](https://github.com/google/guava) - Utility classes for Java.


##Usage

TODO Introduce the main API and link to subsections

###Create a node

TODO Go over the constructors and what they mean.

###The key-value store

TODO Go over getting, setting, and removing values from the store

###Distributed locks

TODO Go over taking and releasing locks

###Change listeners

TODO Go over adding and removing change listeners

###Error handling

TODO Go over error listeners and Prometheus monitoring.


## Caveats

TODO Go over CAP theorem theory


## Contributing

Please read [CONTRIBUTING.md](https://gist.github.com/eloquentlabs/theseus/CONTRIBUTING.md)
for details on our code of conduct, and the process for submitting pull 
requests to us.


## Versioning
* v0.3.0 [2018-10-??] - Initial public release

For the versions available, see the 
[releases page](https://github.com/eloquentlabs/theseus/releases). 


## Authors

Theseus is a collaborative effort of many of us here at Eloquent, and we hope
others will join in as well.
The current main authors are:

* [**Gabor Angeli**](https://github.com/gangeli)
* [**Keenon Werling**](https://github.com/keenon)
* [**Zames Chua**](https://github.com/zameschua)

Also, special thanks to the rest of the team here at Eloquent Labs!
Subscribe to our [blog](https://blog.eloquent.ai/) for updates on our technology.

![cropped-group_brick-1](https://user-images.githubusercontent.com/18271085/45569463-76653100-b814-11e8-9b12-6a6ac344ef76.jpg)


## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details


## References

* [Stanford CoreNLP](https://github.com/stanfordnlp/CoreNLP)
* [Diego Ongaro's work on Raft Consensus algorithm](https://raft.github.io/)

