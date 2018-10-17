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

**Note that this is a pre-release; APIs are liable to change with minor versions**

## Table of Contents
* [Features](#features)
* [Installation](#installation)
    * [From Maven](#from-maven)
    * [From Gradle](#from-gradle)
    * [From Source](#from-source)
* [Usage](#usage)
    * [Create a server](#create-a-server)
    * [The key-value store](#the-key-value-store)
    * [Distributed locks](#distributed-locks)
    * [Change listeners](#change-listeners)
    * [Error handling](#error-handling)
* [Caveats](#caveats)
* [Contributing](#contributing)
* [Versioning](#versioning)
* [Authors](#authors)
* [License](#license)


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

3. **Change Listeners**. Theseus allows for arbitrary user code to listen to
   changes to its underlying key-value store. This allows Theseus to act as a
   sort of low-bandwidth pseudo-messaging service, where listening nodes are 
   informed of changes to the value of a given key.

4. **Simple, Safe APIs**. We constructed our top-level API to be as simple and
   safe as possible. Mutating elements in the key-value store ensures locks are
   taken on that element before the mutation happens, and are guaranteed to 
   release safely afterwards. Distributed locks have an expiration date by 
   default after which they are automatically released (in addition to being
   released if the holding node goes down). Raft threads are sandboxed from
   user-space threads, to ensure that the core Raft algorithm won't falter from
   misbehaving user code. And so on.


## Installation

Theseus can be installed from either Maven Central, or directly from source.
Each of these is documented below:

### From Maven

The most recent version of Theseus on Maven can be found
[here](https://search.maven.org/artifact/ai.eloquent/theseus/0.3.0/jar).
It can be included in a Maven project by adding the following
to the `<dependencies>` section of the project's `pom.xml` file:

```xml
<dependency>
    <groupId>ai.eloquent</groupId>
    <artifactId>theseus</artifactId>
    <version>0.3.0</version>
</dependency>
```

### From Gradle

Installation from Gradle is similar to Maven. The most recent release
can be found
[here](https://search.maven.org/artifact/ai.eloquent/theseus/0.3.0/jar).
It can be included in a Gradle project by adding the following
to the `dependencies` section of the project's `build.gradle` file:

```
compile group: 'ai.eloquent', name: 'theseus', version: '0.3.0'
```

### From Source

Theseus uses Gradle and Make as its build system.
You can build the project either by calling:

    make

or by invoking Gradle directly:

    cd theseus; ./gradlew compileJava processResources


If you want to compile directly without using Gradle, you'll need to ensure
that the following dependencies are in your classpath:

* [SLF4J](https://www.slf4j.org/) - For unified logging.
* [gRPC](https://grpc.io/) - RPC library.
* [protobuf](https://developers.google.com/protocol-buffers/) - Interchange and data storage format.

For running the tests, Theseus additionally depends on:

* [JUnit 4](https://junit.org/junit4/) - Unit testing.
* [Guava](https://github.com/google/guava) - Utility classes for Java.

Theseus is also released on GitHub in the 
[releases section](https://github.com/eloquentlabs/Theseus/releases).


## Usage

This section goes over the basics of how to use Theseus. More in-depth
documentation is coming soon on the 
[wiki](https://github.com/eloquentlabs/Theseus/wiki).

### Create a server

A _server_ is Raft "node," consisting of a replica of the key-value
state machine and a transport on which the server can listen for and send RPCs
to other servers in the cluster. Although it's possible to have multiple servers
on a single machine -- in fact, the unit tests do this regularly -- in most
situations a single machine will have only one running server.

At a high level, there are two ways to create a Raft server:

1. If the cluster is known in advance, the server can (and should) be told about
   the other servers in the cluster. This is done by constructing a new `Theseus`
   object with the appropriate quorum. For example, if my server name is 192.168.1.1,
   and I have two other members in my quorum: 192.168.1.2 and 192.168.1.3, I can create a
   Raft implementation with:

   ```java
   import java.util.Arrays;
   import ai.eloquent.raft.Theseus;
   ...

   Theseus raft = new Theseus("192.168.1.1", Arrays.asList("192.168.1.1", "192.168.1.2", "192.168.1.3"));
   ```

   Note that the server names have to be valid IP addresses, or a valid IP 
   address followed by an underscore (`_`) and a user-specified name (e.g.,
   `192.168.1.1_achilles`).

2. If the cluster is not known in advance -- for example, if running on
   Kubernetes -- a Theseus instance can be created with just the target
   quorum size. This can be done with something like the following:

   ```java
   import ai.eloquent.raft.Theseus;
   ...

   Theseus raft = new Theseus(3);  // create a quorum with 3 members
   ```

In either of these cases, you then need to _bootstrap_ the quorum.
Bootstrapping is idempotent, which means calling it multiple times, or calling
it from multiple servers, is harmless. However, traditionally, only one of
the servers should be bootstrapped, and the others should automatically
detect the bootstrapped server and join the quorum.

The call to bootstrap the server is straightforward:

```java
Theseus raft = ...;
raft.bootstrap();
```

That's it! With a Theseus instance created and bootstrapped, 
Raft you're ready to go and receive requests.
The following sections go over some of the common features of Theseus.

### The key-value store

Although Theseus can work with any number of state machines (as per the
Raft spec), the most common one and the one included in the distribution
is a key-value state machine. That is, String keys are used to store
arbitrary byte values.

Note that all of the operations on the state machine are asynchronous, 
returning a `CompletableFuture` for when the computation finishes. To
ensure that this is clear in the calling code, most of the public methods
on `Theseus` are suffixed with `Async`.

The recommended way to access an element in the key value store is with the
`withElementAsync()` function. This will take a 
[distributed lock](#distributed-locks) on the given element, create or mutate
it, and then release the lock. This comes at the overhead of an additional
RPC call (one to take the lock, one to mutate the value + release the lock
as a single operation). 
The `withElementAsync()` function takes four arguments:

1. The key of the element we're mutating.
2. A mutator, which takes a byte array and returns a mutated
   byte array.
3. An optional creator. This is a `Supplier` that produces a new instance,
   if nothing is in the key value store.
   Note that either the creator or the mutator is run, but not both in the
   same call.
4. A boolean indicating whether this value is permanent. Permanent values are
   persisted even if the machine that created the value is
   removed from the cluster. In most cases, this should be set to `true`, 
   though there are some use-cases where you want Raft to "garbage collect"
   values whose creator is permanently down (e.g., tracking connected
   sessions -- if a server goes down, sessions connected to it should 
   disappear).

An example usage is given below, representing a simple single-byte counter with
a key name of "counter":

```java
Theseus raft = ...;
raft.bootstrap();
...

raft.withElementAsync(
    "counter",  // The key we're saving the element under
    v -> {      // The mutator
      byte[] mutated = new byte[1];
      mutated[0] = (byte) (v[0] + 1);
      return mutated;
    },
    () -> new byte[1],  // The creator
    true                // Store this value permanently
);
```

In addition, Theseus implements the usual get/put/delete methods -- though
without taking a lock on an element, you of course risk race conditions
if two servers are writing at once.
These remaining methods are:

* `Optional<byte[]> getElement(String key)`: Gets an element at the given key,
  or `Optional.empty()` if none exists.
* `CompletableFuture<Boolean> setElementAsync(String key, byte[] value, boolean permanent, Duration timeout)`: 
  Sets the given bytes value for the given key. Like `withElementAsync()`, 
  permanent denotes whether this object should persist even if the creator 
  (as distinct from the last setter!) becomes permanently unavailable.
  The timeout argument specifies a timeout after which the set is considered 
  failed, and the system stops retrying and fails the future.
* `CompletableFuture<Boolean> removeElementAsync(String key, Duration timeout)`:
  Removes an element from the key value state machine.
  The timeout argument specifies a timeout after which the set is considered 
  failed, and the system stops retrying and fails the future.

### Distributed locks

One of the more commonly used features of distributed consensus algorithms are
distributed locks. Theseus' locks have the following guarantees:

1. If a lock is taken on a given server, no other live server holds the lock 
   (this is the basic lock guarantee).
2. If a lock is held by a server that is considered _dead_ (i.e., has been down
   for longer than the specified machine down timeout or explicitly shutdown), 
   then that lock is
   automatically released. While useful in practice, this does create the 
   possibility of two servers holding a lock if there is a prolonged network
   partition. See the [caveats](#caveats) section below.
3. A lock can be automatically released after a user-specified safety window, 
   to prevent a server from accidentally keeping a critical lock past when
   it should.
4. Locks that could not release because there was no valid quorum are
   automatically retried until a quorum is restored.

The central theme in all of these guarantees is an emphasis on ensuring
that locks are released when they should be, to prevent deadlocks.

In addition to the implicit locks taken in `withElementAsync()`, a lock can be
explicitly taken either with `withDistributedLockAsync()` -- which handles
releasing the lock automatically -- or with `tryLock()` or `tryLockAsync()`,
which return explicit lock objects.

A sample usage of `withElementAsync()` could be:

```java
Theseus raft = ...
raft.bootstrap();
...

System.out.println("I am taking the lock");
CompletableFuture<Boolean> future = raft.withDistributedLockAsync("my-lock", () ->
  System.out.println("I have the lock")
);
try {
  boolean success = future.get();
  System.out.println("I released the lock and " + (success ? "ran" : "did not run") + " the code in the runnable");
} catch (InterruptedException | ExecutionException e) {
  e.printStackTrace();
}
```

A sample usage of `tryLock()` could be:

```java
Theseus raft = ...
raft.bootstrap();
...

System.out.println("I am taking the lock");
Optional<Theseus.LongLivedLock> lock = raft.tryLock("my-lock", Duration.ofDays(365));
if (lock.isPresent()) {
  try {
    System.out.println("I have the lock");
  } finally {
    try {
      lock.get().release().get();
      System.out.println("I released the lock");
    } catch (InterruptedException | ExecutionException e) {
      System.out.println("Theseus will try release the lock automatically later");
    }
  }
}

```

### Change listeners

One of the more useful features of Theseus is the implementation of 
_change listeners_. A change listener is a block of code that is executed 
whenever a value is changed in the key-value state machine.
This is useful for, e.g., updating the frontends of all connected sessions
when a value is changed on the backend.
You can think of change listeners as a heavyweight version of messaging
services like RabbitMQ or Kafka.

A change listener takes as arguments three fields: the key being changed, the 
new value (or empty if it's been deleted), and the current entire state of the 
state machine as a read-only map.

An example change listener could be something like below:

```java
Theseus raft = ...
raft.bootstrap();
...

raft.addChangeListener((String changedKey, Optional<byte[]> newValue, Map<String, byte[]> state) -> {
  if (newValue.isPresent()) {
    System.out.println("Key " + changedKey + " changed.");
  } else {
    System.out.println("Key " + changedKey + " was deleted.");
  }
});
```

### Error handling

An important part of any long-running system is being able to log and report
errors. These are meant to be errors that should signal pages -- in fact, in
Eloquent's codebase, they do!

By default, Raft has no registered error handlers.
You can register an error handler directly on the `Theseus` class via:

```java
Theseus raft = ...
raft.bootstrap()15...

raft.addErrorListener((String incidentKey, String debugMessage, StackTraceElement[] stackTrace) -> {
  System.err.println("Raft encountered an error!");
});
```


## Caveats

As a necessary drawback to its design, Theseus has weaker guarantees than the
default Raft implementation. This falls out necessarily from the fact that 
__it's impossible to distinguish a killed machine from a long network
partition__. This comes into play in two situations:

1. Locks are automatically released when a machine goes down. In the case of a
   long network partition, this means it's possible for two servers to hold the
   same lock at the same time.

2. In rare corner cases, it's possible for Raft to lose committed entries.

Both of these should be exceedingly rare in practice, but are important to keep
in mind for environments that are sensitive to them.

The second of the caveats in particular deserves a bit more explanation. 
Assuming an odd quorum size, Theseus is tolerant to a single partition of 
arbitrary length, including a complete network partition (where no
node can see any other node).
But, importantly, it is not robust to multiple carefully timed simultaneous 
partitions. 

To illustrate the failure case in a 3-node 
cluster (server names: `A`, `B`, `C`):

1. `A`,`B`,`C` are all in a single partition, with `A` as the leader.
2. `A` is partitioned off. `B` is elected leader. 
   After 30 seconds, `B` and `C` resize their quorum size to 2. 
   `A` is unable to resize the quorum, as it lacks a majority, and
   therefore does not make progress.
   Note that there is no possible partition that would allow the minority
   group to submit a membership change and make progress.
3. `C` is partitioned off. `B` remains leader. 
   After 30 seconds, `B` resizes its quorum size to 1.
   At roughly the same time, `C` resizes the quorum size to 1 and elects 
   itself leader.
   At this point, both `B` and `C` can make progress!
4. The partition is lifted. One of `B` or `C` becomes the leader, clobbering
   the progress of the other since the partition went up.

Of course, these caveats are only applicable to the dynamically resizing
version of Theseus. If the cluster size is fixed, Theseus maintains all of
the original Raft guarantees.

## Contributing

Contributions to Theseus are welcome and encouraged!
Please read [CONTRIBUTING.md](https://github.com/eloquentlabs/Theseus/blob/master/CONTRIBUTING.md)
for details on our code of conduct, and the process for submitting pull 
requests to us.


## Versioning
* 0.3.0 [2018-10-15] - Initial public release

For the versions available, see the 
[releases page](https://github.com/eloquentlabs/theseus/releases). 


## Authors

Theseus is a collaborative effort of many of us here at Eloquent, and we invite
others from the open source community to join as well.
The current main authors are:

* [**Keenon Werling**](https://github.com/keenon)
* [**Gabor Angeli**](https://github.com/gangeli)
* [**Zames Chua**](https://github.com/zameschua)

Though, many of the other members of the Eloquent team are occasional contributors.
Subscribe to our [blog](https://blog.eloquent.ai/) for updates on our technology.

![cropped-group_brick-1](https://user-images.githubusercontent.com/18271085/45569463-76653100-b814-11e8-9b12-6a6ac344ef76.jpg)


## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

