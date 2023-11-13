---
---
---

# Starburst common engine for Starburst for Galaxy and SEP

## What is this repo all about?

It is a continues fork of Trino OSS that is used to based all Starburst proprietary software.
This is project is updated with changes from OSS Trino in continues manner so all changes in Trino lands in this repo 
eventually. That way we maintain relationship with Trino codebase in order to avoid the hard fork.

## Development model

<img alt="Forks" src=".github/forks.png" />

In the proposed model we have following components:
 * Trino OSS (trinodb/trino repository, airlift etc)
 * Starburst Trino Engine - a continuous fork of Trino OSS that also contains all shared
   components like Warp Speed. The fork is continuous as periodically it gets updated with all new changes from Trino OSS.
 * Galaxy Trino fork (starburstdata/galaxy-trino repository) - a continuous fork of Starburst Common Trino Fork with Galaxy
   specific changes. The fork is continuous as periodically it gets updated with all new changes from Common Trino
   Fork. It is also using release artifacts of shared components.
 * Galaxy - product that consumes Galaxy Trino fork to provision query execution clusters (control plane). It is also
   using release artifacts of shared components (consider Buffer Service or Schema Discovery).
 * Starburst Enterprise (SEP) (starburstdata/starburst-enterprise repository) a consumer of Starburst Common Trino fork.
   Trino is used here as library and extension points are used to implement here SEP specific features.

## Development

### Code owners

This is crucial repository for the company and so we need to be more strict about things we merge here. Especially one
needs to consider that changes needs to be continues fork friendly. Therefore we would like to follow
that:
 - In order to merge the code, you need to have the review from the group that is known as Code Owners.
 - Code Owners are defined in [CODEOWNERS file](.github/CODEOWNERS).
 - Code Owners have the right to merge the code, but they also have the responsibility to periodically update the fork
 with Trino changes.

### How to build this project?

You will need to use settings.xml file with credentials for AWS CodeArtifact.

Use one either from [Galaxy](https://github.com/starburstdata/stargate) or
[SEP](https://github.com/starburstdata/starburst-enterprise) repository.

### How to update this Trino fork to a newer Trino version?

Follow instructions in [Maintaining cork (Common Starburst Trino Fork)](oss-update.md).


---
---
---

<p align="center">
    <a href="https://trino.io/"><img alt="Trino Logo" src=".github/homepage.png" /></a>
</p>
<p align="center">
    <b>Trino is a fast distributed SQL query engine for big data analytics.</b>
</p>
<p align="center">
    See the <a href="https://trino.io/docs/current/">User Manual</a> for deployment instructions and end user documentation.
</p>
<p align="center">
   <a href="https://trino.io/download.html">
       <img src="https://img.shields.io/maven-central/v/io.trino/trino-server.svg?label=Trino" alt="Trino download" />
   </a>
   <a href="https://trino.io/slack.html">
       <img src="https://img.shields.io/static/v1?logo=slack&logoColor=959DA5&label=Slack&labelColor=333a41&message=join%20conversation&color=3AC358" alt="Trino Slack" />
   </a>
   <a href="https://trino.io/trino-the-definitive-guide.html">
       <img src="https://img.shields.io/badge/Trino%3A%20The%20Definitive%20Guide-download-brightgreen" alt="Trino: The Definitive Guide book download" />
   </a>
</p>

## Development

See [DEVELOPMENT](.github/DEVELOPMENT.md) for information about code style,
development process, and guidelines.

See [CONTRIBUTING](.github/CONTRIBUTING.md) for contribution requirements.

## Security

See the project [security policy](.github/SECURITY.md) for
information about reporting vulnerabilities.

## Build requirements

* Mac OS X or Linux
* Java 17.0.4+, 64-bit
* Docker

## Building Trino

Trino is a standard Maven project. Simply run the following command from the
project root directory:

    ./mvnw clean install -DskipTests

On the first build, Maven downloads all the dependencies from the internet
and caches them in the local repository (`~/.m2/repository`), which can take a
while, depending on your connection speed. Subsequent builds are faster.

Trino has a comprehensive set of tests that take a considerable amount of time
to run, and are thus disabled by the above command. These tests are run by the
CI system when you submit a pull request. We recommend only running tests
locally for the areas of code that you change.

## Running Trino in your IDE

### Overview

After building Trino for the first time, you can load the project into your IDE
and run the server.  We recommend using
[IntelliJ IDEA](http://www.jetbrains.com/idea/). Because Trino is a standard
Maven project, you easily can import it into your IDE.  In IntelliJ, choose
*Open Project* from the *Quick Start* box or choose *Open*
from the *File* menu and select the root `pom.xml` file.

After opening the project in IntelliJ, double check that the Java SDK is
properly configured for the project:

* Open the File menu and select Project Structure
* In the SDKs section, ensure that JDK 17 is selected (create one if none exist)
* In the Project section, ensure the Project language level is set to 17

### Running a testing server

The simplest way to run Trino for development is to run the `TpchQueryRunner`
class. It will start a development version of the server that is configured with
the TPCH connector. You can then use the CLI to execute queries against this
server. Many other connectors have their own `*QueryRunner` class that you can
use when working on a specific connector.

### Running the full server

Trino comes with sample configuration that should work out-of-the-box for
development. Use the following options to create a run configuration:

* Main Class: `io.trino.server.DevelopmentServer`
* VM Options: `-ea -Dconfig=etc/config.properties -Dlog.levels-file=etc/log.properties -Djdk.attach.allowAttachSelf=true`
* Working directory: `$MODULE_DIR$`
* Use classpath of module: `trino-server-dev`

The working directory should be the `trino-server-dev` subdirectory. In
IntelliJ, using `$MODULE_DIR$` accomplishes this automatically.

If `VM options` doesn't exist in the dialog, you need to select `Modify options`
and enable `Add VM options`.

### Running the CLI

Start the CLI to connect to the server and run SQL queries:

    client/trino-cli/target/trino-cli-*-executable.jar

Run a query to see the nodes in the cluster:

    SELECT * FROM system.runtime.nodes;

Run a query against the TPCH connector:

    SELECT * FROM tpch.tiny.region;
