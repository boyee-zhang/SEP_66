===========
JDBC Driver
===========

The Presto JDBC (Java Database Connectivity) driver allows users to run SQL 
queries on a variety of data sources through Presto. Both client- and 
server-side applications, such those used for reporting and database 
development, use the JDBC driver.

Requirements
------------

The Presto JDBC driver is compatible with Java 8, and can be used with 
applications running on Java virtual machines version 8 and higher.

Installing
----------

Download the Presto :maven_download:`jdbc` or alternatively, you can download 
it from Maven Central:

.. parsed-literal::

    <dependency>
        <groupId>io.prestosql</groupId>
        <artifactId>presto-jdbc</artifactId>
        <version>\ |version|\ </version>
    </dependency>

If you do not want to use the latest versions linked above, a list of all 
currently available versions can be found in the Maven `prestosql repository 
<https://repo.maven.apache.org/maven2/io/prestosql/presto-jdbc>`__.

Once downloaded, you must add the JAR file to the classpath of your Java 
application. Often, this will be the lib folder of a Java application, but
you should check the documentation for your specific application. Depending 
on your application, you may be able to accomplish this from a dialog rather 
than on the command line.  

Once you have downloaded the Presto JDBC driver and added it to your 
classpath, you will typically need to restart your application in order to 
recognize the new driver. After that, you must register and configure the 
driver in the application.

Registering
-----------

If your application has a UI, look for a Settings option or a Drivers tab and 
register the Presto JDBC driver there. The steps to register the Presto JDBC
driver for your application on the command line depend on the specific  
application you are using. Please check the application's documentation.

Configuring
-----------

Along with the moving the Presto JDBC JAR file into your application's 
classpath and registering it with the application, you must also configure 
the following two required parameters in your Java application:

* Class name: ``io.prestosql.jdbc.PrestoDriver`` 
* JDBC URL (example): ``jdbc:presto://host:port/catalog/schema``

This example JDBC URL locates a Presto instance running on port ``8080`` in
``example.net``, with the catalog ``hive`` and the schema ``sales`` defined. 
Other JDBC URL formats are also supported:

.. code-block:: none

    jdbc:presto://host:port
    jdbc:presto://host:port/catalog
    jdbc:presto://host:port/catalog/schema

Note that there may be additional, optional parameters available for use in 
your application to configure the Presto JDBC driver, such as:

* Name: ``Presto`` 
* Website: ``http://prestosql.io``

Connecting
----------

When your driver is registered and configured, you are ready to connect to 
Presto from your application. The following example uses the example JDBC URL  
from above to create a connection:

.. code-block:: java

    String url = "jdbc:presto://example.net:8080/hive/sales";
    Connection connection = DriverManager.getConnection(url, "test", null);

Connection Parameters
---------------------

The Presto JDBC driver supports various parameters that may be set as URL parameters,
or as properties passed to ``DriverManager``. Both of the following
examples are equivalent:

.. code-block:: java

    // URL parameters
    String url = "jdbc:presto://example.net:8080/hive/sales";
    Properties properties = new Properties();
    properties.setProperty("user", "test");
    properties.setProperty("password", "secret");
    properties.setProperty("SSL", "true");
    Connection connection = DriverManager.getConnection(url, properties);

    // properties
    String url = "jdbc:presto://example.net:8080/hive/sales?user=test&password=secret&SSL=true";
    Connection connection = DriverManager.getConnection(url);

These methods may be mixed; some parameters may be specified in the URL,
while others are specified using properties. However, the same parameter
may not be specified using both methods.

Parameter Reference
-------------------

====================================== =======================================================================
Name                                   Description
====================================== =======================================================================
``user``                               Username to use for authentication and authorization.
``password``                           Password to use for LDAP authentication.
``socksProxy``                         SOCKS proxy host and port. Example: ``localhost:1080``
``httpProxy``                          HTTP proxy host and port. Example: ``localhost:8888``
``clientInfo``                         Extra information about the client.
``clientTags``                         Client tags for selecting resource groups. Example: ``abc,xyz``
``traceToken``                         Trace token for correlating requests across systems.
``applicationNamePrefix``              Prefix to append to any specified ``ApplicationName`` client info
                                       property, which is used to set the source name for the Presto query.
                                       If neither this property nor ``ApplicationName`` are set, the source
                                       for the query is ``presto-jdbc``.
``accessToken``                        Access token for token based authentication.
``SSL``                                Use HTTPS for connections
``SSLKeyStorePath``                    The location of the Java KeyStore file that contains the certificate
                                       and private key to use for authentication.
``SSLKeyStorePassword``                The password for the KeyStore.
``SSLTrustStorePath``                  The location of the Java TrustStore file to use.
                                       to validate HTTPS server certificates.
``SSLTrustStorePassword``              The password for the TrustStore.
``KerberosRemoteServiceName``          Presto coordinator Kerberos service name. This parameter is
                                       required for Kerberos authentication.
``KerberosPrincipal``                  The principal to use when authenticating to the Presto coordinator.
``KerberosUseCanonicalHostname``       Use the canonical hostname of the Presto coordinator for the Kerberos
                                       service principal by first resolving the hostname to an IP address
                                       and then doing a reverse DNS lookup for that IP address.
                                       This is enabled by default.
``KerberosServicePrincipalPattern``    Presto coordinator Kerberos service principal pattern. The default is
                                       ``${SERVICE}@${HOST}``. ``${SERVICE}`` is replaced with the value of
                                       ``KerberosRemoteServiceName`` and ``${HOST}`` is replaced with the
                                       hostname of the coordinator (after canonicalization if enabled).
``KerberosConfigPath``                 Kerberos configuration file.
``KerberosKeytabPath``                 Kerberos keytab file.
``KerberosCredentialCachePath``        Kerberos credential cache.
``useSessionTimeZone``                 Should dates and timestamps use the session time zone (default: false).
                                       Note that this property only exists for backward compatibility with the
                                       previous behavior and will be removed in the future.
``extraCredentials``                   Extra credentials for connecting to external services,
                                       specified as a list of key-value pairs. For example,
                                       ``foo:bar;abc:xyz`` creates the credential named ``abc``
                                       with value ``xyz`` and the credential named ``foo`` with value ``bar``.
``roles``                              Authorization roles to use for catalogs, specified as a list of
                                       key-value pairs for the catalog and role. For example,
                                       ``catalog1:roleA;catalog2:roleB`` sets ``roleA``
                                       for ``catalog1`` and ``roleB`` for ``catalog2``.
``sessionProperties``                  Session properties to set for the system and for catalogs,
                                       specified as a list of key-value pairs.
                                       For example, ``abc:xyz;example.foo:bar`` sets the system property
                                       ``abc`` to the value ``xyz`` and the ``foo`` property for
                                       catalog ``example`` to the value ``bar``.
====================================== =======================================================================
