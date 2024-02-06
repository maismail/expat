# Introduction


Expat is a tool of the Hopsworks ecosystem which is used for upgrading Hopsworks existing installations.
Expat is versioned and it follows the same versions as Hopsworks. The migration steps to be executed are defined in 
``resources/expat-site-default.xml`` under the ``<version-*>`` tags. When upgrading an existing Hopsworks cluster, 
the cluster definition contains the current version and the version to upgrade to. Expat will run all migration steps
that fall between the two Hopsworks versions, excluding the current version of course. For example. If the current 
Hopsworks cluster to be upgraded is running version ``0.6.1`` and you are upgrading to ``1.0.0``, steps ``070``, 
``010`` and ``10`` will be executed. Bug fix version upgrades are currently not supported, you can track progress of
this feature by following [HOPSWORKS-814](https://logicalclocks.atlassian.net/browse/HOPSWORKS-814).

# Adding your migration step

1. You need to define a class that implements the ``MigrateStep`` interface
2. Set the class in `expat-site-default.xml` and in hopsworks-chef `expat-site.xml.erb`. Be careful to add the 
``ShutdownHook`` in your class and in the main method in ``Expat``, if needed.
3. If you want to run a script from the scripts folder, you need to add it in the pom.xml as well.

# Build

``mvn clean package``

# Run

Follow the steps of the [migrate](https://github.com/logicalclocks/hopsworks-chef/blob/master/recipes/migrate.rb) recipe, the main steps are:

1. Copy tha expat archive from ./target to the machine where expat is to run
2. Extract it
3. Copy the mysql connector from the hopsworks repo to ./lib
4. Create the etc/expat-site.xml based on the provided template and set the variables. You do not need to set all of
them, but you need at least the db credentials and domain. You might also need to set the expat dir (where you are
running expat from). For testing, you probably need to grant access to user kthfs.
You can do so with `GRANT ALL PRIVILEGES ON *.* to '<user>'@'%' identified by '<pass>';`

You may need to set the `dry_run` to true or false when you run expat.
# Example
HADOOP_USER_NAME=hdfs HOPSWORKS_EAR_HOME=/srv/hops/domains/domain1/applications/hopsworks-ear~1.4.0-SNAPSHOT bin/expat -a migrate -v 1.4.0


