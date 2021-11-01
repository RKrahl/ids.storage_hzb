ids.storage_hzb - An IDS storage plugin for the HZB
===================================================

This is an `IDS`_ storage plugin adapted to the needs of `HZB`_.  For
the moment, it is in an experimental stage.  It is mostly used to
explore the IDS and to try out how IDS storage plugins can be
customized.


Features
~~~~~~~~

+ Customized, meaningful file names in the storage.  The file name
  layout is::

    <storage-root>/<name-space>/<cycle>/<inv-name>/data/<dataset-name>/<datafile-name>

  where ``<name-space>`` is a name space prefix and ``<cycle>``
  corresponds to the facility cycle, both are derived from the
  investigation name ``<inv-name>``.

+ Configurable permissions and posix group for files created in the
  storage.

+ External storage areas: a prefix on the location of Datasets and
  Datafiles determines whether it is to be searched in an external
  storage area or not.  These external storage area are accessed
  read-only and are excluded from the standard two-level storage
  mechanism.

+ Use file system locking when accessing the storage to allow
  concurrent access to the storage by external processes.

+ Check the location argument in the callbacks for consistency to work
  around a vulnerability in IDS.


Installation
~~~~~~~~~~~~

ids.storage_hzb is based on `ids.storage_file`_.  There is no binary
release, you need to build the distribution from the sources.  Once
this is done, the original install instructions for ids.storage_file
mostly applies to ids.storage_hzb as well, see the general
`installation instructions`_ and the specific `installation
instructions for ids.storage_file`_.

There are a few additional configuration options in
ids.storage_hzb.main.properties though, see the comments in the file.

This plugin requires ids.plugin 1.4.0 and ids.server 1.9.0 or greater.


Compatibility with ids.server
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

+----------------+--------------------+
| plugin version | ids.server version |
+================+====================+
| 0.6.0 - 0.6.3  | 1.10.0 - 1.12.0    |
+----------------+--------------------+
| 0.5.0 - 0.5.2  | 1.9.0 - 1.9.1      |
+----------------+--------------------+
| 0.4.0          | 1.8.0              |
+----------------+--------------------+
| 0.3.3 - 0.3.5  | 1.6.0 - 1.7.0      |
+----------------+--------------------+


Bugs and limitations
~~~~~~~~~~~~~~~~~~~~

+ It is impossible to safely remove a lock file from the storage
  without a race condition.  For this reason, this plugin does not
  remove the lock files at all, but rather leaves this to an external
  tool that tries to minimize the risk.  Note that this is not a bug
  in this particular implementation, but rather a design flaw in POSIX
  fcntl() style locks.  The problem is that fcntl() locks are obtained
  on open file descriptors, but there is no way to open and lock a
  file in one single atomic operation.


Release notes
~~~~~~~~~~~~~

Version 0.6.2 (2021-11-01)
--------------------------

+ Bump logback-classic from 1.1.3 to 1.2.0: CVE-2017-5929

Version 0.6.2 (2021-10-29)
--------------------------

+ Bugfix: fixed regular expression for misc proposal numbers.

Version 0.6.1 (2019-04-30)
--------------------------

+ Bugfix: Should ignore IOException while trying to set permissions or
  file modification time of the lock file or the directory in DirLock.

Version 0.6.0 (2019-04-26)
--------------------------

+ Base file locking in main storage on the new lock() method
  introduced with ids.plugin 1.5.0.  As a consequence, file locking
  will only work with ids.server 1.10 or newer.

+ Merge ids.storage_file 1.4.2: delete() methods do not throw an
  exception if the files to be deleted do not exist.

+ Require ids.plugin 1.5.0.

+ Add proposal number name spaces 'pub' and 'misc'.

Version 0.5.2 (2019-02-04)
--------------------------

+ Throw InvalidPathException rather then IOException if a file path
  fails sanitation checks.

+ Reenable logging.

Version 0.5.1 (2018-12-18)
--------------------------

+ Allow underscore as first character in file and dataset names.

Version 0.5.0 (2018-08-06)
--------------------------

+ Adapt storage path to new style proposal numbers.

+ Drop facility name and visit id from the zipper name.

+ Add a configuration option to define the group for files and
  directories created by the plugin in the storage.

+ Implement file locking in ArchiveFileStorage also for put().

+ Merge ids.storage_file 1.4.1.  Derive the storage classes from the
  new abstract classes defined in the plugin.  Require ids.plugin
  1.4.0.

Version 0.4.0 (2017-09-04)
--------------------------

+ Merge ids.storage_file 1.4.0.  Adapt to new configuration interface
  introduced with ids.server 1.8.0.

+ Rename package to org.icatproject.site.hzb.ids.storage.

Version 0.3.5 (2017-03-03)
--------------------------

+ Add a configuration option to define the permissions for files and
  directories created by the plugin in the storage.

+ Reenable deleting of parent directories on delete() in
  MainFileStorage.  Note that this will not work if file locking is
  enabled.

Version 0.3.4 (2016-12-14)
--------------------------

+ Add configuration options to switch file locking on or off (default
  off).

+ Do not remove old lock files.

+ ArchiveFileStorage.delete() does not throw an exception if the
  archive does not exist.  (See also `icatproject/ids.server#61`_.)

+ Upgrade to icat.utils 4.15.1.

Version 0.3.3 (2016-06-03)
--------------------------

+ Add another directory level "data" above the dataset.

+ Merge ids.storage_file 1.3.3.  This requires ids.server 1.6.0.

Version 0.3.2 (2015-03-05)
--------------------------

Bugfixes:

+ Must not lock directories in external storage areas.

+ Create the directory lock file in the parent directory rather then
  in the directory itself.

Version 0.3.1 (2015-02-27)
--------------------------

Do not check the existence of dataset directories in external storage
areas, rather assume them always to exist, e.g. always return true.
This removes any semantic from the location attribute in Dataset other
then the presence of a storage area prefix, e.g. for Dataset, the
location does not need to be an existing file or directory.

Version 0.3.0 (2015-02-26)
--------------------------

Implement locking of dataset directories in the main storage.

Version 0.2.0 (2015-02-20)
--------------------------

Implement external storage areas.

Version 0.1.0 (2015-01-23)
--------------------------

Basically a customized version of ids.storage_file.  Originally
derived from ids.storage_file 1.2.0, later ported to 1.3.0.


Copyright and License
~~~~~~~~~~~~~~~~~~~~~

Copyright 2012-2018 The ICAT Collaboration
Copyright 2015-2018 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License.  You may
obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied.  See the License for the specific language governing
permissions and limitations under the License.


.. _HZB: https://www.helmholtz-berlin.de/
.. _IDS: https://icatproject.org/user-documentation/icat-data-service/
.. _ids.storage_file: https://repo.icatproject.org/site/ids/storage_file/1.3.3/
.. _installation instructions: https://icatproject.org/installation/component/
.. _installation instructions for ids.storage_file: https://repo.icatproject.org/site/ids/storage_file/1.3.3/installation.html
.. _icatproject/ids.server#61: https://github.com/icatproject/ids.server/issues/61
