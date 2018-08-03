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

    <storage-root>/<facility-name>/<prefix>/<inv-name>/<visit-id>/data/<dataset-name>/<datafile-name>

  where ``<prefix>`` corresponds to the facility cycle and is derived
  from the investigation name ``<inv-name>``.

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

This plugin requires ids.plugin 1.3.1 and ids.server 1.6.0 or greater.


Bugs and limitations
~~~~~~~~~~~~~~~~~~~~

+ The file locking in the main storage has some issues: it is not
  always reliable and not very efficient.  This is due to the lack of
  support for locking in the current version of ids.server and cannot
  be fixed in the plugin.


Compatibility with ids.server
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

+----------------+--------------------+
| plugin version | ids.server version |
+================+====================+
| 0.4.0          | 1.8.0              |
+----------------+--------------------+
| 0.3.3 - 0.3.5  | 1.6.0 - 1.7.0      |
+----------------+--------------------+


Release notes
~~~~~~~~~~~~~

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

Copyright 2012-2016 The ICAT Collaboration
Copyright 2015-2017 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH

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