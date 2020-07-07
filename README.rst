==================================================================
SwiftPath: A pathlib-derived interface to Openstack Swift
==================================================================


🐉 Installation
=================

Install from `PyPI`_:

  ::

    $ pip install swiftpath

.. _PyPI: https://www.pypi.org/project/swiftpath
.. _Github: https://github.com/canonical/swiftpath


🐉 About
==========

Swift Connection
-------------------

**swiftpath** will automatically read environment variables to determine how to connect to a swift instance.

Currently, **swiftpath** looks for the following environment variables when connecting to swift:

  - **OS_USER_ID** or **OS_USERNAME**
  - **OS_PASSWORD**
  - **OS_PROJECT_NAME** or **OS_TENANT_NAME**
  - **OS_AUTH_URL** or **OS_AUTHENTICATION_URL**
  - **OS_STORAGE_URL**
  - **OS_REGION_NAME**
  - **OS_PROJECT_ID**

Caveats
---------

Note that the following methods are not provided as they are not available on swift:

  - *SwiftPath.cwd()*
  - *SwiftPath.home()*
  - *SwiftPath.chmod()*
  - *SwiftPath.expanduser()*
  - *SwiftPath.lchmod()*
  - *SwiftPath.group()*
  - *SwiftPath.is_block_device()*
  - *SwiftPath.is_char_device()*
  - *SwiftPath.lstat()*
  - *SwiftPath.resolve()*


🐉 Usage
==========

To construct a path to a swift instance, simply use the syntax ``/containername/path/to/key`` when referencing the object. All ``pathlib.Path``
methods that are not explicitly excluded are available on the subsequently created object.

.. code:: python

    >>> from swiftpath import SwiftPath
    >>> path = SwiftPath("/mycontainer/my-file.txt")
    >>> path.write_text("hello\n")
    >>> path.read_text()
    hello


.. note:: Symlinks are natively supported in swift, and you will find they are also supported in this library.

