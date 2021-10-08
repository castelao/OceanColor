.. highlight:: shell

============
Installation
============

Stable release
--------------

If you want to do science, the stable release is the correct choice.

Using PIP
~~~~~~~~~

To install Ocean Color package, run this command in your terminal:

.. code-block:: console

    $ pip install OceanColor

If you don't have `pip`_ installed, this `Python installation guide`_ can guide
you through the process.

.. _pip: https://pip.pypa.io
.. _Python installation guide: http://docs.python-guide.org/en/latest/starting/installation/

Using Conda
~~~~~~~~~~~

If you use conda, you shall add conda-forge to your channels, if you don't already have it,

.. code-block:: console

   $ conda config --append channels conda-forge

and install OceanColor together with loky. While loky is not required, it will allow for faster searches

.. code-block:: console

   $ conda install oceancolor
   $ conda install loky


From sources
------------

If you are looking for the source code with the intention of contributing with modifications, please check the :ref:`Contributing section <Contributing>` of this manual.

PIP
~~~

Probably the most convenient way to install from the source is is using PIP:

.. code-block:: console

    $ pip install git+https://github.com/castelao/OceanColor.git

It the background, it will download the latest stable source and install it.

Manually
~~~~~~~~

The sources for Ocean Color can be downloaded from the `GitHub repo`_.

You can either clone the public repository:

.. code-block:: console

    $ git clone git://github.com/castelao/OceanColor

Or download the `tarball`_:

.. code-block:: console

    $ curl -OJL https://github.com/castelao/OceanColor/tarball/master

Once you have a copy of the source, you can install it with:

.. code-block:: console

    $ python setup.py install


.. _GitHub repo: https://github.com/castelao/OceanColor
.. _tarball: https://github.com/castelao/OceanColor/tarball/master
