"""
This is the setup script for attila_sftp.

To install the package:
    python setup.py install

To test the package while you're modifying it:
    python setup.py develop

To generate a .zip containing the source files:
    python setup.py source

To generate a .whl file for distribution and installation with pip:
    python setup.py bdist_wheel
"""

import os
import sys


try:
    # noinspection PyUnresolvedReferences
    import infotags
except ImportError:
    print("This setup script depends on infotags. Please install infotags using the command, "
          "'pip install infotags' and then run this setup script again.")
    sys.exit(2)


try:
    # noinspection PyUnresolvedReferences
    from attila.installation import setup
except ImportError:
    print("This setup script depends on attila. Please install attila using the command, "
          "'pip install attila' and then run this setup script again.")
    sys.exit(2)


PACKAGE_NAME = 'attila_sftp'


cwd = os.getcwd()
if os.path.dirname(__file__):
    os.chdir(os.path.dirname(__file__))
try:
    info = infotags.get_info(PACKAGE_NAME)
    setup(**info)
finally:
    os.chdir(cwd)
