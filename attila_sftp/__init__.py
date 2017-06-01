"""
Paramiko-based Attila plugin for SFTP path support.
"""

import logging
import os

from urllib.parse import urlparse

# See http://docs.paramiko.org/en/2.1/api/sftp.html for relevant Paramiko SFTP documentation.
import paramiko

from attila.configurations import ConfigManager
from attila.abc.files import FSConnector, Path, fs_connection
from attila.exceptions import verify_type, DirectoryNotEmptyError
from attila.security import credentials
from attila import strings
from attila.fs import local, ftp
from attila.fs.proxies import ProxyFile


__author__ = 'Aaron Hosford'
__author_email__ = 'aaron.hosford@ericsson.com'
__version__ = '0.0.3'
__url__ = 'https://scmgr.eams.ericsson.net/PythonLibs/attila_sftp'
__description__ = 'Paramiko-based Attila Plugin for SFTP support'
__license__ = 'MIT'
__long_description__ = __doc__
__install_requires__ = ['attila>=1.10.2', 'paramiko>=2.1.2']

# This tells Attila how to find our plugins.
__entry_points__ = {
    'attila.config_loader': [
        'SFTPConnector = attila_sftp:SFTPConnector',
        'sftp_connection = attila_sftp:sftp_connection',
    ],
    'attila.url_scheme': [
        'sftp = attila_sftp:SFTPConnector',
    ]
}


__all__ = [
    'DEFAULT_SFTP_PORT',
    'SFTPConnector',
    'sftp_connection',
]


log = logging.getLogger(__name__)


DEFAULT_SFTP_PORT = 22


class SFTPConnector(FSConnector):
    """
    Stores the SFTP connection information as a single object which can then be passed around
    instead of using multiple parameters to a function.
    """

    @classmethod
    def load_url(cls, manager, url):
        """
        Load a new Path instance from a URL string.

        The standard format for an SFTP URL is "sftp://user:password@host:port/path". However, storing
        of plaintext passwords in parameters is not permitted, so the format is
        "sftp://user@host:port/path", where the password is automatically loaded from the password
        database.

        :param manager: The ConfigManager instance.
        :param url: The URL to load.
        :return: The resultant Path instance.
        """
        verify_type(manager, ConfigManager)
        verify_type(url, str)

        if '://' not in url:
            url = 'sftp://' + url
        scheme, netloc, path, params, query, fragment = urlparse(url)
        assert not params and not query and not fragment
        assert scheme.lower() == 'sftp'
        assert '@' in netloc

        user, address = netloc.split('@')

        if ':' in address:
            server, port = address.split(':')
            port = int(port)
        else:
            server = address
            port = DEFAULT_SFTP_PORT

        # We do not permit passwords to be stored in plaintext in the parameter value.
        assert ':' not in user

        credential_string = '%s@%s/sftp' % (user, server)
        credential = manager.load_value(credential_string, credentials.Credential)

        return Path(path, cls('%s:%s' % (server, port), credential).connect())

    @classmethod
    def load_config_section(cls, manager, section, *args, **kwargs):
        """
        Load a new instance from a config section on behalf of a config loader.

        :param manager: An attila.configurations.ConfigManager instance.
        :param section: The name of the section being loaded.
        :return: An instance of this type.
        """
        verify_type(manager, ConfigManager)
        assert isinstance(manager, ConfigManager)

        verify_type(section, str, non_empty=True)

        server = manager.load_option(section, 'Server', str)
        port = manager.load_option(section, 'Port', int, None)
        credential = manager.load_section(section, credentials.Credential)

        if port is not None:
            server = '%s:%s' % (server, port)

        return super().load_config_section(
            manager,
            section,
            *args,
            server=server,
            credential=credential,
            **kwargs
        )

    @classmethod
    def from_ftp(cls, ftp_connector: ftp.FTPConnector, port: int = None) -> 'SFTPConnector':
        """Convert an ordinary FTP connector to an SFTP connector."""
        if port is None:
            port = ftp_connector.port
            if port == ftp.DEFAULT_FTP_PORT:
                port = DEFAULT_SFTP_PORT

        return cls('%s:%s' % (ftp_connector.server, port), ftp_connector.credential, ftp_connector.initial_cwd)

    def __init__(self, server, credential, initial_cwd=None):
        verify_type(server, str, non_empty=True)
        server, port = strings.split_port(server, DEFAULT_SFTP_PORT)

        assert credential.user

        super().__init__(sftp_connection, initial_cwd)

        self._server = server
        self._port = port
        self._credential = credential

    def to_ftp(self, port: int = None, passive: bool = True) -> ftp.FTPConnector:
        """Convert an SFTP connector to an ordinary FTP connector."""
        if port is None:
            port = self._port
            if port == DEFAULT_SFTP_PORT:
                port = ftp.DEFAULT_FTP_PORT

        return ftp.FTPConnector('%s:%s' % (self._server, port), self._credential, passive, self._initial_cwd)

    def __repr__(self):
        server_string = None
        if self._server is not None:
            if self._port == DEFAULT_SFTP_PORT:
                server_string = self._server
            else:
                server_string = '%s:%s' % (self._server, self._port)
        args = [repr(server_string), repr(self._credential)]
        return type(self).__name__ + '(' + ', '.join(args) + ')'

    @property
    def server(self):
        """The DNS name or IP address of the remote server."""
        return self._server

    @property
    def port(self):
        """The remote server's port."""
        return self._port

    @property
    def credential(self):
        """The use name/password used to connect to the remote server."""
        return self._credential

    def connect(self):
        """Create a new connection and return it."""
        return super().connect()


# noinspection PyPep8Naming
class sftp_connection(fs_connection):
    """
    An sftp_connection manages the state for a connection to an SFTP server, providing a
    standardized interface for interacting with remote files and directories.
    """

    @classmethod
    def get_connector_type(cls):
        """Get the connector type associated with this connection type."""
        return SFTPConnector

    def __init__(self, connector):
        """
        Create a new sftp_connection instance.

        Example:
            # Get a connection to the SFTP server.
            connection = sftp_connection(connector)
        """
        assert isinstance(connector, SFTPConnector)
        super().__init__(connector)

        self._session = None

    @property
    def is_open(self):
        """Whether the SFTP connection is currently open."""
        if self._is_open:
            if self._session is None:
                self._is_open = False
            else:
                # noinspection PyBroadException
                try:
                    self._session.listdir()
                except Exception:
                    # noinspection PyBroadException
                    try:
                        self.close()
                    except Exception:
                        pass
                    self._is_open = False
        return super().is_open

    def open(self):
        """Open the SFTP connection."""
        assert not self.is_open

        cwd = self.getcwd()

        user, password, _ = self._connector.credential

        transport = paramiko.Transport(self._connector.server, self._connector.port)
        transport.connect(username=self._connector.credential.user, password=self._connector.credential.password)
        self._session = paramiko.SFTPClient.from_transport(transport)

        super().open()
        if cwd is None:
            # This forces the CWD to be refreshed.
            self.getcwd()
        else:
            # This overrides the CWD based on what it was set to before the connection was opened.
            self.chdir(cwd)

    def close(self):
        """Close the SFTP connection"""
        assert self._is_open

        try:
            self._session.close()
        finally:
            self._session = None
            self._is_open = False

    def getcwd(self):
        """Get the current working directory of this SFTP connection."""
        if self.is_open:
            super().chdir(self._session.getcwd() or '/')
            return super().getcwd()

        return super().getcwd()

    def chdir(self, path):
        """Set the current working directory of this SFTP connection."""
        super().chdir(path)
        if self.is_open:
            self._session.chdir(str(super().getcwd()))

    def _download(self, remote_path, local_path):
        assert self.is_open
        remote_path = self.check_path(remote_path)
        assert isinstance(local_path, str)

        dir_path, file_name = os.path.split(remote_path)

        with Path(dir_path, self):
            self._session.get(file_name, local_path)

    def _upload(self, local_path, remote_path):
        assert self.is_open

        if isinstance(local_path, Path):
            assert isinstance(local_path.connection, local.local_fs_connection)
            local_path = str(local_path)
        assert isinstance(local_path, str)

        remote_path = self.check_path(remote_path)

        dir_path, file_name = os.path.split(remote_path)

        with Path(dir_path, self):
            self._session.put(local_path, file_name)

    def open_file(self, path, mode='r', buffering=-1, encoding=None, errors=None, newline=None, closefd=True,
                  opener=None):
        """
        Open the file.

        :param path: The path to operate on.
        :param mode: The file mode.
        :param buffering: The buffering policy.
        :param encoding: The encoding.
        :param errors: The error handling strategy.
        :param newline: The character sequence to use for newlines.
        :param closefd: Whether to close the descriptor after the file closes.
        :param opener: A custom opener.
        :return: The opened file object.
        """
        assert self.is_open

        mode = mode.lower()
        path = self.check_path(path)

        # We can't work directly with an SFTP file. Instead, we will create a temp file and return it
        # as a proxy.
        with local.local_fs_connection() as connection:
            temp_path = str(abs(connection.get_temp_file_path(self.name(path))))

        # If we're not truncating the file, then we'll need to copy down the data.
        if mode not in ('w', 'wb'):
            self._download(path, temp_path)

        if mode in ('r', 'rb'):
            writeback = None
        else:
            writeback = self._upload

        return ProxyFile(Path(path, self), mode, buffering, encoding, errors, newline, closefd,
                         opener, proxy_path=temp_path, writeback=writeback)

    def list(self, path, pattern='*'):
        """
        Return a list of the names of the files and directories appearing in this folder.

        :param path: The path to operate on.
        :param pattern: A glob-style pattern against which names must match.
        :return: A list of matching file and directory names.
        """
        assert self.is_open
        path = Path(self.check_path(path), self)
        with path:
            listing = self._session.listdir()
            # We have to do this because we can't check if path is a directory,
            # and if we call nlst on a file name, sometimes it will just return
            # that file name in the list instead of bombing out.
            listing = [name for name in listing if self.exists(path[name])]
        if pattern == '*':
            return listing
        else:
            pattern = strings.glob_to_regex(pattern)
            return [name for name in listing if pattern.match(name)]

    def size(self, path):
        """
        Get the size of the file.

        :param path: The path to operate on.
        :return: The size in bytes.
        """
        assert self.is_open
        path = self.check_path(path)

        return self._session.stat(path).st_size

    def modified_time(self, path):
        """
        Get the last time the data of file system object was modified.

        :param path: The path to operate on.
        :return: The time stamp, as a float.
        """
        assert self.is_open
        path = Path(self.check_path(path), self)

        return self._session.stat(path).st_mtime

    def remove(self, path):
        """
        Remove the folder or file.

        :param path: The path to operate on.
        """
        assert self.is_open
        path = self.check_path(path)

        if self.is_dir(path):
            dir_path, dir_name = os.path.split(path)
            with Path(dir_path, self):
                self._session.rmdir(dir_name)
        else:
            dir_path, file_name = os.path.split(path)
            with Path(dir_path, self):
                self._session.remove(file_name)

    def make_dir(self, path, overwrite=False, clear=False, fill=True, check_only=None):
        """
        Create a directory at this location.

        :param path: The path to operate on.
        :param overwrite: Whether existing files/folders that conflict with this function are to be
            deleted/overwritten.
        :param clear: Whether the directory at this location must be empty for the function to be
            satisfied.
        :param fill: Whether the necessary parent folder(s) are to be created if the do not exist
            already.
        :param check_only: Whether the function should only check if it's possible, or actually
            perform the operation.
        :return: None
        """
        path = self.check_path(path)

        if check_only is None:
            # First check to see if it can be done before we actually make any changes. This doesn't
            # make the whole thing perfectly atomic, but it eliminates most cases where we start to
            # do things and then find out we shouldn't have.
            self.make_dir(path, overwrite, clear, fill, check_only=True)

            # If we don't do this, we'll do a redundant check first on each step in the recursion.
            check_only = False

        if self.is_dir(path):
            if clear:
                children = self.glob(path)
                if children:
                    if not overwrite:
                        raise DirectoryNotEmptyError(path)
                    if not check_only:
                        for child in children:
                            child.remove()
        elif self.exists(path):
            # It's not a folder, and it's in our way.
            if not overwrite:
                raise FileExistsError(path)
            if not check_only:
                self.remove(path)
                self._session.mkdir(path)
        else:
            # The path doesn't exist yet, so we need to create it.

            # First ensure the parent folder exists.
            if not self.dir(path).is_dir:
                if not fill:
                    raise NotADirectoryError(self.dir(path))
                self.dir(path).make_dir(overwrite, clear=False, fill=True, check_only=check_only)

            # Then create the target folder.
            if not check_only:
                self._session.mkdir(path)

    def rename(self, path, new_name):
        """
        Rename a file object.

        :param path: The path to be operated on.
        :param new_name: The new name of the file object, as as string.
        :return: None
        """
        assert self.is_open
        path = self.check_path(path)
        assert new_name and isinstance(new_name, str)

        dir_path, file_name = os.path.split(path)
        if file_name != new_name:
            with Path(dir_path, self):
                self._session.rename(file_name, new_name)

    def is_dir(self, path):
        """
        Determine if the path refers to an existing directory.

        :param path: The path to operate on.
        :return: Whether the path is a directory.
        """
        assert self.is_open
        path = self.check_path(path)

        # noinspection PyBroadException
        try:
            with Path(path, self):
                # noinspection PyBroadException
                try:
                    self._session.listdir()
                except Exception:
                    return False
                else:
                    return True
        except Exception:
            return False

    def is_file(self, path):
        """
        Determine if the path refers to an existing file.

        :param path: The path to operate on.
        :return: Whether the path is a file.
        """
        assert self.is_open
        path = self.check_path(path)

        # noinspection PyBroadException
        try:
            self.size(path)
            return True
        except Exception:
            return False

    def join(self, *path_elements):
        """
        Join several path elements together into a single path.

        :param path_elements: The path elements to join.
        :return: The resulting path.
        """
        if path_elements:
            # There is a known Python bug which causes any TypeError raised by a generator during
            # argument interpolation with * to be incorrectly reported as:
            #       TypeError: join() argument after * must be a sequence, not generator
            # The bug is documented at:
            #       https://mail.python.org/pipermail/new-bugs-announce/2009-January.txt
            # To avoid this confusing misrepresentation of errors, I have broken this section out
            # into multiple statements so TypeErrors get the opportunity to propagate correctly.
            starting_slash = path_elements and str(path_elements[0]).startswith('/')
            path_elements = tuple(self.check_path(element).strip('/\\') for element in path_elements)
            if starting_slash:
                path_elements = ('',) + path_elements
            return Path('/'.join(path_elements), connection=self)
        else:
            return Path(connection=self)
