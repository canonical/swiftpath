"""A module for interacting with **Openstack Swift** using the standard
:mod:`pathlib.Path` interface.
"""
import atexit
import base64
import contextlib
import datetime
import io
import logging
import os
import pathlib
import posix
import re
import sys
import tempfile
import urllib.parse
from pathlib import PurePath
from re import L
from typing import (
    IO,
    Any,
    AnyStr,
    Dict,
    Generator,
    Iterable,
    List,
    Optional,
    Protocol,
    Type,
    TypeVar,
    Union,
)

import attr
from requests.exceptions import StreamConsumedError

try:
    import keystoneauth1
    import keystoneauth1.exceptions.catalog
    import keystoneauth1.session
    import keystoneauth1.identity
    import swiftclient.client
    import swiftclient.exceptions
except ImportError:
    keystoneauth1 = None
    swiftclient = None

try:
    import filelock
except ImportError:
    filelock = None


IOTYPES = Union[Type["SwiftKeyReadableFileObject"], Type["SwiftKeyWritableFileObject"]]
TStrTypes = TypeVar("TStrTypes", str, bytes)

# See https://stackoverflow.com/a/8571649 for explanation
BASE64_RE = re.compile(b"^([A-Za-z0-9+/]{4})*([A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{2}==)?$")
_SUPPORTED_OPEN_MODES = {"r", "br", "rb", "tr", "rt", "w", "wb", "bw", "wt", "tw"}


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())


def log(message, level="info"):
    getattr(logger, level.lower())(message)


class AttrProto(Protocol):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        ...


def fromisoformat(dt: str) -> datetime.datetime:
    result: datetime.datetime
    try:
        result = datetime.datetime.fromisoformat(dt)  # type: ignore
    except AttributeError:
        result = datetime.datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S.%f")
    return result


@attr.s(frozen=True)
class ObjectPath(AttrProto):
    #: The name of the container
    container = attr.ib(type=str)
    #: The optional path to the target object
    key = attr.ib(type=Optional[str])

    def __str__(self):
        if not self.key:
            return f"/{self.container}/"
        return f"/{self.container}/{self.key}"

    def as_path(self) -> "SwiftPath":
        return SwiftPath(str(self))

    @classmethod
    def from_path(cls, path: "SwiftPath") -> "ObjectPath":
        container = str(path.container) if path.container else None
        if not container:
            if path.root == str(path):
                container = ""
            else:
                raise ValueError(
                    f"Absolute path required to parse container, got {path!s}"
                )
        container = container.strip(path._flavour.sep)
        key = str(path.key) if path.key else None
        if key is not None:
            key = key.lstrip(path._flavour.sep)
        return cls(container=container, key=key)


class _Backend:
    def __init__(
        self,
        username: Optional[str] = None,
        project: Optional[str] = None,
        password: Optional[str] = None,
        auth_url: Optional[str] = None,
        domain: Optional[str] = None,
        object_storage_url: Optional[str] = None,
        project_id: Optional[str] = None,
        user_id: Optional[str] = None,
        project_domain: Optional[str] = None,
        region: Optional[str] = None,
    ) -> None:
        swift_credentials = {
            "user_domain_name": domain
            or os.environ.get("OS_USER_DOMAIN_NAME", "default"),
            "project_domain_name": project_domain
            or os.environ.get("OS_PROJECT_DOMAIN_NAME", "default"),
            "password": password or os.environ.get("OS_PASSWORD"),
        }
        os_options = {}
        user_id = user_id or os.environ.get("OS_USER_ID", None)
        username = username or os.environ.get("OS_USERNAME", None)
        project = project or os.environ.get(
            "OS_PROJECT_NAME", os.environ.get("OS_TENANT_NAME")
        )
        if not auth_url:
            auth_url = os.environ.get(
                "OS_AUTH_URL", os.environ.get("OS_AUTHENTICATION_URL")
            )
        object_storage_url = object_storage_url or os.environ.get("OS_STORAGE_URL")
        region = region or os.environ.get("OS_REGION_NAME")
        project_id = project_id or os.environ.get("OS_PROJECT_ID")
        if username:
            swift_credentials["username"] = username
        elif user_id:
            swift_credentials["user_id"] = user_id
        if project:
            swift_credentials["project_name"] = project
            os_options["project_name"] = project
        if object_storage_url:
            os_options["object_storage_url"] = object_storage_url
        if region:
            os_options["region_name"] = region
        if project_id:
            os_options["project_id"] = project_id
        if auth_url:
            swift_credentials["auth_url"] = auth_url
        self.os_options = os_options
        self.auth = keystoneauth1.identity.v3.Password(**swift_credentials)
        self.swift = self._get_connection()

    def _get_session(self) -> keystoneauth1.session.Session:
        return keystoneauth1.session.Session(auth=self.auth)

    def _get_connection(self) -> swiftclient.client.Connection:
        return swiftclient.client.Connection(
            session=self._get_session(), os_options=self.os_options
        )

    @contextlib.contextmanager
    def connection(self) -> Generator[swiftclient.client.Connection, None, None]:
        with contextlib.closing(self._get_connection()) as swift_conn:
            yield swift_conn


class _SwiftFlavour(pathlib._PosixFlavour):  # type: ignore
    is_supported = bool(keystoneauth1)

    def make_uri(self, path):
        uri = super().make_uri(path)
        return uri.replace("file:///", "swift://")


class _SwiftScandir:
    def __init__(self, *, swift_accessor, path):
        self._swift_accessor = swift_accessor
        self._path = path

    def __enter__(self):
        return self

    def __exit__(self, exc_typ, exc_val, exc_tb):
        return

    def __iter__(self):
        try:
            parsed_path = ObjectPath.from_path(self._path)
        except ValueError:
            parsed_path = None
        if not parsed_path.container:
            path_prefix = "/"
        else:
            path_prefix = f"/{parsed_path.container}/"
        with self._swift_accessor.Backend.connection() as conn:
            if not parsed_path or not parsed_path.container:
                _, containers = conn.get_account()
                for container in containers:
                    yield SwiftDirEntry(container["name"], is_dir=True)
                return
            path = parsed_path.key if parsed_path.key else ""
            if path and not path.endswith(self._path._flavour.sep):
                path = f"{path}{self._path._flavour.sep}"
            headers, paths = conn.get_container(
                parsed_path.container, prefix=path, delimiter=self._path._flavour.sep
            )
            for p in paths:
                if "subdir" in p:
                    sub_path = type(self._path)(f"{path_prefix}{p['subdir']}")
                    name = str(sub_path.relative_to(self._path))
                    yield SwiftDirEntry(name, is_dir=True)
                else:
                    is_symlink = p.get("content_type", "") == "application/symlink"
                    sub_path = type(self._path)(f"{path_prefix}{p['name']}")
                    name = str(sub_path.relative_to(self._path))
                    yield SwiftDirEntry(
                        name,
                        is_dir=False,
                        size=p["bytes"],
                        last_modified=p["last_modified"],
                        is_symlink=is_symlink,
                    )


class _SwiftAccessor:
    Backend: _Backend = _Backend()

    @staticmethod
    def stat(target: "SwiftPath") -> "StatResult":
        parsed_path = ObjectPath.from_path(target)
        with _SwiftAccessor.Backend.connection() as conn:
            headers = {}
            try:
                headers = conn.head_object(parsed_path.container, parsed_path.key)
            except swiftclient.exceptions.ClientException:
                try:
                    result = conn.get_container(
                        parsed_path.container, prefix=parsed_path.key
                    )
                except (swiftclient.exceptions.ClientException, TypeError):
                    raise FileNotFoundError(str(target))
                else:
                    if result is not None:
                        headers, _ = result
            if "x-object-meta-mtime" in headers:
                last_modified = float(headers["x-object-meta-mtime"])
            elif "x-timestamp" in headers:
                try:
                    last_modified = fromisoformat(headers["x-timestamp"]).timestamp()
                except ValueError:
                    last_modified = float(headers["x-timestamp"])
            else:
                last_modified = 0
            return StatResult(
                size=headers["content-length"], last_modified=last_modified,
            )

    @staticmethod
    def lstat(target: "SwiftPath") -> None:
        raise NotImplementedError("lstat() not available on this system")

    @staticmethod
    def open(
        path, *, mode="r", buffering=-1, encoding=None, errors=None, newline=None
    ) -> IO:
        file_object: IOTYPES = (
            SwiftKeyReadableFileObject if "r" in mode else SwiftKeyWritableFileObject
        )
        result = file_object(
            path,
            mode=mode,
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline,
        )
        return result

    @staticmethod
    def listdir(target: "SwiftPath") -> List[str]:
        results: List[str] = []
        parsed_path = ObjectPath.from_path(target)
        target_path = parsed_path.key
        paths: List[Dict[str, str]] = []
        if target_path and not target_path.endswith(target._flavour.sep):
            target_path = f"{target_path}{target._flavour.sep}"
        with _SwiftAccessor.Backend.connection() as conn:
            if not parsed_path.container:
                acct_results = conn.get_account()
                if acct_results is not None:
                    _, paths = acct_results
                for container in paths:
                    results.append(container["name"])
            else:
                try:
                    container_results = conn.get_container(
                        parsed_path.container,
                        prefix=target_path,
                        delimiter=target._flavour.sep,
                    )
                except swiftclient.exceptions.ClientException:
                    raise FileNotFoundError(str(target))
                else:
                    if container_results is not None:
                        _, paths = container_results
                for p in paths:
                    if "subdir" in p:
                        results.append(str(p["subdir"]).strip(target._flavour.sep))
                    else:
                        results.append(str(p["name"]).strip(target._flavour.sep))
                results = [os.path.basename(str(r)) for r in results]
            return results

    @staticmethod
    def scandir(path: "SwiftPath") -> _SwiftScandir:
        return _SwiftScandir(swift_accessor=_SwiftAccessor, path=path)

    @staticmethod
    def chmod(target: "SwiftPath") -> None:
        raise NotImplementedError("chmod() is not available on this platform")

    def lchmod(self, pathobj: "SwiftPath", mode: int) -> None:
        raise NotImplementedError("lchmod() not available on this system")

    @staticmethod
    def mkdir(path: "SwiftPath", exist_ok: bool = False, parents: bool = False) -> None:
        """Create the provided directory.

        This operation is a no-op on swift.
        """
        parsed_path = ObjectPath.from_path(path)
        if path.exists() or path.joinpath(".swiftkeep").exists():
            if not exist_ok:
                raise FileExistsError(str(path))
        if path.key:
            path.joinpath(".swiftkeep").touch()
            return None
        with _SwiftAccessor.Backend.connection() as conn:
            try:
                conn.put_container(parsed_path.container)
            except swiftclient.exceptions.ClientException:
                raise FileExistsError(parsed_path.container)
        return None

    @staticmethod
    def unlink(path: "SwiftPath", missing_ok: bool) -> None:
        parsed_path = ObjectPath.from_path(path)
        with _SwiftAccessor.Backend.connection() as conn:
            try:
                conn.delete_object(parsed_path.container, parsed_path.key)
            except swiftclient.exceptions.ClientException:
                if not missing_ok:
                    raise FileNotFoundError(str(path))
        return None

    @staticmethod
    def link_to(
        src: "SwiftPath",
        link_name: Union[str, "SwiftPath"],
        *,
        src_dir_fd: Optional[int] = None,
        dst_dir_fd: Optional[int] = None,
        follow_symlinks: bool = True,
    ) -> None:
        if not isinstance(link_name, SwiftPath):
            target_path = SwiftPath(str(link_name))
        else:
            target_path = link_name
        parsed_path = ObjectPath.from_path(src)
        if not target_path.is_absolute():
            target_path = SwiftPath(f"/{parsed_path.container!s}/{target_path!s}")
        with _SwiftAccessor.Backend.connection() as conn:
            conn.copy_object(parsed_path.container, parsed_path.key, str(target_path))

    @staticmethod
    def rmdir(path: "SwiftPath", *args: Any, **kwargs: Any) -> None:
        # force = kwargs.pop("force", False)
        # if not force:
        #     contents = list(path.iterdir(include_swiftkeep=True, recurse=True))
        #     for p in contents:
        #         p.unlink()
        #     # else:
        #     #     raise OSError(
        #     #         "Object container directories are auto-destroyed when they are emptied"
        #     #     )
        #     # if contents and all(p.name == ".swiftkeep" for p in contents):
        #     #     for p in contents:
        #     #         if p.name == ".swiftkeep":
        #     #             p.unlink()
        #     return
        with _SwiftAccessor.Backend.connection() as conn:
            try:
                for item in path.iterdir():
                    if item.is_dir():
                        item.rmdir()
                    else:
                        parsed_path = ObjectPath.from_path(item)
                        conn.delete_object(parsed_path.container, parsed_path.key)
            except FileNotFoundError:
                return None
        return None

    @staticmethod
    def rename(path: "SwiftPath", target: Union[pathlib.PurePath, str]) -> None:
        caller_name = "[_SwiftAccessor.rename]"
        if not isinstance(target, SwiftPath):
            target_path = SwiftPath(str(target))
        else:
            target_path = target
        parsed_path = ObjectPath.from_path(path)
        if not target_path.is_absolute():
            target_path = SwiftPath(f"/{parsed_path.container!s}/{target!s}")
            log(
                f"{caller_name} Added {path.container!s} to target: {target!s}",
                level="debug",
            )
        with _SwiftAccessor.Backend.connection() as conn:
            if path.is_dir():
                for entry in path.iterdir():
                    sub_target = target_path.joinpath(entry.relative_to(path))
                    entry.rename(sub_target)
                path.rmdir()
            else:
                parsed_path = ObjectPath.from_path(path)
                container = parsed_path.container
                key = parsed_path.key
                log(
                    f"{caller_name} Renaming key: {key} from {container!s} to {target_path!s}",
                    level="debug",
                )
                conn.copy_object(container, key, str(target_path))
                path.unlink()

    @staticmethod
    def replace(path: "SwiftPath", target: "SwiftPath") -> None:
        return _SwiftAccessor.rename(path, target)

    @staticmethod
    def symlink(
        a: "SwiftPath",
        b: "SwiftPath",
        target_is_directory: bool = False,
        src_account: Optional[str] = None,
    ) -> None:
        if not a.exists():
            raise FileNotFoundError(a)
        if b.exists():
            raise FileExistsError(b)
        with _SwiftAccessor.Backend.connection() as conn:
            parsed_dest = ObjectPath.from_path(b)
            headers = {
                "X-Symlink-Target": str(a),
            }
            if src_account is not None:
                headers["X-Symlink-Target-Account"] = src_account
            conn.put_object(
                parsed_dest.container,
                parsed_dest.key,
                b"",
                content_length=0,
                content_type="application/symlink",
                headers=headers,
            )

    @staticmethod
    def utime(target: "SwiftPath") -> None:
        if not target.exists():
            raise FileNotFoundError(str(target))
        parsed_path = ObjectPath.from_path(target)
        with _SwiftAccessor.Backend.connection() as conn:
            conn.post_object(
                parsed_path.container,
                parsed_path.key,
                {"x-timestamp": str(datetime.datetime.now().timestamp())},
            )

    # Helper for resolve()
    def readlink(self, path: "SwiftPath") -> "SwiftPath":
        return path

    @property
    def backend(self):
        return self.Backend


_swift_flavour = _SwiftFlavour()


class PureSwiftPath(pathlib.PurePath):
    """Swift PurePath implementation for Openstack."""

    _flavour = _swift_flavour
    __slots__ = ()

    @classmethod
    def _parse_uri(cls, uri: str) -> urllib.parse.ParseResult:
        result = urllib.parse.urlparse(uri)
        # swift://container/path puts container in 'netloc'
        # we want to keep it in the 'path' field as it is not a netloc
        if result.scheme == "swift" and result.netloc:
            container = result.netloc.strip("/")
            path = f"/{container}"
            if result.path:
                path = f"{path}/{result.path}"
            result = result._replace(netloc="", path=path)
        return result

    @classmethod
    def from_uri(cls, uri: str) -> "PureSwiftPath":
        """This method provides ``from_uri`` to the ``SwiftPath`` class below.

        It is not meant to be called directly.
        """
        if not uri.startswith("swift://"):
            raise ValueError(f"Expecting a `swift://` URI, got {uri}")
        return cls(cls._parse_uri(uri).path)

    @property
    def container(self):
        """Container name for the given path.

        Represented by the first part of an absolute path
        """
        if not self.is_absolute():
            raise ValueError("Must provide an absolute path to determine container")
        try:
            _, container, *_ = self.parts
        except ValueError:
            return None
        return SwiftPath(self._flavour.sep, container)

    @property
    def key(self):
        """The key name for the given path with the bucket removed."""
        if not self.is_absolute():
            raise ValueError("Must provide an absolute path to determine key")
        key = self._flavour.sep.join(self.parts[2:])
        if not key:
            return None
        return SwiftPath(key)

    def as_uri(self):
        """Return the path as swift URI."""
        return super().as_uri()


_swift_accessor: _SwiftAccessor = _SwiftAccessor()


class SwiftPath(pathlib.Path, PureSwiftPath):
    _flavour = _SwiftFlavour()

    __slots__ = ()

    def touch(self, mode=0o666, exist_ok=True) -> None:
        if not self.exists():
            self.write_bytes(b"")
        else:
            self._accessor.utime(self)
        return None

    def is_dir(self) -> bool:
        """Check whether the provided path is a directory."""
        if str(self) == self.root:
            return True
        parsed_path = ObjectPath.from_path(self)
        path = parsed_path.key if parsed_path.key else ""
        if path == ".":
            path = ""
        if path and not path.endswith(self._flavour.sep):
            path = f"{path}{self._flavour.sep}"
        files = []
        with self._accessor.backend.connection() as conn:
            try:
                container_and_files = conn.get_container(
                    parsed_path.container, prefix=path
                )

            except swiftclient.exceptions.ClientException:
                return False
            else:
                if container_and_files is not None:
                    _, files = container_and_files
            return bool(files)

    def is_file(self) -> bool:
        """Check whether the provided path is a file."""
        if not self.is_absolute():
            raise ValueError(
                f"Container name is required to open files on Swift, got {self!s}"
            )
        parsed_path = ObjectPath.from_path(self)
        if parsed_path.key and parsed_path.key == ".":
            return False
        if not self.container or not self.key:
            return False
        with self._accessor.backend.connection() as conn:
            try:
                conn.head_object(parsed_path.container, parsed_path.key)
            except swiftclient.exceptions.ClientException:
                return False
            else:
                return True

    def mkdir(
        self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False
    ) -> None:
        """Create a new container or prefix.

        :param int mode: The mode to create, defaults to 0o777
        :param bool parents: Whether to create missing parents, defaults to False
        :param bool exist_ok: Whether to ignore errors thrown if the path exists,
            defaults to False
        """
        try:
            if self.key is not None and not parents:
                raise FileNotFoundError(
                    "Only bucket path can be created, got {}".format(self)
                )
            if self.container.exists() and not self.key:
                raise FileExistsError(
                    "Container {} already exists".format(self.container)
                )
            return super().mkdir(mode, parents=parents, exist_ok=exist_ok)
        except OSError:
            if not exist_ok:
                raise

    def is_symlink(self) -> bool:
        """Check whether the provided path is a symlink."""
        parsed_path = ObjectPath.from_path(self)
        with self._accessor.backend.connection() as conn:
            try:
                headers = conn.head_object(
                    parsed_path.container, parsed_path.key, query_string="symlink=get"
                )
            except swiftclient.exceptions.ClientException:
                raise FileNotFoundError(str(self))
            if headers and headers.get("content-type", "") == "application/symlink":
                return True
        return False

    def exists(self) -> bool:
        """Check whether the provided path exists."""
        return any([self.is_dir(), self.is_file()])

    def rename(  # type: ignore[override]
        self, target: Union[str, pathlib.PurePath]
    ) -> "SwiftPath":
        if self._closed:  # type: ignore
            self._raise_closed()
        self._accessor.rename(self, target)
        return self.__class__(target)

    def replace(self, target: Union[str, PurePath]) -> "SwiftPath":  # type: ignore[override]
        """Renames this container / key prefix / key to the given target.

        If target points to an existing container / key prefix / key, it
        will be unconditionally replaced.
        """
        caller_name = "[SwiftPath.replace]"
        if not self.is_absolute():
            raise ValueError(
                f"Container name is required to open files on Swift, got {self!s}"
            )
        log(
            f"{caller_name} Called `replace` using {self!s} with argument {target!s}"
            "-> calling self.rename",
            level="debug",
        )
        return self.rename(target)

    def symlink_to(  # type: ignore
        self,
        src: Union[str, "SwiftPath"],
        target_is_directory: bool = False,
        src_container: Optional[str] = None,
        src_account: Optional[str] = None,
    ) -> None:
        """Make this path a symlink pointing to the given path.

        Note the order of arguments (self, target) is the reverse of
        os.symlink's.
        """
        if not isinstance(src, type(self)):
            src = type(self)(src)
        self._accessor.symlink(
            src,  # type: ignore
            self,
            target_is_directory=target_is_directory,
            src_account=src_account,
        )

    def unlink(self, missing_ok: bool = False) -> None:
        self._accessor.unlink(self, missing_ok=missing_ok)

    def iterdir(
        self,
        conn: Optional[swiftclient.client.Connection] = None,
        recurse: bool = False,
        include_swiftkeep: bool = False,
    ) -> Generator["SwiftPath", None, None]:
        """Iterate over the files in this directory.

        Does not yield any result for the special paths '.' and '..'.
        """
        for name in self._accessor.listdir(self):
            if name in {".", ".."} or name == ".swiftkeep" and not include_swiftkeep:
                # Yielding a path object for these makes little sense
                continue
            path = self._make_child_relpath(name)
            if not recurse or not path.is_dir():
                yield path
            else:
                # yield path
                yield from path.iterdir(conn=conn, recurse=recurse)

    def glob(self, pattern):
        """Glob the given relative pattern in the given path, yielding all
        matching files (of any kind)"""
        yield from super().glob(pattern)

    def rglob(self, pattern):
        """This is like calling SwiftPath().glob with "**/" added in front of
        the given relative pattern."""
        yield from super().rglob(pattern)

    def _raise_closed(self):
        raise ValueError("I/O operation on closed path")

    def open(
        self,
        mode="r",
        buffering=io.DEFAULT_BUFFER_SIZE,
        encoding=None,
        errors=None,
        newline=None,
    ):
        """Opens the provided container and key, returning a read/writable file
        object."""
        # non-binary files won't error if given an encoding, but we will open them in
        # binary mode anyway, so we need to fix this first
        if "w" in mode and "b" not in mode and encoding is not None:
            encoding = None
        if not self.is_absolute():
            raise ValueError(
                f"Container name is required to open files on Swift, got {self!s}"
            )
        if mode not in _SUPPORTED_OPEN_MODES:
            raise ValueError(
                "supported modes are {} got {}".format(_SUPPORTED_OPEN_MODES, mode)
            )
        if buffering == 0 or buffering == 1:
            raise ValueError(
                "supported buffering values are only block sizes, no 0 or 1"
            )
        if "b" in mode and encoding:
            raise ValueError("binary mode doesn't take an encoding argument")

        if self._closed:
            self._raise_closed()
        return self._accessor.open(
            self,
            mode=mode,
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline,
        )

    def is_mount(self) -> bool:
        return False

    def is_fifo(self) -> bool:
        return False

    def is_socket(self) -> bool:
        return False

    @classmethod
    def cwd(cls):
        raise NotImplementedError("Operation: cwd is not supported by swift")

    @classmethod
    def home(cls):
        raise NotImplementedError("Operation: home is not supported by swift")

    def chmod(self, mode: int):
        raise NotImplementedError("Operation: chmod is not supported by Swift")

    def expanduser(self):
        raise NotImplementedError("Operation: expanduser is not supported by Swift")

    def lchmod(self, mode: int):
        raise NotImplementedError("Operation: lchmod is not supported by Swift")

    def group(self):
        raise NotImplementedError("Operation: group is not supported by Swift")

    def is_block_device(self):
        raise NotImplementedError(
            "Operation: is_block_device is not supported by Swift"
        )

    def is_char_device(self):
        raise NotImplementedError("Operation: is_char_device is not supported by Swift")

    def lstat(self):
        raise NotImplementedError("Operation: lstat is not supported by Swift")

    def resolve(self):
        raise NotImplementedError("Operation: resolve is not supported by Swift")

    def __new__(cls, *args: Any, **kwargs: Any) -> "SwiftPath":
        self: "SwiftPath"
        self = cls._from_parts(args, init=False)  # type: ignore
        if not self._flavour.is_supported:
            raise NotImplementedError(f"Cannot instantiate {cls.__name__!r}")
        self._init()
        return self

    def _make_child_relpath(self, part):
        parts = self._parts + [part]  # type: ignore
        child: "SwiftPath"
        child = self._from_parsed_parts(self._drv, self._root, parts)  # type: ignore
        return child

    def _init(self, template=None):
        self._closed = False
        super()._init(template)  # type: ignore
        if template is None:
            self._accessor = _swift_accessor


def decode(
    content: Union[str, bytes, memoryview, IO],
    mode: str = "",
    encoding: Optional[str] = "utf-8",
) -> Union[str, bytes]:
    rv: Union[str, bytes] = b"" if "b" in mode else ""
    if encoding is None:
        encoding = "utf-8"
    if isinstance(content, memoryview):
        if "b" not in mode:
            rv = content.tobytes().decode(encoding=encoding)
        else:
            rv = content.tobytes()
    elif isinstance(content, IO):
        rv = decode(content.read())
    elif isinstance(content, bytes):
        if "b" not in mode:
            rv = content.decode(encoding=encoding)
        else:
            rv = content
    elif isinstance(content, str):
        if "b" in mode:
            rv = content.encode(encoding=encoding)
        else:
            rv = content
    return rv


class SwiftKeyWritableFileObject(IO, io.RawIOBase):
    def __init__(
        self,
        path,
        *,
        mode="w",
        buffering=io.DEFAULT_BUFFER_SIZE,
        encoding=None,
        errors=None,
        newline=None,
    ):
        super().__init__()
        mode = mode.rstrip("+")
        self.path = path
        self.parsed_path = ObjectPath.from_path(self.path)
        self._write_mode = mode if "b" in mode else f"{mode}b"
        self._mode = mode
        self.buffering = buffering
        self.encoding = encoding
        self.errors = errors
        self.newline = newline
        self._context = contextlib.ExitStack()
        self._cache = self._context.enter_context(
            tempfile.NamedTemporaryFile(
                mode=self._write_mode + "+",
                buffering=self.buffering,
                encoding=self.encoding,
                newline=self.newline,
            )
        )
        atexit.register(self._cache.close)

    @property
    def mode(self):
        return self._cache.mode

    def __enter__(self):
        return self

    def __exit__(self, exc_typ, exc_val, exc_tb) -> None:
        if not exc_typ:
            self._cache.flush()
            self._cache.seek(0)
            with _SwiftAccessor.Backend.connection() as conn:
                conn.put_object(
                    self.parsed_path.container, self.parsed_path.key, self._cache
                )

    def __getattr__(self, item):
        try:
            return getattr(self._cache, item)
        except AttributeError:
            return super().__getattribute__(item)

    def writable(self, *args, **kwargs):
        return "w" in self.mode

    def encode(
        self, text: Union[str, bytes, io.BufferedIOBase, memoryview]
    ) -> Union[str, bytes]:
        encoding = self.encoding if self.encoding else "utf-8"
        contents: Union[str, bytes]
        errors = self.errors
        if not errors:
            try:
                errors = sys.getfilesystemencodeerrors()  # type: ignore
            except AttributeError:
                errors = "surrogateescape"
        if isinstance(text, memoryview):
            if "b" not in self._write_mode:
                contents = text.tobytes().decode(encoding=encoding, errors=errors)
            else:
                contents = text.tobytes()
        elif isinstance(text, str) and "b" in self._write_mode:
            contents = text.encode(encoding=encoding, errors=errors)
        elif isinstance(text, bytes) and "b" not in self._write_mode:
            contents = text.decode(encoding=encoding, errors=errors)
        elif isinstance(text, io.BufferedIOBase):
            contents = self.encode(text.read())
        elif (isinstance(text, str) and "b" not in self._write_mode) or (
            isinstance(text, bytes) and "b" in self._write_mode
        ):
            return text
        else:
            raise TypeError(f"Invalid type to encode: {text!r}")
        return contents

    @property
    def name(self) -> str:
        return str(self.path)

    def _write_cache(self) -> int:
        size: int = self._cache.tell()
        self._cache.seek(0)
        with _SwiftAccessor.Backend.connection() as conn:
            conn.put_object(
                self.parsed_path.container, self.parsed_path.key, self._cache
            )
        return size

    def write(self, s: AnyStr) -> int:
        self._cache.write(self.encode(s))
        return self._write_cache()

    def writelines(self, lines: Iterable) -> None:  # type: ignore[override]
        encoded_newline = self.encode("\n")
        encoded_lines = [self.encode(line) for line in lines]
        encoded_str = encoded_newline.join(encoded_lines)  # type: ignore
        if "b" in self.mode:
            assert isinstance(encoded_str, bytes)
            self._cache.write(encoded_str)
        else:
            assert isinstance(encoded_str, str)
            self._cache.write(encoded_str)
        return None

    def readable(self):
        return "+" in self.mode and sys.platform != "win32"

    def read(self, *args, **kwargs):
        if not self.readable():
            raise io.UnsupportedOperation("Cannot read write-only file")
        self._cache.seek(0)
        return decode(self._cache.read(), self.mode, self.encoding)

    def readline(self, limit: int = -1):
        if not self.readable():
            raise io.UnsupportedOperation("Cannot read write-only file")
        return decode(self._cache.readline(), self.mode, encoding=self.encoding)

    def readlines(self, hint: int = -1):
        if not self.readable():
            raise io.UnsupportedOperation("Cannot read write-only file")
        self._cache.seek(0)
        return [
            decode(line, self.mode, self.encoding) for line in self._cache.readlines()
        ]


def iter_slices(
    string: Union[str, bytes], slice_length: Union[int, None]
) -> Generator[Union[str, bytes], None, None]:
    """Iterate over slices of a string."""
    pos = 0
    if slice_length is None or slice_length <= 0:
        slice_length = len(string)
    while pos < len(string):
        yield string[pos : pos + slice_length]
        pos += slice_length


class SwiftKeyReadableFileObject(IO, io.RawIOBase):
    def __init__(
        self,
        path,
        *,
        mode: str = "b",
        buffering: int = io.DEFAULT_BUFFER_SIZE,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        newline: Optional[Union[str, bytes]] = None,
    ):
        super().__init__()
        self.path = path
        self.parsed_path = ObjectPath.from_path(self.path)
        self._mode = mode
        self.buffering = buffering
        self.encoding = encoding
        self._errors = errors
        self.newline = newline
        self._content: Union[str, bytes] = decode(b"", self.mode, self.encoding)
        self._streaming_body: Optional[swiftclient.client._RetryBody] = None
        self._line_iter = None
        self._content_consumed = False

    @property
    def mode(self) -> str:
        return self._mode

    def errors(self) -> Optional[str]:
        return self._errors

    def __iter__(self):
        return self

    def __next__(self):
        return self.readline()

    def __getattr__(self, item):
        try:
            return getattr(self._streaming_body, item)
        except AttributeError:
            return super().__getattribute__(item)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def name(self) -> str:
        return str(self.path)

    def close(self, *args, **kwargs):
        super().close(*args, **kwargs)

    def decode_b64(self, content: bytes) -> bytes:
        if len(content) % 4 == 0 and BASE64_RE.fullmatch(content):
            return base64.b64decode(content)
        return content

    def iter_content(
        self, chunk_size: int = 1
    ) -> Generator[Union[str, bytes], None, None]:
        def generate() -> Generator[Union[str, bytes], None, None]:
            while True:
                chunk = self.read(chunk_size)
                if not chunk:
                    break
                yield chunk
            self._content_consumed = True

        reused_chunks = iter_slices(self._content, chunk_size)
        stream_chunks = generate()
        chunks = reused_chunks if self._content_consumed else stream_chunks
        return chunks

    def iter_lines(
        self,
        chunk_size: int = 512,
        decode_unicode: bool = False,
        delimiter: Optional[Union[str, bytes]] = None,
    ) -> Generator[Union[str, bytes], None, None]:
        """Iterates over the response data, one line at a time.  When
        stream=True is set on the request, this avoids reading the content at
        once into memory for large responses.

        .. note:: This method is not reentrant safe.
        """

        pending = None

        for chunk in self.iter_content(chunk_size=chunk_size):

            if pending is not None:
                chunk = pending + chunk  # type: ignore

            if delimiter:
                try:
                    lines = chunk.split(delimiter)  # type: ignore
                except TypeError:
                    lines = chunk.splitlines()
            else:
                lines = chunk.splitlines()

            if lines and lines[-1] and chunk and lines[-1][-1] == chunk[-1]:
                pending = lines.pop()
            else:
                pending = None

            for line in lines:
                yield decode(line, self.mode, self.encoding)

        if pending is not None:
            yield decode(pending, self.mode, self.encoding)

    def readable(self) -> bool:
        if "r" not in self.mode:
            return False
        with contextlib.ExitStack() as stack:
            stack.enter_context(
                contextlib.suppress(swiftclient.exceptions.ClientException)
            )
            conn = stack.enter_context(_SwiftAccessor.Backend.connection())
            if self._streaming_body is None:
                _, file_contents = conn.get_object(
                    self.parsed_path.container,
                    self.parsed_path.key,
                    resp_chunk_size=self.buffering,
                )
                self._streaming_body = file_contents
            return True
        return False

    def read(self, n: int = -1):
        if not self.readable():
            raise io.UnsupportedOperation("not readable")
        result = b""
        if self._streaming_body is not None:
            result = self._streaming_body.read() or b""
        rv = decode(result, self.mode, self.encoding)
        return rv

    def readlines(  # type: ignore[override]
        self, hint: int = -1
    ) -> Union[List[str], List[bytes]]:
        if not self.readable():
            raise io.UnsupportedOperation("not readable")
        join_str: Union[bytes, str] = decode(b"", self.mode, self.encoding)
        self._content = join_str.join(self.iter_content(512)) or join_str  # type: ignore
        self._content_consumed = True
        rv = self._content.splitlines()
        return rv

    def readline(self, limit: int = -1) -> Union[str, bytes]:  # type: ignore[override]
        if not self.readable():
            raise io.UnsupportedOperation("not readable")
        try:
            line = next(self.iter_lines())
        except (StopIteration, ValueError, StreamConsumedError):
            line = b""  # type: ignore
        rv = decode(line, self.mode, self.encoding)
        return rv

    def write(self, s: AnyStr) -> int:
        raise io.UnsupportedOperation("Read-only file is not writeable")

    def writelines(self, lines: Iterable) -> None:
        raise io.UnsupportedOperation("Read-only file is not writeable")

    def writable(self, *args: Any, **kwargs: Any) -> bool:
        return False


def convert_to_timestamp(
    last_modified_timestamp: Optional[Union[str, int, float, datetime.datetime]]
) -> Optional[datetime.datetime]:
    if last_modified_timestamp is None:
        return None
    if isinstance(last_modified_timestamp, datetime.datetime):
        return last_modified_timestamp
    try:
        return datetime.datetime.fromtimestamp(float(last_modified_timestamp))
    except ValueError:
        if isinstance(last_modified_timestamp, str):
            return fromisoformat(last_modified_timestamp)
    raise TypeError(f"Cannot convert {last_modified_timestamp!r} to timestamp")


def optional_float_inst(optional_float: Optional[Union[str, float]]) -> Optional[float]:
    if optional_float is not None:
        return float(optional_float)
    return None


@attr.s(frozen=True)
class StatResult(AttrProto):
    """os.stat result-like tuple for storing Swift stat results."""

    size = attr.ib(type=Optional[float], converter=optional_float_inst, default=None)
    last_modified = attr.ib(
        type=Optional[datetime.datetime], converter=convert_to_timestamp, default=None
    )

    def __getattr__(self, item):
        if item in vars(posix.stat_result):
            raise io.UnsupportedOperation(
                "{} do not support {} attribute".format(type(self).__name__, item)
            )
        return super().__getattribute__(item)

    @property
    def st_size(self):
        return self.size

    @property
    def st_mtime(self):
        return self.last_modified.timestamp()


# XXX: Approach borrowed from https://github.com/liormizr/s3path/blob/4ba7ad7/s3path.py#L859
# for API consistency - Apache licensed
class SwiftDirEntry:
    def __init__(self, name, is_dir, size=None, last_modified=None, is_symlink=False):
        self.name: str = name
        self._is_dir: bool = is_dir
        self._size = size
        self._last_modified = last_modified
        self._stat = StatResult(size=size, last_modified=last_modified)
        self._is_symlink: bool = is_symlink

    def __repr__(self):
        return "{}(name={}, is_dir={}, stat={})".format(
            type(self).__name__, self.name, self._is_dir, self._stat
        )

    def inode(self, *args, **kwargs):
        return None

    def is_dir(self) -> bool:
        return self._is_dir

    def is_file(self) -> bool:
        return not self._is_dir

    def is_symlink(self, *args, **kwargs) -> bool:
        return self._is_symlink

    def stat(self):
        return self._stat
