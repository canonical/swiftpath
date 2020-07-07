import io
import sys
import tempfile
from pathlib import Path

import pytest
from swiftclient.exceptions import ClientException

import swiftpath.swiftpath


def test_path_support():
    from swiftpath.swiftpath import PureSwiftPath, SwiftPath

    assert PureSwiftPath in SwiftPath.mro()
    assert Path in SwiftPath.mro()


def test_stat(mock_swift, mock_swiftpath):
    path = mock_swiftpath("fake-bucket/fake-key")
    with pytest.raises(ValueError):
        path.stat()

    path = mock_swiftpath("/fake-bucket/fake-key")
    with pytest.raises(FileNotFoundError):
        path.stat()
    mock_swift.put_container("test-container")
    mock_swift.put_object(
        container="test-container", obj="Test.test", contents=b"test data"
    )
    headers = mock_swift.head_object(container="test-container", obj="Test.test")
    mtime = float(headers.get("x-object-meta-mtime", headers.get("x-timestamp", "0")))
    size = float(headers.get("content-length", 0))

    path = mock_swiftpath("/test-container/Test.test")
    stat = path.stat()

    assert isinstance(stat, swiftpath.swiftpath.StatResult)
    assert stat == swiftpath.swiftpath.StatResult(size=size, last_modified=mtime,)

    with tempfile.NamedTemporaryFile() as local_file:
        local_file.write(path.read_bytes())
        local_file.flush()
        local_path = Path(local_file.name)

        local_stat = local_path.stat()
        swift_stat = path.stat()

        assert swift_stat.st_size == local_stat.st_size == swift_stat.size
        assert swift_stat.last_modified.timestamp() == swift_stat.st_mtime
        assert swift_stat.st_mtime < local_stat.st_mtime

    with pytest.raises(io.UnsupportedOperation):
        path.stat().st_atime

    path = mock_swiftpath("/test-container")
    assert path.stat() is not None


def test_exists(mock_swift, mock_swiftpath):
    path = mock_swiftpath("./fake-key")
    with pytest.raises(ValueError):
        path.exists()

    path = mock_swiftpath("/fake-bucket/fake-key")
    assert path.exists() is False
    mock_swift.put_container("test-container")
    mock_swift.put_object(
        "test-container", "directory/Test.test", contents=b"test data"
    )

    assert not mock_swiftpath("/test-container/Test.test").exists()
    path = mock_swiftpath("/test-container/directory/Test.test")
    assert path.exists()
    for parent in path.parents:
        assert parent.exists()


@pytest.mark.parametrize(
    "glob_search, glob_result",
    [
        ("directory/*.test", ["/test-container/directory/Test.test"]),
        ("**/*.test", ["/test-container/directory/Test.test"]),
        (
            "*.py",
            [
                "/test-container/pathlib.py",
                "/test-container/setup.py",
                "/test-container/test_pathlib.py",
            ],
        ),
        ("*/*.py", ["/test-container/docs/conf.py"]),
        (
            "**/*.py",
            [
                "/test-container/build/lib/pathlib.py",
                "/test-container/docs/conf.py",
                "/test-container/pathlib.py",
                "/test-container/setup.py",
                "/test-container/test_pathlib.py",
            ],
        ),
        ("*cs", ["/test-container/docs"]),
    ],
)
def test_glob(mock_swift, mock_swiftpath, glob_search, glob_result):
    src_container = "test-container"
    paths = (
        "pathlib.py",
        "setup.py",
        "test_pathlib.py",
        "docs/conf.py",
        "build/lib/pathlib.py",
        "directory/Test.test",
    )
    mock_swift.put_container(src_container)
    assert list(mock_swiftpath(f"/{src_container}/").glob("*.test")) == []
    for path in paths:
        mock_swift.put_object(src_container, path, contents=b"test data")
    assert list(sorted(mock_swiftpath(f"/{src_container}/").glob(glob_search))) == list(
        sorted([mock_swiftpath(p) for p in glob_result])
    )
    path_from_uri = mock_swiftpath.from_uri(f"swift://{src_container}/")
    glob_list = sorted(list(path_from_uri.glob(glob_search)))
    result_glob_list = sorted([mock_swiftpath(p) for p in glob_result])
    assert (
        glob_list == result_glob_list
    ), f"Path: {path_from_uri}\nGlob list: {glob_list}"


@pytest.mark.parametrize(
    "glob_search, result",
    [
        ("*.test", ["directory/Test.test"]),
        ("**/*.test", ["directory/Test.test"]),
        (
            "*.py",
            sorted(
                (
                    "pathlib.py",
                    "setup.py",
                    "test_pathlib.py",
                    "docs/conf.py",
                    "build/lib/pathlib.py",
                )
            ),
        ),
    ],
)
def test_rglob(mock_swift, mock_swiftpath, glob_search, result):
    src_container = "test-container"
    paths = (
        "pathlib.py",
        "setup.py",
        "test_pathlib.py",
        "docs/conf.py",
        "build/lib/pathlib.py",
        "directory/Test.test",
    )
    mock_swift.put_container(src_container)
    for path in paths:
        mock_swift.put_object(src_container, path, contents=b"test data")
    path = mock_swiftpath(f"/{src_container}/directory")
    with path._accessor.backend.connection() as conn:
        _, files = conn.get_container(str(path.container))
        assert type(conn) == type(mock_swift)
    glob_result = list(sorted(mock_swiftpath(f"/{src_container}/").rglob(glob_search)))
    expected = [mock_swiftpath(f"/{src_container}/{path}") for path in result]
    assert len(glob_result) == len(expected)
    for i, out in enumerate(glob_result):
        assert (
            out == expected[i]
        ), f"Result cparts: {out._cparts}\nExpected cparts: {expected[i]._cparts}"

    glob_result = sorted(
        mock_swiftpath.from_uri(f"swift://{src_container}/").rglob(f"{glob_search}")
    )
    expected = [mock_swiftpath(f"/{src_container}/{path}") for path in result]
    assert len(glob_result) == len(expected)
    for i, out in enumerate(glob_result):
        assert (
            out == expected[i]
        ), f"Result cparts: {out._cparts}\nExpected cparts: {expected[i]._cparts}"


@pytest.mark.parametrize(
    "src_container, paths, fake_paths",
    [
        (
            "test-container",
            (
                "directory/Test.test",
                "pathlib.py",
                "setup.py",
                "test_pathlib.py",
                "build/lib/pathlib.py",
                "docs/conf.py",
            ),
            ("fake.test", "fake/", "fakedir"),
        )
    ],
)
def test_is_dir(mock_swift, mock_swiftpath, src_container, paths, fake_paths):
    mock_swift.put_container(src_container)
    for path in paths:
        mock_swift.put_object(src_container, path, contents=b"test data")
    dirs = ["docs/", "build/", "build/lib/"]
    for path in paths + fake_paths:
        assert mock_swiftpath(f"/{src_container}/{path}").is_dir() is False
    for path in dirs:
        assert mock_swiftpath(f"/{src_container}/{path}").is_dir() is True


@pytest.mark.parametrize(
    "src_container, paths, fake_paths",
    [
        (
            "test-container",
            (
                "directory/Test.test",
                "pathlib.py",
                "setup.py",
                "test_pathlib.py",
                "build/lib/pathlib.py",
                "docs/conf.py",
            ),
            ("fake.test", "fake/", "fakedir"),
        )
    ],
)
def test_is_file(mock_swift, mock_swiftpath, src_container, paths, fake_paths):
    mock_swift.put_container(src_container)
    for path in paths:
        mock_swift.put_object(src_container, path, contents=b"test data")
    dirs = ["docs/", "build/", "build/lib/"]
    for path in dirs + list(fake_paths):
        assert mock_swiftpath(f"/{src_container}/{path}").is_file() is False
    for path in paths:
        assert mock_swiftpath(f"/{src_container}/{path}").is_file() is True


@pytest.mark.parametrize(
    "src_container, paths",
    [
        (
            "test-container",
            (
                "directory/Test.test",
                "pathlib.py",
                "setup.py",
                "test_pathlib.py",
                "build/lib/pathlib.py",
                "docs/conf.py",
                "docs/make.bat",
                "docs/index.rst",
                "docs/Makefile",
                "docs/_templates/11conf.py",
                "docs/_build/22conf.py",
                "docs/_static/conf.py",
            ),
        )
    ],
)
def test_iterdir(mock_swift, mock_swiftpath, src_container, paths):
    mock_swift.put_container(src_container)
    for path in paths:
        mock_swift.put_object(src_container, path, contents=b"test data")

    def get_first_two(path):
        return "/".join(Path(path).parts[:2])

    swift_path = mock_swiftpath(f"/{src_container}/docs")
    assert sorted(swift_path.iterdir()) == [
        mock_swiftpath(f"/{src_container}/{get_first_two(path)}")
        for path in sorted(paths)
        if path.startswith("docs")
    ]


def test_open_for_reading(mock_swift, mock_swiftpath):
    mock_swift.put_container("test-container")
    mock_swift.put_object(
        "test-container", "directory/Test.test", contents=b"test data"
    )

    path = mock_swiftpath("/test-container/directory/Test.test")
    file_obj = path.open()
    assert file_obj.read() == "test data"


@pytest.mark.skip("streaming is not yet implementend for swift")
def test_open_for_write(mock_swift, mock_swiftpath):
    mock_swift.put_container("test-container")
    _, objects = mock_swift.get_container("test-container")
    assert len(objects) == 0

    path = mock_swiftpath("/test-container/directory/Test.test")
    file_obj = path.open(mode="bw")
    assert file_obj.writable()
    file_obj.write(b"test data\n")
    file_obj.writelines([b"test data"])

    _, objects = mock_swift.get_container("test-container")
    assert len(objects) == 1

    _, obj = mock_swift.get_object("test-container", "directory/Test.test")
    streaming_body = obj

    assert list(streaming_body.iter_lines()) == [b"test data", b"test data"]


def test_open_binary_read(mock_swift, mock_swiftpath):
    mock_swift.put_container("test-container")
    mock_swift.put_object(
        "test-container", "directory/Test.test", contents=b"test data"
    )

    path = mock_swiftpath("/test-container/directory/Test.test")
    with path.open(mode="br") as file_obj:
        assert file_obj.readlines() == [b"test data"]

    with path.open(mode="rb") as file_obj:
        assert file_obj.readline() == b"test data"
        assert file_obj.readline() == b""
        assert file_obj.readline() == b""


@pytest.mark.skipif(sys.version_info < (3, 5), reason="requires python3.5 or higher")
def test_read_bytes(mock_swift, mock_swiftpath):
    mock_swift.put_container("test-container")
    mock_swift.put_object(
        "test-container", "directory/Test.test", contents=b"test data"
    )

    path = mock_swiftpath("/test-container/directory/Test.test")
    assert path.read_bytes() == b"test data"


def test_open_text_read(mock_swift, mock_swiftpath):
    mock_swift.put_container("test-container")
    mock_swift.put_object(
        "test-container", "directory/Test.test", contents=b"test data"
    )

    path = mock_swiftpath("/test-container/directory/Test.test")
    with path.open(mode="r") as file_obj:
        assert file_obj.readlines() == ["test data"]

    with path.open(mode="rt") as file_obj:
        assert file_obj.readline() == "test data"
        assert file_obj.readline() == ""
        assert file_obj.readline() == ""


@pytest.mark.skipif(sys.version_info < (3, 5), reason="requires python3.5 or higher")
def test_read_text(mock_swift, mock_swiftpath):
    mock_swift.put_container("test-container")
    mock_swift.put_object(
        "test-container", "directory/Test.test", contents=b"test data"
    )

    path = mock_swiftpath("/test-container/directory/Test.test")
    assert path.read_text() == "test data"


@pytest.mark.parametrize(
    "src_container, target_container, paths",
    [
        (
            "test-container",
            "target-container",
            (
                "docs/conf.py",
                "docs/make.bat",
                "docs/index.rst",
                "docs/Makefile",
                "docs/_templates/11conf.py",
                "docs/_build/22conf.py",
                "docs/_static/conf.py",
            ),
        )
    ],
)
def test_rename_swift_to_swift(
    mock_swift, mock_swiftpath, src_container, target_container, paths
):
    mock_swift.put_container(src_container)
    mock_swift.put_container(target_container)
    for path in paths:
        mock_swift.put_object(src_container, path, contents=b"test data")

    rename_file = mock_swiftpath("/test-container/docs/conf.py")
    assert rename_file.exists()
    rename_to = rename_file.with_name(f"{rename_file.stem}1{rename_file.suffix}")
    rename_file.rename(rename_to)
    assert rename_file.exists() is False
    assert rename_to.is_file() is True
    base_folder = mock_swiftpath("/test-container/docs")
    target_folder = mock_swiftpath("/target-container/folder")
    base_folder.rename(target_folder)
    assert base_folder.exists() is False
    for path in paths:
        if path == "docs/conf.py":
            path = "docs/conf1.py"
        assert target_folder.joinpath(path.replace("docs/", "")).is_file()


@pytest.mark.parametrize(
    "src_container, target_container, paths",
    [
        (
            "test-container",
            "target-container",
            (
                "docs/conf.py",
                "docs/make.bat",
                "docs/index.rst",
                "docs/Makefile",
                "docs/_templates/11conf.py",
                "docs/_build/22conf.py",
                "docs/_static/conf.py",
            ),
        )
    ],
)
def test_replace_swift_to_swift(
    mock_swift, mock_swiftpath, src_container, target_container, paths
):
    mock_swift.put_container(src_container)
    mock_swift.put_container(target_container)
    for path in paths:
        mock_swift.put_object(src_container, path, contents=b"test data")

    replace_file = mock_swiftpath("/test-container/docs/conf.py")
    assert replace_file.exists()
    rename_to = replace_file.with_name(f"{replace_file.stem}1{replace_file.suffix}")
    replace_file.replace(str(rename_to))
    assert replace_file.exists() is False
    assert rename_to.is_file() is True
    base_folder = mock_swiftpath("/test-container/docs")
    target_folder = mock_swiftpath("/target-container/folder")
    base_folder.replace(target_folder)
    assert base_folder.exists() is False
    for path in paths:
        if path == "docs/conf.py":
            path = "docs/conf1.py"
        assert target_folder.joinpath(path.replace("docs/", "")).is_file()


@pytest.mark.parametrize(
    "src_container, paths",
    [
        (
            "test-container",
            (
                "docs/conf.py",
                "docs/make.bat",
                "docs/index.rst",
                "docs/Makefile",
                "docs/_templates/11conf.py",
                "docs/_build/22conf.py",
                "docs/_static/conf.py",
            ),
        )
    ],
)
def test_rmdir(mock_swift, mock_swiftpath, src_container, paths):
    mock_swift.put_container(src_container)
    for path in paths:
        mock_swift.put_object(src_container, path, contents=b"test data")

    dirs = ["docs/_templates", "docs/_build", "docs/_static", "docs"]
    for dir_ in dirs:
        path = mock_swiftpath(f"/{src_container}/{dir_}")
        assert path.is_dir() is True
        path.rmdir()
        assert path.exists() is False


def test_mkdir(mock_swift, mock_swiftpath):

    mock_swiftpath("/test-container/").mkdir()
    # ensure the container is created
    assert "test-container" in [c["name"] for c in mock_swift.get_account()[1]]
    # make sure this doesn't raise an error
    mock_swiftpath("/test-container/").mkdir(exist_ok=True)
    # make sure this does raise an error
    with pytest.raises(FileExistsError):
        mock_swiftpath("/test-container/").mkdir(exist_ok=False)
    # make sure we can't recursively create directories in non-existent containers
    with pytest.raises(FileNotFoundError):
        mock_swiftpath("/test-second-container/test-directory/file.name").mkdir()

    mock_swiftpath("/test-second-container/test-directory/file.name").mkdir(
        parents=True
    )

    assert "test-second-container" in [c["name"] for c in mock_swift.get_account()[1]]


def test_write_text(mock_swift, mock_swiftpath):

    mock_swift.put_container("test-container")
    mock_swift.put_object("test-container", "temp_key", contents=b"test data")

    path = mock_swiftpath("/test-container/temp_key")
    data = path.read_text()
    assert isinstance(data, str)

    path.write_text(data)
    assert path.read_text() == data


def test_write_bytes(mock_swift, mock_swiftpath):

    mock_swift.put_container("test-container")
    mock_swift.put_object("test-container", "temp_key", contents=b"test data")

    path = mock_swiftpath("/test-container/temp_key")
    data = path.read_bytes()
    assert isinstance(data, bytes)

    path.write_bytes(data)
    assert path.read_bytes() == data


def test_unlink(mock_swift, mock_swiftpath):

    mock_swift.put_container("test-container")
    mock_swift.put_object("test-container", "temp_key", contents=b"test data")

    path = mock_swiftpath("/test-container/temp_key")
    subdir_key = mock_swiftpath("/test-container/fake_folder/some_key")
    subdir_key.write_text("some text")
    assert path.exists() is True
    assert subdir_key.exists() is True
    path.unlink()
    assert path.exists() is False
    with pytest.raises(FileNotFoundError):
        mock_swiftpath("/test-container/fake_subfolder/fake_subkey").unlink()
    with pytest.raises(IsADirectoryError):
        mock_swiftpath("/test-container/fake_folder").unlink()
    with pytest.raises(FileNotFoundError):
        mock_swiftpath("/fake-bucket/").unlink()


def test_symlink(mock_swift, mock_swiftpath):

    mock_swift.put_container("test-container")
    mock_swift.put_object("test-container", "temp_key", contents=b"test data")

    path = mock_swiftpath("/test-container/temp_key")
    data = path.read_bytes()
    assert isinstance(data, bytes)

    path.write_bytes(data)
    assert path.read_bytes() == data
    new_path = mock_swiftpath("/test-container/new_key")
    new_path.symlink_to(path)
    assert new_path.is_symlink() is True
    assert new_path.read_bytes() == data
    assert not path.is_symlink()
    with pytest.raises(FileNotFoundError):
        assert not mock_swiftpath("/fake-bucket/fake-key").is_symlink()
