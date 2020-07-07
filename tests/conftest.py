import contextlib
from unittest import mock

import pytest

import swiftpath.swiftpath


@pytest.fixture(scope="function")
def mock_swiftpath(mock_swift):
    def _get_connection(o):
        return mock_swift

    @contextlib.contextmanager
    def connection(o):
        yield o._get_connection()

    stack = contextlib.ExitStack()
    stack.enter_context(
        mock.patch.object(
            swiftpath.swiftpath._Backend, "_get_connection", _get_connection
        )
    )
    stack.enter_context(
        mock.patch.object(swiftpath.swiftpath._Backend, "connection", connection)
    )
    stack.enter_context(
        mock.patch.object(
            swiftpath.swiftpath._SwiftAccessor,
            "Backend",
            swiftpath.swiftpath._Backend(),
        )
    )
    stack.enter_context(
        mock.patch(
            "swiftpath.swiftpath._swift_accessor", swiftpath.swiftpath._SwiftAccessor()
        )
    )

    def _init(self, template=None):
        super(type(self), self)._init(template)
        if template is None:
            self._accessor = swiftpath.swiftpath._swift_accessor

    stack.enter_context(
        mock.patch.object(swiftpath.swiftpath.SwiftPath, "_init", _init)
    )
    yield swiftpath.swiftpath.SwiftPath
    stack.close()
