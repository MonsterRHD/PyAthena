from typing import Any

import pytest
from botocore.exceptions import ClientError

from pyathena import DataError
from pyathena.util import RetryConfig, parse_output_location, retry_api_call, strtobool


def test_parse_output_location():
    # valid
    actual = parse_output_location("s3://bucket/path/to")
    assert actual[0] == "bucket"
    assert actual[1] == "path/to"

    # invalid
    with pytest.raises(DataError):
        parse_output_location("http://foobar")


def test_strtobool():
    yes = ("y", "Y", "yes", "True", "t", "true", "True", "On", "on", "1")
    no = ("n", "no", "f", "false", "off", "0", "Off", "No", "N")

    for y in yes:
        assert strtobool(y)

    for n in no:
        assert not strtobool(n)


class _WithCodeError(Exception):
    def __init__(self, code: int) -> None:
        super().__init__(f"error:{code}")
        self.response = {"Error": {"Code": code}}


class _NoResponseError(Exception):
    def __init__(self) -> None:
        super().__init__("error")
        self.response = None


def _test_retry(ex: Exception) -> None:
    calls = {"n": 0}

    def fn() -> Any:
        calls["n"] += 1
        raise ex

    cfg = RetryConfig(attempt=1, max_delay=1)

    with pytest.raises(type(ex)):
        retry_api_call(fn, config=cfg)

    assert calls["n"] == 1


def test_retry_api_call():
    _test_retry(_WithCodeError(500))


def test_retry_api_call_with_none_error():
    _test_retry(_NoResponseError())


@pytest.mark.parametrize(
    ("code", "message", "exceptions", "expected_calls"),
    [
        ("ThrottlingException", "Rate exceeded", ("ThrottlingException",), 2),
        (
            "MetadataException",
            "Rate exceeded (Service: AmazonDataCatalog; Status Code: 400; "
            "Error Code: ThrottlingException; Request ID: example; Proxy: null)",
            ("ThrottlingException",),
            2,
        ),
        (
            "MetadataException",
            "Not authorized (Service: AmazonDataCatalog; Status Code: 400; "
            "Error Code: AccessDeniedException; Request ID: example; Proxy: null)",
            ("ThrottlingException",),
            1,
        ),
        ("MetadataException", "Table ThrottlingException not found", ("ThrottlingException",), 1),
        ("MetadataException", "Rate exceeded", ("ThrottlingException",), 1),
        (
            "MetadataException",
            "Rate exceeded (Service: AmazonDataCatalog; Status Code: 400; "
            "Error Code: ThrottlingException; Request ID: example; Proxy: null)",
            (),
            1,
        ),
        ("MetadataException", "Custom error", ("MetadataException",), 2),
        (
            "MetadataException",
            "Too many requests (Service: AmazonDataCatalog; Status Code: 400; "
            "Error Code: TooManyRequestsException; Request ID: example; Proxy: null)",
            ("TooManyRequestsException",),
            2,
        ),
        (
            "MetadataException",
            "Table '(Service: AmazonDataCatalog; Status Code: 400; "
            "Error Code: ThrottlingException; Request ID: fake; Proxy: null)' not found "
            "(Service: AmazonDataCatalog; Status Code: 400; "
            "Error Code: EntityNotFoundException; Request ID: actual; Proxy: null)",
            ("ThrottlingException",),
            1,
        ),
    ],
)
def test_retry_metadata_errors(code, message, exceptions, expected_calls):
    error = ClientError({"Error": {"Code": code, "Message": message}}, "GetTableMetadata")
    calls = 0

    def call():
        nonlocal calls
        calls += 1
        if calls == 1:
            raise error
        return "success"

    config = RetryConfig(exceptions=exceptions, attempt=2, multiplier=0, max_delay=0)
    if expected_calls == 1:
        with pytest.raises(ClientError) as caught:
            retry_api_call(call, config)
        assert caught.value is error
    else:
        assert retry_api_call(call, config) == "success"
    assert calls == expected_calls


@pytest.mark.parametrize("wrapped", [False, True])
def test_retry_api_call_with_single_pass_exceptions(wrapped):
    error = ClientError(
        {
            "Error": {
                "Code": "MetadataException" if wrapped else "ThrottlingException",
                "Message": "Rate exceeded (Service: AmazonDataCatalog; Status Code: 400; "
                "Error Code: ThrottlingException; Request ID: example; Proxy: null)",
            }
        },
        "GetTableMetadata",
    )
    config = RetryConfig(
        exceptions=iter(("ThrottlingException",)), attempt=3, multiplier=0, max_delay=0
    )
    for _ in range(2):
        calls = 0

        def call():
            nonlocal calls
            calls += 1
            if calls < 3:
                raise error
            return "success"

        assert retry_api_call(call, config) == "success"
        assert calls == 3
