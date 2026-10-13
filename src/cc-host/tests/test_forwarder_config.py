import pytest

from cc_host.forwarder import ForwarderConfig


@pytest.mark.parametrize(
    "url",
    ["http://127.0.0.1:8723/ingest", "https://telemetry.example.internal/ingest", "http://[::1]:8723/ingest/"],
)
def test_accepts_http_and_https_ingest_urls(url):
    assert ForwarderConfig(ingest_url=url, ingest_token="t").ingest_url == url


@pytest.mark.parametrize("url", ["file:///tmp/x", "ingest", "gopher://h/"])
def test_rejects_non_http_ingest_url(url):
    with pytest.raises(ValueError, match="http"):
        ForwarderConfig(ingest_url=url, ingest_token="t")
