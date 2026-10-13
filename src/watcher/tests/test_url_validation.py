import pytest

from watcher.config import WatcherConfig


@pytest.mark.parametrize("url", ["http://127.0.0.1:8723", "https://telemetry.internal/", "http://[::1]:8723"])
def test_accepts_http_and_https(url):
    assert WatcherConfig(api_base=url).api_base == url


@pytest.mark.parametrize("url", ["file:///etc/passwd", "127.0.0.1:8723", "ftp://host/x", "http://"])
def test_rejects_non_http_urls(url):
    with pytest.raises(ValueError, match="http"):
        WatcherConfig(api_base=url)
