"""Tests for API retry mechanism."""

from unittest.mock import Mock, patch

import httpx
import pytest

from pydrime.api import DrimeClient
from pydrime.exceptions import (
    DrimeAPIError,
    DrimeAuthenticationError,
    DrimeNetworkError,
    DrimeNotFoundError,
    DrimePermissionError,
    DrimeRateLimitError,
)


class TestRetryMechanism:
    """Test retry mechanism for API requests."""

    def test_client_init_with_retry_params(self):
        """Test that client can be initialized with custom retry parameters."""
        client = DrimeClient(api_key="test_key", max_retries=5, retry_delay=2.0)
        assert client.max_retries == 5
        assert client.retry_delay == 2.0

    def test_client_init_default_retry_params(self):
        """Test that client uses default retry parameters."""
        client = DrimeClient(api_key="test_key")
        assert client.max_retries == 3
        assert client.retry_delay == 1.0

    @patch("pydrime.api.httpx.Client.request")
    def test_successful_request_no_retry(self, mock_request):
        """Test that successful requests don't trigger retries."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.content = b'{"status": "success"}'
        mock_response.json.return_value = {"status": "success"}
        mock_response.raise_for_status = Mock()
        mock_request.return_value = mock_response

        client = DrimeClient(api_key="test_key", max_retries=3)
        result = client._request("GET", "/test")

        assert result == {"status": "success"}
        assert mock_request.call_count == 1

    @patch("time.sleep", return_value=None)  # Skip actual sleep
    @patch("pydrime.api.httpx.Client.request")
    def test_retry_on_network_error(self, mock_request, mock_sleep):
        """Test that network errors are retried."""
        call_count = [0]

        def side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] < 3:
                raise httpx.ConnectError("Network error")
            # Success on third try
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.headers = {"Content-Type": "application/json"}
            mock_response.content = b'{"status": "success"}'
            mock_response.json.return_value = {"status": "success"}
            mock_response.raise_for_status = Mock()
            return mock_response

        mock_request.side_effect = side_effect

        client = DrimeClient(api_key="test_key", max_retries=3, retry_delay=0.1)
        result = client._request("GET", "/test")

        assert result == {"status": "success"}
        assert call_count[0] == 3  # Initial + 2 retries
        assert mock_sleep.call_count == 2  # Slept before each retry

    @patch("time.sleep", return_value=None)
    @patch("pydrime.api.httpx.Client.request")
    def test_retry_on_rate_limit_429(self, mock_request, mock_sleep):
        """Test that 429 rate limit errors are retried."""
        call_count = [0]

        def side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] < 2:
                # Rate limit error
                mock_response = Mock()
                mock_response.status_code = 429
                mock_response.headers = {}
                raise httpx.HTTPStatusError(
                    "Rate limit exceeded",
                    request=Mock(),
                    response=mock_response,
                )
            # Success on second try
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.headers = {"Content-Type": "application/json"}
            mock_response.content = b'{"status": "success"}'
            mock_response.json.return_value = {"status": "success"}
            mock_response.raise_for_status = Mock()
            return mock_response

        mock_request.side_effect = side_effect

        client = DrimeClient(api_key="test_key", max_retries=2, retry_delay=0.1)
        result = client._request("GET", "/test")

        assert result == {"status": "success"}
        assert call_count[0] == 2
        assert mock_sleep.call_count == 1

    @patch("time.sleep", return_value=None)
    @patch("pydrime.api.httpx.Client.request")
    def test_retry_with_retry_after_header(self, mock_request, mock_sleep):
        """Test that Retry-After header is respected for rate limits."""
        call_count = [0]

        def side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] < 2:
                # Rate limit error with Retry-After header
                mock_response = Mock()
                mock_response.status_code = 429
                mock_response.headers = {"Retry-After": "5"}
                raise httpx.HTTPStatusError(
                    "Rate limit exceeded",
                    request=Mock(),
                    response=mock_response,
                )
            # Success on second try
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.headers = {"Content-Type": "application/json"}
            mock_response.content = b'{"status": "success"}'
            mock_response.json.return_value = {"status": "success"}
            mock_response.raise_for_status = Mock()
            return mock_response

        mock_request.side_effect = side_effect

        client = DrimeClient(api_key="test_key", max_retries=2, retry_delay=0.1)
        result = client._request("GET", "/test")

        assert result == {"status": "success"}
        assert call_count[0] == 2
        # Should have slept for 5 seconds as per Retry-After header
        mock_sleep.assert_called_with(5.0)

    @patch("time.sleep", return_value=None)
    @patch("pydrime.api.httpx.Client.request")
    def test_retry_on_server_error_500(self, mock_request, mock_sleep):
        """Test that 500 server errors are retried."""
        call_count = [0]

        def side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] < 3:
                # Server error
                mock_response = Mock()
                mock_response.status_code = 500
                mock_response.headers = {}
                mock_response.content = b""
                raise httpx.HTTPStatusError(
                    "Server error",
                    request=Mock(),
                    response=mock_response,
                )
            # Success on third try
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.headers = {"Content-Type": "application/json"}
            mock_response.content = b'{"status": "success"}'
            mock_response.json.return_value = {"status": "success"}
            mock_response.raise_for_status = Mock()
            return mock_response

        mock_request.side_effect = side_effect

        client = DrimeClient(api_key="test_key", max_retries=3, retry_delay=0.1)
        result = client._request("GET", "/test")

        assert result == {"status": "success"}
        assert call_count[0] == 3
        assert mock_sleep.call_count == 2

    @patch("time.sleep", return_value=None)
    @patch("pydrime.api.httpx.Client.request")
    def test_retry_on_server_error_503(self, mock_request, mock_sleep):
        """Test that 503 service unavailable errors are retried."""
        call_count = [0]

        def side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] < 2:
                # Service unavailable
                mock_response = Mock()
                mock_response.status_code = 503
                mock_response.headers = {}
                mock_response.content = b""
                raise httpx.HTTPStatusError(
                    "Service unavailable",
                    request=Mock(),
                    response=mock_response,
                )
            # Success on second try
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.headers = {"Content-Type": "application/json"}
            mock_response.content = b'{"status": "success"}'
            mock_response.json.return_value = {"status": "success"}
            mock_response.raise_for_status = Mock()
            return mock_response

        mock_request.side_effect = side_effect

        client = DrimeClient(api_key="test_key", max_retries=2, retry_delay=0.1)
        result = client._request("GET", "/test")

        assert result == {"status": "success"}
        assert call_count[0] == 2

    @patch("time.sleep", return_value=None)
    @patch("pydrime.api.httpx.Client.request")
    def test_retry_exhausted_on_network_error(self, mock_request, mock_sleep):
        """Test that retries are exhausted and error is raised."""
        mock_request.side_effect = httpx.ConnectError("Network error")

        client = DrimeClient(api_key="test_key", max_retries=2, retry_delay=0.1)

        with pytest.raises(DrimeNetworkError, match="Network error"):
            client._request("GET", "/test")

        # max_retries=2 means 3 attempts total (initial + 2 retries)
        assert mock_request.call_count == 3
        assert mock_sleep.call_count == 2

    @patch("time.sleep", return_value=None)
    @patch("pydrime.api.httpx.Client.request")
    def test_retry_exhausted_on_rate_limit(self, mock_request, mock_sleep):
        """Test that rate limit errors raise after retries exhausted."""

        def side_effect(*args, **kwargs):
            mock_response = Mock()
            mock_response.status_code = 429
            mock_response.headers = {}
            raise httpx.HTTPStatusError(
                "Rate limit exceeded",
                request=Mock(),
                response=mock_response,
            )

        mock_request.side_effect = side_effect

        client = DrimeClient(api_key="test_key", max_retries=2, retry_delay=0.1)

        with pytest.raises(DrimeRateLimitError, match="Rate limit exceeded"):
            client._request("GET", "/test")

        # max_retries=2 means 3 attempts total
        assert mock_request.call_count == 3
        assert mock_sleep.call_count == 2

    @patch("pydrime.api.httpx.Client.request")
    def test_no_retry_on_401_auth_error(self, mock_request):
        """Test that 401 authentication errors are not retried."""
        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Unauthorized",
            request=Mock(),
            response=mock_response,
        )
        mock_request.return_value = mock_response

        client = DrimeClient(api_key="test_key", max_retries=3)

        with pytest.raises(DrimeAuthenticationError, match="Invalid API key"):
            client._request("GET", "/test")

        # Should not retry auth errors
        assert mock_request.call_count == 1

    @patch("pydrime.api.httpx.Client.request")
    def test_no_retry_on_403_permission_error(self, mock_request):
        """Test that 403 permission errors are not retried."""
        mock_response = Mock()
        mock_response.status_code = 403
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Forbidden",
            request=Mock(),
            response=mock_response,
        )
        mock_request.return_value = mock_response

        client = DrimeClient(api_key="test_key", max_retries=3)

        with pytest.raises(DrimePermissionError, match="Access forbidden"):
            client._request("GET", "/test")

        assert mock_request.call_count == 1

    @patch("pydrime.api.httpx.Client.request")
    def test_no_retry_on_404_not_found_error(self, mock_request):
        """Test that 404 not found errors are not retried."""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Not found",
            request=Mock(),
            response=mock_response,
        )
        mock_request.return_value = mock_response

        client = DrimeClient(api_key="test_key", max_retries=3)

        with pytest.raises(DrimeNotFoundError, match="Resource not found"):
            client._request("GET", "/test")

        assert mock_request.call_count == 1

    @patch("time.sleep", return_value=None)
    @patch("pydrime.api.httpx.Client.request")
    def test_exponential_backoff(self, mock_request, mock_sleep):
        """Test that retry delays increase exponentially."""
        mock_request.side_effect = httpx.ConnectError("Network error")

        client = DrimeClient(api_key="test_key", max_retries=3, retry_delay=1.0)

        with pytest.raises(DrimeNetworkError):
            client._request("GET", "/test")

        # Check that sleep was called with increasing delays
        assert mock_sleep.call_count == 3
        # First retry: ~1s, second: ~2s, third: ~4s (with jitter)
        # We just verify that delays are increasing
        calls = [call[0][0] for call in mock_sleep.call_args_list]
        assert calls[0] < calls[1] < calls[2]

    @patch("time.sleep", return_value=None)
    @patch("pydrime.api.httpx.Client.request")
    def test_retry_on_timeout_error(self, mock_request, mock_sleep):
        """Test that timeout errors are retried."""
        call_count = [0]

        def side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] < 2:
                raise httpx.TimeoutException("Request timed out")
            # Success on second try
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.headers = {"Content-Type": "application/json"}
            mock_response.content = b'{"status": "success"}'
            mock_response.json.return_value = {"status": "success"}
            mock_response.raise_for_status = Mock()
            return mock_response

        mock_request.side_effect = side_effect

        client = DrimeClient(api_key="test_key", max_retries=2, retry_delay=0.1)
        result = client._request("GET", "/test")

        assert result == {"status": "success"}
        assert call_count[0] == 2

    @patch("time.sleep", return_value=None)
    @patch("pydrime.api.httpx.Client.request")
    def test_no_retry_on_400_client_error(self, mock_request, mock_sleep):
        """Test that 400 client errors are not retried."""
        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.headers = {}
        mock_response.content = b'{"message": "Bad request"}'
        mock_response.json.return_value = {"message": "Bad request"}
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Bad request",
            request=Mock(),
            response=mock_response,
        )
        mock_request.return_value = mock_response

        client = DrimeClient(api_key="test_key", max_retries=3)

        with pytest.raises(DrimeAPIError, match="API request failed with status 400"):
            client._request("GET", "/test")

        # Should not retry 4xx errors (except 429)
        assert mock_request.call_count == 1
        assert mock_sleep.call_count == 0

    def test_should_retry_method(self):
        """Test the _should_retry helper method."""
        client = DrimeClient(api_key="test_key", max_retries=3)

        # Should retry network errors
        assert client._should_retry(DrimeNetworkError("test"), 0) is True
        assert client._should_retry(DrimeNetworkError("test"), 2) is True
        # Should not retry after max attempts
        assert client._should_retry(DrimeNetworkError("test"), 3) is False

        # Should retry rate limit errors
        assert client._should_retry(DrimeRateLimitError("test"), 0) is True
        assert client._should_retry(DrimeRateLimitError("test"), 2) is True
        assert client._should_retry(DrimeRateLimitError("test"), 3) is False

        # Should not retry auth errors
        assert client._should_retry(DrimeAuthenticationError("test"), 0) is False

    def test_calculate_retry_delay(self):
        """Test the _calculate_retry_delay method."""
        client = DrimeClient(api_key="test_key", max_retries=3, retry_delay=1.0)

        # Test exponential backoff
        delay_0 = client._calculate_retry_delay(0)
        delay_1 = client._calculate_retry_delay(1)
        delay_2 = client._calculate_retry_delay(2)

        # Delays should roughly double each time (with jitter)
        assert 0.75 <= delay_0 <= 1.25  # ~1s with 25% jitter
        assert 1.5 <= delay_1 <= 2.5  # ~2s with 25% jitter
        assert 3.0 <= delay_2 <= 5.0  # ~4s with 25% jitter

    @patch("time.sleep", return_value=None)
    @patch("pydrime.api.httpx.Client.request")
    def test_retry_preserves_last_exception(self, mock_request, mock_sleep):
        """Test that the last exception is preserved after retries."""

        def side_effect(*args, **kwargs):
            raise httpx.ConnectError("Network error")

        mock_request.side_effect = side_effect

        client = DrimeClient(api_key="test_key", max_retries=2, retry_delay=0.1)

        with pytest.raises(DrimeNetworkError) as exc_info:
            client._request("GET", "/test")

        # Verify the exception message is preserved
        assert "Network error" in str(exc_info.value)
