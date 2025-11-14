"""
Tests for retry handler module.
"""
import pytest
import time
from unittest.mock import Mock, patch
from src.retry_handler import RetryHandler, retry, CircuitBreaker


class TestRetryHandler:
    """Test cases for RetryHandler class."""
    
    def test_initialization(self):
        """Test RetryHandler initialization."""
        handler = RetryHandler(max_retries=5, base_delay=2)
        assert handler.max_retries == 5
        assert handler.base_delay == 2
    
    def test_successful_operation_first_try(self):
        """Test that successful operation on first try returns immediately."""
        handler = RetryHandler(max_retries=3, base_delay=1)
        
        def success_func():
            return "success"
        
        result = handler.retry_operation(success_func, "test_op")
        assert result == "success"
    
    def test_retry_on_failure(self):
        """Test that operations are retried on failure."""
        handler = RetryHandler(max_retries=3, base_delay=0.1)
        
        call_count = 0
        
        def eventually_succeeds():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise Exception(f"Attempt {call_count} failed")
            return "success"
        
        result = handler.retry_operation(eventually_succeeds, "test_op")
        assert result == "success"
        assert call_count == 3
    
    def test_all_retries_exhausted(self):
        """Test that exception is raised after all retries exhausted."""
        handler = RetryHandler(max_retries=3, base_delay=0.1)
        
        def always_fails():
            raise Exception("Always fails")
        
        with pytest.raises(Exception, match="Always fails"):
            handler.retry_operation(always_fails, "test_op")
    
    def test_exponential_backoff_delay(self):
        """Test that delay increases exponentially."""
        handler = RetryHandler(max_retries=3, base_delay=2)
        
        assert handler.get_delay(1) == 2   # 2 * 2^0
        assert handler.get_delay(2) == 4   # 2 * 2^1
        assert handler.get_delay(3) == 8   # 2 * 2^2
    
    def test_retry_decorator(self):
        """Test the retry decorator."""
        call_count = 0
        
        @retry(max_retries=3, base_delay=0.1)
        def decorated_func():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise Exception("Fail")
            return "success"
        
        result = decorated_func()
        assert result == "success"
        assert call_count == 2


class TestCircuitBreaker:
    """Test cases for CircuitBreaker class."""
    
    def test_initialization(self):
        """Test CircuitBreaker initialization."""
        breaker = CircuitBreaker(failure_threshold=5, timeout=60)
        assert breaker.failure_threshold == 5
        assert breaker.timeout == 60
        assert breaker.state == "CLOSED"
        assert breaker.failure_count == 0
    
    def test_successful_operation(self):
        """Test successful operation through circuit breaker."""
        breaker = CircuitBreaker(failure_threshold=3, timeout=10)
        
        def success_func():
            return "success"
        
        result = breaker.call(success_func)
        assert result == "success"
        assert breaker.state == "CLOSED"
        assert breaker.failure_count == 0
    
    def test_circuit_opens_after_threshold(self):
        """Test that circuit opens after reaching failure threshold."""
        breaker = CircuitBreaker(failure_threshold=3, timeout=10)
        
        def failing_func():
            raise Exception("Failure")
        
        # Fail 3 times to reach threshold
        for _ in range(3):
            try:
                breaker.call(failing_func)
            except Exception:
                pass
        
        assert breaker.state == "OPEN"
        assert breaker.failure_count == 3
    
    def test_circuit_blocks_when_open(self):
        """Test that circuit blocks calls when open."""
        breaker = CircuitBreaker(failure_threshold=2, timeout=10)
        
        def failing_func():
            raise Exception("Failure")
        
        # Open the circuit
        for _ in range(2):
            try:
                breaker.call(failing_func)
            except Exception:
                pass
        
        # Circuit should now be open and block calls
        with pytest.raises(Exception, match="Circuit breaker is OPEN"):
            breaker.call(failing_func)
    
    def test_circuit_resets_on_success(self):
        """Test that circuit resets failure count on success."""
        breaker = CircuitBreaker(failure_threshold=3, timeout=10)
        
        def failing_func():
            raise Exception("Failure")
        
        def success_func():
            return "success"
        
        # Fail once
        try:
            breaker.call(failing_func)
        except Exception:
            pass
        
        assert breaker.failure_count == 1
        
        # Succeed
        breaker.call(success_func)
        
        # Failure count should reset
        assert breaker.failure_count == 0
    
    def test_manual_reset(self):
        """Test manual circuit breaker reset."""
        breaker = CircuitBreaker(failure_threshold=2, timeout=10)
        
        def failing_func():
            raise Exception("Failure")
        
        # Open the circuit
        for _ in range(2):
            try:
                breaker.call(failing_func)
            except Exception:
                pass
        
        assert breaker.state == "OPEN"
        
        # Manual reset
        breaker.reset()
        
        assert breaker.state == "CLOSED"
        assert breaker.failure_count == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])