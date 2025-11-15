"""
Retry handler module with configurable retry logic.
Implements exponential backoff for failed operations.
"""
import logging
import time
from typing import Callable, Any, Optional
from functools import wraps

from src.config import MAX_RETRIES, RETRY_DELAY

# Setup logger
logger = logging.getLogger(__name__)


class RetryHandler:
    """Handler for retrying failed operations with exponential backoff."""

    def __init__(self, max_retries: int = MAX_RETRIES, base_delay: int = RETRY_DELAY):
        """
        Initialize retry handler.

        Args:
            max_retries: Maximum number of retry attempts
            base_delay: Base delay in seconds between retries
        """
        self.max_retries = max_retries
        self.base_delay = base_delay
        logger.info(
            f"RetryHandler initialized with max_retries={max_retries}, base_delay={base_delay}s"
        )

    def retry_operation(
        self, operation: Callable, operation_name: str, *args, **kwargs
    ) -> Any:
        """
        Execute an operation with retry logic.

        Args:
            operation: Function to execute
            operation_name: Name of the operation for logging
            *args: Positional arguments to pass to operation
            **kwargs: Keyword arguments to pass to operation

        Returns:
            Result of the operation

        Raises:
            Exception: If all retry attempts fail
        """
        last_exception = None

        for attempt in range(1, self.max_retries + 1):
            try:
                logger.info(
                    f"Attempting {operation_name} (attempt {attempt}/{self.max_retries})"
                )
                result = operation(*args, **kwargs)
                logger.info(f"{operation_name} succeeded on attempt {attempt}")
                return result

            except Exception as e:
                last_exception = e
                logger.warning(
                    f"{operation_name} failed on attempt {attempt}: {str(e)}"
                )

                if attempt < self.max_retries:
                    # Calculate delay with exponential backoff
                    delay = self.base_delay * (2 ** (attempt - 1))
                    logger.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    logger.error(
                        f"{operation_name} failed after {self.max_retries} attempts"
                    )

        # All retries exhausted
        raise last_exception

    def get_delay(self, attempt: int) -> int:
        """
        Calculate delay for a given attempt using exponential backoff.

        Args:
            attempt: Current attempt number (1-indexed)

        Returns:
            Delay in seconds
        """
        return self.base_delay * (2 ** (attempt - 1))


def retry(max_retries: int = MAX_RETRIES, base_delay: int = RETRY_DELAY):
    """
    Decorator for automatic retry with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts
        base_delay: Base delay in seconds between retries

    Example:
        @retry(max_retries=3, base_delay=5)
        def my_function():
            # Function that might fail
            pass
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            handler = RetryHandler(max_retries=max_retries, base_delay=base_delay)
            return handler.retry_operation(func, func.__name__, *args, **kwargs)

        return wrapper

    return decorator


class CircuitBreaker:
    """
    Circuit breaker pattern to prevent cascading failures.
    Opens circuit after consecutive failures, preventing further attempts.
    """

    def __init__(self, failure_threshold: int = 5, timeout: int = 60):
        """
        Initialize circuit breaker.

        Args:
            failure_threshold: Number of consecutive failures before opening circuit
            timeout: Seconds to wait before attempting to close circuit
        """
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.failure_count = 0
        self.last_failure_time: Optional[float] = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
        logger.info(
            f"CircuitBreaker initialized with threshold={failure_threshold}, timeout={timeout}s"
        )

    def call(self, operation: Callable, *args, **kwargs) -> Any:
        """
        Execute operation through circuit breaker.

        Args:
            operation: Function to execute
            *args: Positional arguments
            **kwargs: Keyword arguments

        Returns:
            Result of operation

        Raises:
            Exception: If circuit is open or operation fails
        """
        if self.state == "OPEN":
            if time.time() - self.last_failure_time >= self.timeout:
                logger.info("Circuit breaker entering HALF_OPEN state")
                self.state = "HALF_OPEN"
            else:
                raise Exception(
                    f"Circuit breaker is OPEN. Wait {self.timeout}s before retry"
                )

        try:
            result = operation(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise e

    def _on_success(self):
        """Handle successful operation."""
        self.failure_count = 0
        if self.state == "HALF_OPEN":
            logger.info("Circuit breaker closing after successful operation")
            self.state = "CLOSED"

    def _on_failure(self):
        """Handle failed operation."""
        self.failure_count += 1
        self.last_failure_time = time.time()

        if self.failure_count >= self.failure_threshold:
            logger.error(f"Circuit breaker opening after {self.failure_count} failures")
            self.state = "OPEN"

    def reset(self):
        """Manually reset circuit breaker."""
        logger.info("Circuit breaker manually reset")
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"


if __name__ == "__main__":
    # Test retry handler
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Test 1: Successful operation on first try
    def successful_operation():
        logger.info("Executing successful operation")
        return "Success!"

    handler = RetryHandler(max_retries=3, base_delay=1)
    result = handler.retry_operation(successful_operation, "Test Operation 1")
    logger.info(f"Test 1 Result: {result}")

    # Test 2: Operation that fails twice then succeeds
    attempt_count = 0

    def eventually_successful_operation():
        global attempt_count  # Changed from 'nonlocal' to 'global'
        attempt_count += 1
        if attempt_count < 3:
            raise Exception(f"Simulated failure on attempt {attempt_count}")
        return "Success after retries!"

    attempt_count = 0
    result = handler.retry_operation(
        eventually_successful_operation, "Test Operation 2"
    )
    logger.info(f"Test 2 Result: {result}")

    # Test 3: Using decorator
    @retry(max_retries=2, base_delay=1)
    def decorated_function():
        logger.info("Executing decorated function")
        return "Decorated success!"

    result = decorated_function()
    logger.info(f"Test 3 Result: {result}")

    logger.info("All retry handler tests passed")
