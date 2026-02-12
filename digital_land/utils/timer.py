"""
Phase timing decorator for performance monitoring.

Provides a reusable decorator to measure execution time, row count, and processing
rate for any phase's process() method. All timing information is logged automatically.

Usage:
    from digital_land.utils.timer import timer
    
    class MyPhase(Phase):
        @timer
        def process(self, stream):
            for block in stream:
                # your logic
                yield block

Logs:
    - Phase start message
    - Elapsed time (seconds)
    - Row count processed
    - Processing rate (rows/second)

Example output:
    INFO digital_land.phase.example:timer.py:11 Starting ExamplePhase phase...
    INFO digital_land.phase.example:timer.py:28 ExamplePhase phase completed in 0.05 seconds (1000 rows processed, 20000.00 rows/sec)
"""

import time
import logging
from functools import wraps
from typing import Generator, Dict, Any, Callable

logger = logging.getLogger(__name__)


def timer(func: Callable) -> Callable:
    """Decorator to measure phase process() method execution time.
    
    Args:
        func: The process() method to wrap (should be a generator yielding blocks)
        
    Returns:
        Wrapped function that logs timing metrics
        
    Attributes tracked:
        - Elapsed time in seconds (float, 2 decimal places)
        - Number of rows processed (integer)
        - Processing rate in rows/second (float, 2 decimal places)
    """
    @wraps(func)
    def wrapper(self, stream: Generator) -> Generator[Dict[str, Any], None, None]:
        """Wraps the process method with timing logic."""
        start_time = time.time()
        row_count = 0
        phase_name = self.__class__.__name__
        
        logger.info(f"Starting {phase_name} phase...")
        
        try:
            for block in func(self, stream):
                row_count += 1
                yield block
        finally:
            elapsed_time = time.time() - start_time
            # Prevent division by zero for very fast executions
            rows_per_sec = row_count / elapsed_time if elapsed_time > 0 else 0
            
            logger.info(
                f"{phase_name} phase completed in {elapsed_time:.2f} seconds "
                f"({row_count} rows processed, {rows_per_sec:.2f} rows/sec)"
            )
    
    return wrapper
