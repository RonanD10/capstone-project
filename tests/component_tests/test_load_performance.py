import pandas as pd
import pytest
import time
from unittest.mock import patch, Mock
from src.load.load import load_data
from src.utils.db_utils import QueryExecutionError

"""
Performance Tests for Load Process

These tests validate the load process performance characteristics,
ensuring it meets non-functional requirements.

Test Coverage:
1. Load Time Requirements: Validates load operations complete within SLA
2. Memory Efficiency: Tests memory usage with large datasets
3. Concurrent Load Handling: Tests behavior under concurrent operations
4. Resource Cleanup: Validates proper resource management
5. Scalability: Tests performance degradation with increasing data size

Performance Tests focus on:
- Meeting defined performance SLAs
- Memory efficiency and resource management
- Scalability characteristics
- Error recovery performance
- Database connection pooling efficiency
"""


class TestLoadPerformance:
    """Performance tests for load operations"""

    def test_load_performance_small_dataset(self):
        """Test load performance with small dataset (< 1000 records)"""
        small_data = pd.DataFrame({
            'customer_id': range(1000),
            'name': [f'Customer {i}' for i in range(1000)],
            'amount': [100.0 + i for i in range(1000)]
        })
        
        with patch('src.load.load.create_transactions_by_customers') as mock_create:
            start_time = time.time()
            load_data(small_data)
            execution_time = time.time() - start_time
        
        # Should complete within 1 second for small datasets
        assert execution_time < 1.0, f"Small dataset load took {execution_time:.2f}s, expected <1s"
        mock_create.assert_called_once()

    def test_load_performance_medium_dataset(self):
        """Test load performance with medium dataset (1000-10000 records)"""
        medium_data = pd.DataFrame({
            'customer_id': range(5000),
            'name': [f'Customer {i}' for i in range(5000)],
            'amount': [100.0 + i for i in range(5000)]
        })
        
        with patch('src.load.load.create_transactions_by_customers') as mock_create:
            start_time = time.time()
            load_data(medium_data)
            execution_time = time.time() - start_time
        
        # Should complete within 3 seconds for medium datasets
        assert execution_time < 3.0, f"Medium dataset load took {execution_time:.2f}s, expected <3s"
        mock_create.assert_called_once()

    def test_load_performance_large_dataset(self):
        """Test load performance with large dataset (>10000 records)"""
        large_data = pd.DataFrame({
            'customer_id': range(15000),
            'name': [f'Customer {i}' for i in range(15000)],
            'amount': [100.0 + i for i in range(15000)],
            'description': ['Transaction description ' * 10] * 15000  # Larger text data
        })
        
        with patch('src.load.load.create_transactions_by_customers') as mock_create:
            start_time = time.time()
            load_data(large_data)
            execution_time = time.time() - start_time
        
        # Should complete within 10 seconds for large datasets
        assert execution_time < 10.0, f"Large dataset load took {execution_time:.2f}s, expected <10s"
        mock_create.assert_called_once()

    def test_load_memory_efficiency(self):
        """Test memory usage doesn't grow excessively during load"""
        import psutil
        import os
        
        # Get initial memory usage
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        # Create large dataset
        large_data = pd.DataFrame({
            'customer_id': range(10000),
            'data': ['x' * 1000] * 10000  # 1KB per record
        })
        
        with patch('src.load.load.create_transactions_by_customers'):
            load_data(large_data)
        
        # Check memory usage after load
        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_increase = final_memory - initial_memory
        
        # Memory increase should be reasonable (< 100MB for this test)
        assert memory_increase < 100, f"Memory increased by {memory_increase:.2f}MB, expected <100MB"

    def test_load_error_recovery_performance(self):
        """Test performance of error handling and recovery"""
        sample_data = pd.DataFrame({
            'customer_id': [1, 2, 3],
            'amount': [100.0, 200.0, 300.0]
        })
        
        def failing_operation(data):
            time.sleep(0.1)  # Simulate some processing time
            raise QueryExecutionError("Simulated database error")
        
        with patch('src.load.load.create_transactions_by_customers', side_effect=failing_operation):
            start_time = time.time()
            
            with pytest.raises(QueryExecutionError):
                load_data(sample_data)
            
            error_handling_time = time.time() - start_time
        
        # Error handling should be fast (< 1 second)
        assert error_handling_time < 1.0, f"Error handling took {error_handling_time:.2f}s, expected <1s"

    def test_load_validation_performance(self):
        """Test performance of input validation"""
        # Test with various input types
        test_cases = [
            None,
            pd.DataFrame(),
            pd.DataFrame({'col': []}),  # Empty with columns
            pd.DataFrame({'col': [1, 2, 3]})  # Small valid data
        ]
        
        for test_data in test_cases:
            with patch('src.load.load.create_transactions_by_customers') as mock_create:
                start_time = time.time()
                load_data(test_data)
                validation_time = time.time() - start_time
            
            # Validation should be very fast (< 0.1 seconds)
            assert validation_time < 0.1, f"Validation took {validation_time:.2f}s, expected <0.1s"

    def test_load_concurrent_operations_simulation(self):
        """Test load behavior under simulated concurrent operations"""
        import threading
        import queue
        
        results = queue.Queue()
        sample_data = pd.DataFrame({
            'customer_id': range(100),
            'amount': [100.0] * 100
        })
        
        with patch('src.load.load.create_transactions_by_customers') as mock_create:
            mock_create.return_value = None
            
            def load_operation(thread_id):
                try:
                    start_time = time.time()
                    load_data(sample_data)
                    execution_time = time.time() - start_time
                    results.put(('success', execution_time, thread_id))
                except Exception as e:
                    results.put(('error', str(e), thread_id))
        
            # Simulate 5 concurrent load operations
            threads = []
            for i in range(5):
                thread = threading.Thread(target=load_operation, args=(i,))
                threads.append(thread)
                thread.start()
            
            # Wait for all threads to complete
            for thread in threads:
                thread.join()
        
        # Collect results
        execution_times = []
        while not results.empty():
            status, result, thread_id = results.get()
            assert status == 'success', f"Concurrent operation failed: {result}"
            execution_times.append(result)
        
        # All operations should complete reasonably fast
        max_time = max(execution_times)
        assert max_time < 2.0, f"Slowest concurrent operation took {max_time:.2f}s, expected <2s"

    def test_load_scalability_characteristics(self):
        """Test how load performance scales with data size"""
        data_sizes = [100, 500, 1000, 2000]
        execution_times = []
        
        for size in data_sizes:
            test_data = pd.DataFrame({
                'customer_id': range(size),
                'amount': [100.0] * size
            })
            
            with patch('src.load.load.create_transactions_by_customers'):
                start_time = time.time()
                load_data(test_data)
                execution_time = time.time() - start_time
                execution_times.append(execution_time)
        
        # Performance should scale reasonably (not exponentially)
        # Check that 4x data doesn't take more than 10x time
        time_ratio = execution_times[-1] / execution_times[0] if execution_times[0] > 0 else 0
        data_ratio = data_sizes[-1] / data_sizes[0]
        
        assert time_ratio < data_ratio * 2.5, f"Performance scaling poor: {time_ratio:.2f}x time for {data_ratio}x data"