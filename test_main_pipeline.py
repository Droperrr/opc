"""
Test Suite for Main Pipeline
Tests the complete BANT system functionality
"""

import unittest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import tempfile
import os
import sqlite3

# Import BANT modules
from main_pipeline import MainPipeline
from prediction_layer import PredictionLayer
from error_monitor import ErrorMonitor
from block_detector import BlockDetector
from block_analyzer import BlockAnalyzer

class TestMainPipeline(unittest.TestCase):
    """Test cases for Main Pipeline"""
    
    def setUp(self):
        """Set up test environment"""
        # Create temporary database for testing
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        self.db_path = self.temp_db.name
        
        # Initialize pipeline with test database
        self.pipeline = MainPipeline(self.db_path)
        
        # Sample test data
        self.sample_prices = [100.0, 105.0, 95.0, 110.0, 90.0, 115.0, 85.0, 120.0]
        self.sample_volatility = 0.2
    
    def tearDown(self):
        """Clean up test environment"""
        # Remove temporary database
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
    
    def test_pipeline_initialization(self):
        """Test pipeline initialization"""
        self.assertIsNotNone(self.pipeline.prediction_layer)
        self.assertIsNotNone(self.pipeline.error_monitor)
        self.assertIsNotNone(self.pipeline.block_detector)
        self.assertIsNotNone(self.pipeline.block_analyzer)
        self.assertIsNotNone(self.pipeline.formula_engine)
        self.assertIsNotNone(self.pipeline.block_reporter)
    
    def test_prediction_cycle(self):
        """Test prediction cycle functionality"""
        result = self.pipeline.run_prediction_cycle(
            self.sample_prices, 
            self.sample_volatility
        )
        
        self.assertIsInstance(result, dict)
        self.assertIn('prediction', result)
        self.assertIn('actual', result)
        self.assertIn('confidence', result)
        self.assertIn('error', result)
        self.assertIn('timestamp', result)
        
        # Check that prediction is a valid number
        self.assertIsInstance(result['prediction'], (int, float))
        self.assertGreater(result['prediction'], 0)
    
    def test_error_monitoring(self):
        """Test error monitoring functionality"""
        # Run prediction cycle to generate error data
        self.pipeline.run_prediction_cycle(self.sample_prices, self.sample_volatility)
        
        # Check error history
        errors = self.pipeline.error_monitor.get_errors()
        self.assertIsInstance(errors, pd.DataFrame)
        
        # Check error statistics
        stats = self.pipeline.error_monitor.calculate_error_statistics()
        self.assertIsInstance(stats, dict)
    
    def test_block_detection(self):
        """Test block detection functionality"""
        # First, generate some error data
        for i in range(10):
            self.pipeline.run_prediction_cycle(self.sample_prices, self.sample_volatility)
        
        # Detect blocks
        blocks = self.pipeline.detect_blocks()
        self.assertIsInstance(blocks, list)
    
    def test_block_analysis(self):
        """Test block analysis functionality"""
        # Generate error data
        for i in range(10):
            self.pipeline.run_prediction_cycle(self.sample_prices, self.sample_volatility)
        
        # Analyze blocks
        analysis = self.pipeline.analyze_blocks()
        self.assertIsInstance(analysis, dict)
    
    def test_system_status(self):
        """Test system status functionality"""
        status = self.pipeline.get_system_status()
        
        self.assertIsInstance(status, dict)
        self.assertIn('timestamp', status)
        self.assertIn('database', status)
        self.assertIn('components', status)
        
        # Check components status
        components = status['components']
        self.assertIn('prediction_layer', components)
        self.assertIn('error_monitor', components)
        self.assertIn('block_detector', components)
        self.assertIn('block_analyzer', components)
        self.assertIn('formula_engine', components)
        self.assertIn('block_reporter', components)
    
    def test_full_cycle(self):
        """Test complete system cycle"""
        result = self.pipeline.run_full_cycle(
            self.sample_prices, 
            self.sample_volatility
        )
        
        self.assertIsInstance(result, dict)
        self.assertIn('prediction', result)
        self.assertIn('blocks', result)
        self.assertIn('analysis', result)
        self.assertIn('report', result)
        self.assertIn('timestamp', result)
    
    def test_database_integration(self):
        """Test database integration"""
        # Check that database file exists
        self.assertTrue(os.path.exists(self.db_path))
        
        # Check that error_history table exists
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='error_history'
        """)
        
        table_exists = cursor.fetchone() is not None
        conn.close()
        
        self.assertTrue(table_exists)
    
    def test_error_handling(self):
        """Test error handling with invalid inputs"""
        # Test with empty prices
        result = self.pipeline.run_prediction_cycle([], self.sample_volatility)
        self.assertIsInstance(result, dict)
        
        # Test with invalid volatility
        result = self.pipeline.run_prediction_cycle(self.sample_prices, -1.0)
        self.assertIsInstance(result, dict)
    
    def test_performance(self):
        """Test system performance with multiple cycles"""
        start_time = datetime.now()
        
        # Run multiple cycles
        for i in range(50):
            self.pipeline.run_prediction_cycle(self.sample_prices, self.sample_volatility)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # Should complete 50 cycles in reasonable time
        self.assertLess(duration, 10.0)  # Less than 10 seconds
        
        # Check that errors were recorded
        errors = self.pipeline.error_monitor.get_errors()
        self.assertGreaterEqual(len(errors), 50)

class TestIndividualComponents(unittest.TestCase):
    """Test individual components"""
    
    def setUp(self):
        """Set up test environment"""
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        self.db_path = self.temp_db.name
    
    def tearDown(self):
        """Clean up test environment"""
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
    
    def test_prediction_layer(self):
        """Test PredictionLayer component"""
        prediction_layer = PredictionLayer()
        
        # Test prediction
        prediction, confidence = prediction_layer.predict_next_price([100, 105, 95, 110])
        
        self.assertIsInstance(prediction, (int, float))
        self.assertIsInstance(confidence, (int, float))
        self.assertGreater(prediction, 0)
        self.assertGreaterEqual(confidence, 0)
        self.assertLessEqual(confidence, 1)
    
    def test_error_monitor(self):
        """Test ErrorMonitor component"""
        error_monitor = ErrorMonitor(self.db_path)
        
        # Test error recording
        error_monitor.update(
            timestamp=datetime.now(),
            predicted=100.0,
            actual=105.0,
            volatility=0.1,
            formula_id='TEST',
            confidence=0.8,
            method='test'
        )
        
        # Test error retrieval
        errors = error_monitor.get_errors()
        self.assertIsInstance(errors, pd.DataFrame)
        
        # Test statistics
        stats = error_monitor.calculate_error_statistics()
        self.assertIsInstance(stats, dict)
    
    def test_block_detector(self):
        """Test BlockDetector component"""
        block_detector = BlockDetector(self.db_path)
        
        # Test block detection (will be empty initially)
        blocks = block_detector.detect_block_boundaries()
        self.assertIsInstance(blocks, list)
    
    def test_block_analyzer(self):
        """Test BlockAnalyzer component"""
        block_analyzer = BlockAnalyzer(self.db_path)
        
        # Test block retrieval (will be empty initially)
        blocks = block_analyzer.get_blocks()
        self.assertIsInstance(blocks, pd.DataFrame)
        
        # Test statistics
        stats = block_analyzer.get_block_statistics()
        self.assertIsInstance(stats, dict)

def run_tests():
    """Run all tests"""
    # Create test suite
    suite = unittest.TestSuite()
    
    # Add test cases
    suite.addTest(unittest.makeSuite(TestMainPipeline))
    suite.addTest(unittest.makeSuite(TestIndividualComponents))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    return result.wasSuccessful()

if __name__ == "__main__":
    success = run_tests()
    if success:
        print("✅ All tests passed!")
    else:
        print("❌ Some tests failed!")
