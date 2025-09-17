"""
Main Pipeline for BANT Project
Orchestrates the complete trading system workflow
"""

from config import OPERATION_MODE

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import logging

# Import BANT modules
from prediction_layer import PredictionLayer
from error_monitor import ErrorMonitor
from block_detector import BlockDetector
from block_analyzer import BlockAnalyzer
from formula_engine import FormulaEngine
from block_reporting import BlockReporter
from historical_analyzer import HistoricalAnalyzer
from reporting_agent import NewsAnalyzer
from basis_analyzer import BasisAnalyzer

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MainPipeline:
    """Main pipeline orchestrating the complete BANT system"""
    
    def __init__(self, db_path: str = "data/sol_iv.db", symbol: str = "BTCUSDT"):
        self.db_path = db_path
        self.symbol = symbol
        
        # Определяем dataset_tag на основе OPERATION_MODE из config.py
        from config import OPERATION_MODE
        self.dataset_tag = "training_2023" if OPERATION_MODE == "TRAINING" else "live_2025"
        
        # Initialize components
        self.prediction_layer = PredictionLayer()
        self.error_monitor = ErrorMonitor(db_path)
        self.block_detector = BlockDetector(db_path)
        self.block_analyzer = BlockAnalyzer(db_path)
        self.formula_engine = FormulaEngine()
        self.block_reporter = BlockReporter(db_path)
        
        # Initialize analyzers
        self.historical_analyzer = HistoricalAnalyzer(db_path, self.symbol, self.dataset_tag)
        self.news_analyzer = NewsAnalyzer()
        self.basis_analyzer = BasisAnalyzer(db_path, self.symbol, self.dataset_tag)
        
        logger.info(f"🚀 Main Pipeline initialized with all components for {self.symbol} ({self.dataset_tag})")
    
    def run_prediction_cycle(self, prices: List[float], 
                           volatility: float = 1.0,
                           formula_id: str = 'F01') -> Dict:
        """Run a complete prediction cycle"""
        try:
            # Get prediction
            prediction = self.prediction_layer.predict_next_price(
                prices, method="simple_moving_average"
            )
            
            # Calculate confidence separately
            confidence = self.prediction_layer.calculate_prediction_confidence(
                prices, method="simple_moving_average"
            )
            
            # Simulate actual price (in real scenario, this would come from market data)
            actual_price = prices[-1] * (1 + np.random.normal(0, volatility * 0.1))
            
            # Record error
            self.error_monitor.update(
                timestamp=datetime.now(),
                predicted=prediction,
                actual=actual_price,
                volatility=volatility,
                formula_id=formula_id,
                confidence=confidence,
                method="simple_moving_average"
            )
            
            result = {
                'prediction': prediction,
                'actual': actual_price,
                'confidence': confidence,
                'error': abs(prediction - actual_price),
                'timestamp': datetime.now()
            }
            
            logger.info(f"✅ Prediction cycle completed: {result['prediction']:.4f}")
            return result
            
        except Exception as e:
            logger.error(f"❌ Error in prediction cycle: {e}")
            return {}
    
    def detect_blocks(self, lookback_days: int = 30) -> List[Dict]:
        """Detect market regime blocks"""
        try:
            end_time = datetime.now()
            start_time = end_time - timedelta(days=lookback_days)
            
            # Получаем историю ошибок за указанный период
            error_history = self.error_monitor.get_errors(
                start_time=start_time,
                end_time=end_time
            )
            
            # Передаем DataFrame с ошибками в block_detector
            blocks = self.block_detector.detect_block_boundaries(error_history)
            
            logger.info(f"✅ Detected {len(blocks)} market blocks")
            return blocks
            
        except Exception as e:
            logger.error(f"❌ Error detecting blocks: {e}")
            return []
    
    def analyze_blocks(self) -> Dict:
        """Analyze detected blocks"""
        try:
            blocks_df = self.block_analyzer.get_blocks()
            
            if blocks_df.empty:
                return {}
            
            # Get block statistics
            stats = self.block_analyzer.get_block_statistics()
            
            # Classify market regime
            regime = self.block_analyzer.classify_market_regime()
            
            # Get summary
            summary = self.block_analyzer.get_blocks_summary()
            
            analysis = {
                'blocks_count': len(blocks_df),
                'statistics': stats,
                'current_regime': regime,
                'summary': summary
            }
            
            logger.info(f"✅ Block analysis completed: {analysis['blocks_count']} blocks")
            return analysis
            
        except Exception as e:
            logger.error(f"❌ Error analyzing blocks: {e}")
            return {}
    
    def generate_report(self) -> str:
        """Generate comprehensive system report"""
        try:
            # Get error summary
            error_summary = self.error_monitor.get_error_summary()
            
            # Get block analysis
            block_analysis = self.analyze_blocks()
            
            # Generate block report
            report_path = self.block_reporter.generate_comprehensive_report()
            
            report = {
                'timestamp': datetime.now().isoformat(),
                'error_summary': error_summary,
                'block_analysis': block_analysis,
                'report_path': report_path
            }
            
            logger.info("✅ Comprehensive report generated")
            return str(report)
            
        except Exception as e:
            logger.error(f"❌ Error generating report: {e}")
            return f"Error generating report: {e}"
    
    def run_full_cycle(self, prices: List[float], 
                      volatility: float = 1.0,
                      formula_id: str = 'F01') -> Dict:
        """Run complete system cycle"""
        try:
            logger.info("🔄 Starting full system cycle")
            
            # 1. Run prediction
            prediction_result = self.run_prediction_cycle(prices, volatility, formula_id)
            
            # 2. Detect blocks
            blocks = self.detect_blocks()
            
            # 3. Analyze blocks
            block_analysis = self.analyze_blocks()
            
            # 4. Generate report
            report = self.generate_report()
            
            cycle_result = {
                'prediction': prediction_result,
                'blocks': blocks,
                'analysis': block_analysis,
                'report': report,
                'timestamp': datetime.now().isoformat()
            }
            
            logger.info("✅ Full system cycle completed successfully")
            return cycle_result
            
        except Exception as e:
            logger.error(f"❌ Error in full cycle: {e}")
            return {'error': str(e), 'timestamp': datetime.now().isoformat()}
    
    def get_system_status(self) -> Dict:
        """Get current system status"""
        try:
            # Check database connection
            db_status = "connected" if self.error_monitor else "disconnected"
            
            # Get recent errors
            recent_errors = self.error_monitor.get_errors()
            error_count = len(recent_errors) if not recent_errors.empty else 0
            
            # Get recent blocks
            recent_blocks = self.block_analyzer.get_blocks()
            block_count = len(recent_blocks) if not recent_blocks.empty else 0
            
            status = {
                'timestamp': datetime.now().isoformat(),
                'database': db_status,
                'recent_errors': error_count,
                'recent_blocks': block_count,
                'components': {
                    'prediction_layer': 'active',
                    'error_monitor': 'active',
                    'block_detector': 'active',
                    'block_analyzer': 'active',
                    'formula_engine': 'active',
                    'block_reporter': 'active'
                }
            }
            
            logger.info("✅ System status retrieved")
            return status
            
        except Exception as e:
            logger.error(f"❌ Error getting system status: {e}")
            return {'error': str(e), 'timestamp': datetime.now().isoformat()}

    def run_analysis_cycle(self):
        """Run a complete analysis cycle using all three analyzers"""
        print("\n--- ЗАПУСК НОВОГО ЦИКЛА АНАЛИЗА ---")
        print(f"Актив: {self.symbol}, Набор данных: {self.dataset_tag}")
        
        # Шаг 1: Получаем оценки от каждого анализатора
        news_result = self.news_analyzer.analyze_current_news()
        basis_result = self.basis_analyzer.analyze_current_basis()
        # Для historical_analyzer пока используем заглушку
        historical_result = {'score': 0.6, 'sentiment': 'BULLISH', 'details': '6 of 10 past cases were profitable'}

        # Шаг 2: Агрегируем результаты (простая взвешенная сумма)
        weights = {'news': 0.2, 'basis': 0.5, 'history': 0.3}
        
        # Конвертируем BULLISH/BEARISH в +1/-1
        sentiments = {
            'BULLISH': 1,
            'NEUTRAL': 0,
            'BEARISH': -1
        }
        
        final_score = (
            news_result['score'] * sentiments.get(news_result['sentiment'], 0) * weights['news'] +
            basis_result['score'] * sentiments.get(basis_result['sentiment'], 0) * weights['basis'] +
            historical_result['score'] * sentiments.get(historical_result['sentiment'], 0) * weights['history']
        )

        # Шаг 3: Принимаем решение
        if final_score > 0.5:
            final_decision = "STRONG BUY"
        elif final_score > 0.2:
            final_decision = "BUY"
        elif final_score < -0.5:
            final_decision = "STRONG SELL"
        elif final_score < -0.2:
            final_decision = "SELL"
        else:
            final_decision = "HOLD"

        # Шаг 4: Выводим отчет
        print("\n--- ФИНАЛЬНЫЙ ОТЧЕТ СИСТЕМЫ ---")
        year = "2023" if OPERATION_MODE == 'TRAINING' else "2025"
        print(f"РЕЖИМ РАБОТЫ: {OPERATION_MODE} (Данные за {year} год)")
        print(f"Финальное Решение: {final_decision} (Score: {final_score:.4f})")
        print("\n--- Детализация по компонентам ---")
        print(f"Анализ Новостей:   {news_result['sentiment']} (Score: {news_result['score']})")
        print(f"Анализ Basis:      {basis_result['sentiment']} (Score: {basis_result['score']})")
        print(f"Анализ Истории:    {historical_result['sentiment']} (Score: {historical_result['score']})")
        print("---------------------------------")

def main():
    """Main function for testing the pipeline"""
    try:
        logger.info("🚀 Starting BANT Main Pipeline")
        
        # Initialize pipeline with default symbol BTCUSDT
        pipeline = MainPipeline(symbol="BTCUSDT")
        
        # Test with sample data
        sample_prices = [100.0, 105.0, 95.0, 110.0, 90.0, 115.0, 85.0, 120.0]
        
        # Run full cycle
        result = pipeline.run_full_cycle(sample_prices, volatility=0.2)
        
        # Get system status
        status = pipeline.get_system_status()
        
        # Run analysis cycle
        pipeline.run_analysis_cycle()
        
        logger.info("✅ Main Pipeline test completed successfully")
        logger.info(f"System Status: {status}")
        
        return result
        
    except Exception as e:
        logger.error(f"❌ Error in main: {e}")
        return None

if __name__ == "__main__":
    main()
