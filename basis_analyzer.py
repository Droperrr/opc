import sqlite3

class BasisAnalyzer:
    def __init__(self, db_path='server_opc.db'):
        self.db_path = db_path

    def analyze_current_basis(self) -> dict:
        """Анализирует текущий basis и возвращает оценку."""
        
        try:
            # Подключаемся к базе данных
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Получаем самую последнюю цену из таблицы спотовых данных
                cursor.execute("""
                    SELECT close
                    FROM spot_data
                    ORDER BY time DESC
                    LIMIT 1
                """)
                spot_result = cursor.fetchone()
                
                # Получаем самую последнюю цену из таблицы фьючерсных данных
                cursor.execute("""
                    SELECT close
                    FROM futures_data
                    ORDER BY time DESC
                    LIMIT 1
                """)
                futures_result = cursor.fetchone()
                
                # Проверяем, есть ли данные
                if spot_result is None or futures_result is None:
                    # Если данных нет, возвращаем нейтральную оценку
                    return {
                        'score': 0.5,
                        'sentiment': 'NEUTRAL',
                        'details': {
                            'basis_relative': 0,
                            'spot_price': 0,
                            'futures_price': 0,
                            'reason': 'Нет данных в таблицах spot_data или futures_data'
                        }
                    }
                
                # Извлекаем цены
                spot_price = spot_result[0]
                futures_price = futures_result[0]
                
        except Exception as e:
            # В случае ошибки подключения к БД возвращаем нейтральную оценку
            return {
                'score': 0.5,
                'sentiment': 'NEUTRAL',
                'details': {
                    'basis_relative': 0,
                    'spot_price': 0,
                    'futures_price': 0,
                    'reason': f'Ошибка подключения к БД: {str(e)}'
                }
            }
        
        # Расчет Basis
        basis_abs = futures_price - spot_price
        basis_rel = basis_abs / spot_price if spot_price > 0 else 0
        
        # Анализ и выставление оценки
        if basis_rel > 0.001:  # > 0.1%
            sentiment = 'BULLISH'
            score = 0.8
        elif basis_rel < -0.001: # < -0.1%
            sentiment = 'BEARISH'
            score = 0.8
        else:
            sentiment = 'NEUTRAL'
            score = 0.5
        
        return {
            'score': score,
            'sentiment': sentiment,
            'details': {
                'basis_relative': basis_rel,
                'spot_price': spot_price,
                'futures_price': futures_price
            }
        }