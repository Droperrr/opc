print("--- Verifying fix for OPC-FIX-CRITICAL-01 ---")
try:
    from main_pipeline import MainPipeline
    
    print("Import successful. Initializing pipeline...")
    pipeline = MainPipeline()
    
    sample_prices = [100.0, 105.0, 95.0, 110.0, 90.0, 115.0, 85.0, 120.0]
    
    print("Running prediction cycle...")
    result = pipeline.run_prediction_cycle(sample_prices)
    
    if 'prediction' in result and 'confidence' in result:
        print(f"SUCCESS: Prediction cycle completed.")
        print(f"Prediction: {result['prediction']:.2f}, Confidence: {result['confidence']:.2f}")
    else:
        print("ERROR: Prediction cycle failed to return expected values.")

except Exception as e:
    print(f"CRITICAL ERROR during verification: {e}")

print("--- Verification finished ---")