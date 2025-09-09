from basis_analyzer import BasisAnalyzer

print("--- Verifying BasisAnalyzer ---")
analyzer = BasisAnalyzer()
analysis_result = analyzer.analyze_current_basis()

print("Analysis Result:")
print(analysis_result)

if 'score' in analysis_result and 'sentiment' in analysis_result:
    print("\nSUCCESS: BasisAnalyzer is working correctly and returns data in the correct format.")
else:
    print("\nERROR: BasisAnalyzer failed to return data in the correct format.")

print("--- Verification finished ---")