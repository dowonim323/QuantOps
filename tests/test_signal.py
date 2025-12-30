
import unittest
from unittest.mock import MagicMock, patch
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.market_watcher import get_market_signal

class TestMarketSignal(unittest.TestCase):
    def setUp(self):
        self.mock_kis = MagicMock()
        
        # Mock historical data: 60 days of rising prices for KOSDAQ to make it "Safe"
        # 100, 101, 102... 
        self.kosdaq_history_safe = [100 + i for i in range(60)] # Last values are high
        # Current price > MAs
        
        # Mock historical data: 60 days of falling prices for KOSDAQ to make it "Unsafe"
        self.kosdaq_history_unsafe = [200 - i for i in range(60)] 
        
        # Combined history
        self.history_safe = {"KOSDAQ": self.kosdaq_history_safe}
        self.history_unsafe = {"KOSDAQ": self.kosdaq_history_unsafe}

    def test_signal_buy_when_kosdaq_safe(self):
        # KOSDAQ is Safe (Current 200 > MAs of ~150)
        current_price = 200.0
        
        result = get_market_signal(
            self.mock_kis,
            kosdaq_current=current_price,
            historical_data=self.history_safe,
            verbose=False
        )
        
        self.assertEqual(result["signal"], "buy")
        self.assertTrue(result["details"]["KOSDAQ"]["safe"])

    def test_signal_sell_when_kosdaq_unsafe(self):
        # KOSDAQ is Unsafe (Current 50 < MAs of ~150)
        current_price = 50.0
        
        result = get_market_signal(
            self.mock_kis,
            kosdaq_current=current_price,
            historical_data=self.history_safe, # History is high, current is low
            verbose=False
        )
        
        self.assertEqual(result["signal"], "sell")
        self.assertFalse(result["details"]["KOSDAQ"]["safe"])

    def test_signal_ignores_kospi_implicitly(self):
        # Even if we pass KOSPI data in history, it should use KOSDAQ logic
        # Here KOSDAQ is Safe (Buy)
        history = {
            "KOSDAQ": self.kosdaq_history_safe,
            "KOSPI": self.kosdaq_history_unsafe # KOSPI is crashing
        }
        current_kosdaq = 200.0
        # If signal logic depended on KOSPI being safe (like before), this would fail (return sell)
        # But now it should return buy regardless of KOSPI
        
        result = get_market_signal(
            self.mock_kis,
            kosdaq_current=current_kosdaq,
            historical_data=history,
            verbose=False
        )
        
        self.assertEqual(result["signal"], "buy")
        # Check that details only contain KOSDAQ (or at least result is buy)
        # Based on code, we removed KOSPI analysis, so details should only have KOSDAQ
        self.assertIn("KOSDAQ", result["details"])
        self.assertNotIn("KOSPI", result["details"])

if __name__ == '__main__':
    unittest.main()
