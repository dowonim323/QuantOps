import unittest
from pathlib import Path


class TestSupervisordConfig(unittest.TestCase):
    def test_supervisord_uses_controllers_instead_of_cron(self):
        config_path = Path(__file__).resolve().parent.parent / "scheduler" / "supervisord.conf"
        content = config_path.read_text(encoding="utf-8")

        self.assertIn("[program:nightly-prep-controller]", content)
        self.assertIn("[program:trading-day-controller]", content)
        self.assertIn("python -m pipelines.nightly_prep_controller", content)
        self.assertIn("python -m pipelines.trading_day_controller", content)
        self.assertNotIn("[program:cron]", content)
        self.assertNotIn("tail -F /var/log/cron.log", content)


if __name__ == "__main__":
    unittest.main()
