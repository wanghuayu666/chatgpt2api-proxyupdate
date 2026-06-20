from __future__ import annotations

import os
import importlib
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


os.environ.setdefault("CHATGPT2API_AUTH_KEY", "test-auth")


class RegisterServiceTests(unittest.TestCase):
    def _new_isolated_service(self, tmp_dir: str):
        from services import config as config_module

        old_data_dir = config_module.DATA_DIR
        config_module.DATA_DIR = Path(tmp_dir)
        sys.modules.pop("services.register_service", None)
        try:
            register_module = importlib.import_module("services.register_service")
            service = register_module.RegisterService(Path(tmp_dir) / "register.json")
        finally:
            config_module.DATA_DIR = old_data_dir
        return register_module, service

    def test_get_samples_current_account_pool_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            register_module, service = self._new_isolated_service(tmp_dir)
            service._config["stats"].update({"current_available": 99, "current_quota": 999})

            fake_accounts = [
                {"status": "正常", "quota": 3},
                {"status": "正常", "quota": 5},
                {"status": "异常", "quota": 100},
            ]
            with patch.object(register_module.account_service, "list_accounts", return_value=fake_accounts):
                snapshot = service.get()

            self.assertEqual(snapshot["stats"]["current_available"], 2)
            self.assertEqual(snapshot["stats"]["current_quota"], 8)

    def test_auto_proxy_blacklist_reset_waits_for_proxy_refresh_interval(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            register_module, service = self._new_isolated_service(tmp_dir)

            with patch.object(register_module.time, "monotonic", return_value=109.0), patch.object(
                register_module.openai_register.proxy_pool,
                "reset_proxy_blacklist",
            ) as reset_proxy_blacklist:
                next_reset = service._auto_reset_proxy_blacklist_if_due({"proxy_refresh_interval": 10}, 100.0)

            self.assertEqual(next_reset, 100.0)
            reset_proxy_blacklist.assert_not_called()

    def test_auto_proxy_blacklist_reset_reuses_manual_reset_logic_when_due(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            register_module, service = self._new_isolated_service(tmp_dir)
            metrics = {
                "removed_proxy_state_count": 3,
                "proxy_state_count": 0,
                "proxy_blacklist_count": 0,
            }

            with patch.object(register_module.time, "monotonic", return_value=111.0), patch.object(
                register_module.openai_register.proxy_pool,
                "reset_proxy_blacklist",
                return_value=metrics,
            ) as reset_proxy_blacklist:
                next_reset = service._auto_reset_proxy_blacklist_if_due({"proxy_refresh_interval": 10}, 100.0)

            self.assertEqual(next_reset, 111.0)
            reset_proxy_blacklist.assert_called_once_with()
            self.assertEqual(service._config["stats"]["proxy_blacklist_count"], 0)
            self.assertTrue(any("已自动重置代理黑名单，清除 3 条代理状态" in item["text"] for item in service._logs))

    def test_auto_proxy_blacklist_reset_error_does_not_escape(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            register_module, service = self._new_isolated_service(tmp_dir)

            with patch.object(register_module.time, "monotonic", return_value=111.0), patch.object(
                register_module.openai_register.proxy_pool,
                "reset_proxy_blacklist",
                side_effect=OSError("readonly state"),
            ):
                next_reset = service._auto_reset_proxy_blacklist_if_due({"proxy_refresh_interval": 10}, 100.0)

            self.assertEqual(next_reset, 111.0)
            self.assertTrue(any("自动重置代理黑名单失败: readonly state" in item["text"] for item in service._logs))

    def test_get_restarts_enabled_service_when_runner_is_dead(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            _, service = self._new_isolated_service(tmp_dir)
            service._config["enabled"] = True
            service._runner = None

            with patch.object(service, "_spawn_runner_locked") as spawn_runner:
                service.get()

            spawn_runner.assert_called_once_with()
            self.assertTrue(any("检测到注册守护线程已停止，自动重启" in item["text"] for item in service._logs))

    def test_watchdog_restarts_enabled_service_without_get(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            _, service = self._new_isolated_service(tmp_dir)
            service._config["enabled"] = True
            service._config["check_interval"] = 7
            service._runner = None

            with patch.object(service, "_spawn_runner_locked") as spawn_runner:
                interval = service._watchdog_tick()

            self.assertEqual(interval, 7)
            spawn_runner.assert_called_once_with()
            self.assertTrue(any("检测到注册守护线程已停止，自动重启" in item["text"] for item in service._logs))

    def test_runner_exception_keeps_enabled_for_next_retry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            _, service = self._new_isolated_service(tmp_dir)
            service._config["enabled"] = True
            attempts = {"count": 0}

            def fail_once_then_stop() -> None:
                attempts["count"] += 1
                if attempts["count"] == 1:
                    raise RuntimeError("pool read failed")
                service._config["enabled"] = False

            with patch.object(service, "_run_loop", side_effect=fail_once_then_stop), patch("services.register_service.time.sleep"):
                service._run()

            self.assertEqual(attempts["count"], 2)
            self.assertFalse(service._config["enabled"])
            self.assertEqual(service._config["stats"]["last_error"], "pool read failed")
            self.assertTrue(any("注册守护线程异常: pool read failed" in item["text"] for item in service._logs))


if __name__ == "__main__":
    unittest.main()
