from __future__ import annotations

import gc
import json
import os
import threading
import time
import uuid
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timezone
from pathlib import Path

from services.account_service import account_service
from services.config import DATA_DIR
from services.register import mail_provider, openai_register


REGISTER_FILE = DATA_DIR / "register.json"
REGISTER_FD_RESTART_THRESHOLD = 800
REGISTER_FD_RESTART_COOLDOWN_SECONDS = 300
REGISTER_RUNTIME_KEYS = (
    "mail",
    "proxy",
    "proxy_input_mode",
    "proxy_url",
    "proxy_list_text",
    "proxy_refresh_interval",
    "total",
    "threads",
)


def _serialize_outlook_pool(credentials: list[dict]) -> str:
    return "\n".join(
        f'{c["email"]}----{c.get("password", "")}----{c["client_id"]}----{c["refresh_token"]}' for c in credentials
    )


def _merge_outlook_pool(old_text: str, new_text: str) -> str:
    """合并已存邮箱池与新导入文本，按邮箱去重，新导入的同名邮箱覆盖旧凭据。"""
    merged: dict[str, dict] = {}
    for credential in mail_provider.parse_outlook_credentials(old_text or ""):
        merged[credential["email"].strip().lower()] = credential
    for credential in mail_provider.parse_outlook_credentials(new_text or ""):
        merged[credential["email"].strip().lower()] = credential
    return _serialize_outlook_pool(list(merged.values()))


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_config() -> dict:
    return {**openai_register.config, "mode": "total", "target_quota": 100, "target_available": 10, "check_interval": 5, "enabled": False, "stats": {"success": 0, "fail": 0, "done": 0, "running": 0, "threads": openai_register.config["threads"], "elapsed_seconds": 0, "avg_seconds": 0, "success_rate": 0, "current_quota": 0, "current_available": 0, "current_proxy": "", "proxy_pool_count": 0, "proxy_source": "single", "proxy_pool_last_error": "", "proxy_pool_last_fetch": 0, "proxy_state_count": 0, "proxy_blacklist_count": 0}}


def _safe_int(value: object, fallback: int) -> int:
    try:
        return int(value or fallback)
    except (OverflowError, TypeError, ValueError):
        return fallback


def _safe_bool(value: object, fallback: bool) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return fallback
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return fallback


def _normalize(raw: dict) -> dict:
    cfg = _default_config()
    cfg.update({k: v for k, v in raw.items() if k not in {"stats", "logs"}})
    cfg["total"] = max(1, int(cfg.get("total") or 1))
    cfg["threads"] = max(1, int(cfg.get("threads") or 1))
    cfg["mode"] = str(cfg.get("mode") or "total").strip() if str(cfg.get("mode") or "total").strip() in {"total", "quota", "available"} else "total"
    cfg["target_quota"] = max(1, int(cfg.get("target_quota") or 1))
    cfg["target_available"] = max(1, int(cfg.get("target_available") or 1))
    cfg["check_interval"] = max(1, int(cfg.get("check_interval") or 5))
    cfg["proxy"] = str(cfg.get("proxy") or "").strip()
    cfg["proxy_url"] = str(cfg.get("proxy_url") or "").strip()
    cfg["proxy_list_text"] = str(cfg.get("proxy_list_text") or "").strip()
    cfg["proxy_refresh_interval"] = max(10, _safe_int(cfg.get("proxy_refresh_interval"), 120))
    proxy_input_mode = str(cfg.get("proxy_input_mode") or "").strip().lower()
    if proxy_input_mode not in {"single", "url", "text"}:
        if cfg["proxy_url"]:
            proxy_input_mode = "url"
        elif "\n" in cfg["proxy"] or "\r" in cfg["proxy"]:
            proxy_input_mode = "text"
            cfg["proxy_list_text"] = cfg["proxy"]
            cfg["proxy"] = ""
        else:
            proxy_input_mode = "single"
    cfg["proxy_input_mode"] = proxy_input_mode
    default_mail = _default_config()["mail"] if isinstance(_default_config().get("mail"), dict) else {}
    mail = cfg.get("mail") if isinstance(cfg.get("mail"), dict) else {}
    cfg["mail"] = {**default_mail, **mail}
    cfg["mail"]["api_use_register_proxy"] = _safe_bool(cfg["mail"].get("api_use_register_proxy"), True)
    cfg["mail"].pop("proxy", None)
    cfg["enabled"] = bool(cfg.get("enabled"))
    stats = {**_default_config()["stats"], **(raw.get("stats") if isinstance(raw.get("stats"), dict) else {}),
             "threads": cfg["threads"]}
    cfg["stats"] = stats
    return cfg


class RegisterService:
    def __init__(self, store_file: Path):
        self._store_file = store_file
        self._lock = threading.RLock()
        self._runner: threading.Thread | None = None
        self._watchdog: threading.Thread | None = None
        self._fd_restart_pending = False
        self._fd_restart_last_at = 0.0
        self._logs: list[dict] = []
        openai_register.register_log_sink = self._append_log
        self._config = self._load()
        openai_register.config.update({k: self._config[k] for k in REGISTER_RUNTIME_KEYS})
        openai_register.configure_proxy_pool(fetch_now=False)
        if self._config["enabled"]:
            try:
                self.start()
            except Exception as error:
                self._config["enabled"] = False
                self._append_log(f"注册任务自动恢复失败: {error}", "red")
                self._save()

    def _load(self) -> dict:
        try:
            return _normalize(json.loads(self._store_file.read_text(encoding="utf-8")))
        except Exception:
            return _normalize({})

    def _save(self) -> None:
        self._store_file.parent.mkdir(parents=True, exist_ok=True)
        self._store_file.write_text(json.dumps(self._config, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    def get(self) -> dict:
        with self._lock:
            self._ensure_runner_alive_locked()
            snapshot = json.loads(json.dumps({**self._config, "logs": self._logs[-300:]}, ensure_ascii=False))
            try:
                # ponytail: 读状态时直接采样号池；只更新返回值，避免 SSE 轮询频繁写 register.json。
                snapshot["stats"].update(self._pool_metrics())
            except Exception:
                pass
            snapshot["stats"]["runner_alive"] = bool(self._runner and self._runner.is_alive())
            running_stats = openai_register.stats
            for key in ("current_proxy", "proxy_pool_count", "proxy_source", "proxy_pool_last_error", "proxy_pool_last_fetch"):
                if key in running_stats:
                    snapshot["stats"][key] = running_stats[key]
            snapshot["stats"].update(openai_register.proxy_pool.proxy_state_metrics())
        self._redact_outlook_pools(snapshot)
        return snapshot

    @staticmethod
    def _mask_email(email: str) -> str:
        local, sep, domain = str(email or "").partition("@")
        if not sep:
            return "***"
        masked = (local[:2] + "***" + local[-1:]) if len(local) > 2 else (local[:1] + "***")
        return f"{masked}@{domain}"

    def _redact_outlook_pools(self, snapshot: dict) -> None:
        """把 outlook_token 邮箱池里的密码/refresh_token 从对外输出中抹掉，仅保留脱敏预览与统计。

        mailboxes 改为只写导入框（输出为空），避免把密码与 refresh_token 通过 GET/SSE 反复广播。
        """
        mail = snapshot.get("mail")
        if not isinstance(mail, dict):
            return
        providers = mail.get("providers")
        if not isinstance(providers, list):
            return
        for provider in providers:
            if not isinstance(provider, dict) or provider.get("type") != "outlook_token":
                continue
            credentials = mail_provider.parse_outlook_credentials(str(provider.get("mailboxes") or ""))
            provider["mailboxes"] = ""
            provider["mailboxes_count"] = len(credentials)
            provider["mailboxes_preview"] = [self._mask_email(c["email"]) for c in credentials]
            provider["mailboxes_stats"] = mail_provider.outlook_token_pool_stats(credentials)

    def _drop_mail_proxy(self) -> None:
        if isinstance(self._config.get("mail"), dict):
            self._config["mail"].pop("proxy", None)

    def _merge_outlook_pools(self, updates: dict) -> None:
        """对 outlook_token provider：把前端新导入的 mailboxes 与已存池按邮箱合并去重。

        前端 mailboxes 是只写导入框，留空表示不改动；填入的新行追加/覆盖已存凭据。
        按数组下标与已存的同类型 provider 对齐。
        """
        mail = updates.get("mail")
        if not isinstance(mail, dict) or not isinstance(mail.get("providers"), list):
            return
        old_mail = self._config.get("mail") if isinstance(self._config.get("mail"), dict) else {}
        old_providers = old_mail.get("providers") if isinstance(old_mail.get("providers"), list) else []
        for index, provider in enumerate(mail["providers"]):
            if not isinstance(provider, dict) or provider.get("type") != "outlook_token":
                continue
            old = old_providers[index] if index < len(old_providers) and isinstance(old_providers[index], dict) else {}
            old_text = str(old.get("mailboxes") or "") if old.get("type") == "outlook_token" else ""
            new_text = str(provider.get("mailboxes") or "")
            provider["mailboxes"] = _merge_outlook_pool(old_text, new_text) if (old_text or new_text) else ""
            for key in ("mailboxes_count", "mailboxes_preview", "mailboxes_stats"):
                provider.pop(key, None)

    def _prune_unused_outlook_pools(self) -> int:
        mail = self._config.get("mail")
        if not isinstance(mail, dict):
            return 0
        providers = mail.get("providers")
        if not isinstance(providers, list):
            return 0
        total_removed = 0
        for provider in providers:
            if not isinstance(provider, dict) or provider.get("type") != "outlook_token":
                continue
            credentials = mail_provider.parse_outlook_credentials(str(provider.get("mailboxes") or ""))
            kept, removed = mail_provider.prune_outlook_unused_credentials(credentials)
            if removed:
                provider["mailboxes"] = _serialize_outlook_pool(kept)
                total_removed += removed
            for key in ("mailboxes_count", "mailboxes_preview", "mailboxes_stats"):
                provider.pop(key, None)
        return total_removed

    def update(self, updates: dict) -> dict:
        with self._lock:
            self._merge_outlook_pools(updates)
            self._config = _normalize({**self._config, **updates})
            self._drop_mail_proxy()
            openai_register.config.update({k: self._config[k] for k in REGISTER_RUNTIME_KEYS})
            openai_register.configure_proxy_pool(fetch_now=False)
            self._save()
            return self.get()

    def start(self) -> dict:
        with self._lock:
            if self._fd_restart_pending and self._runner and self._runner.is_alive():
                self._spawn_watchdog_locked()
                self._save()
                return self.get()
            if self._runner and self._runner.is_alive():
                self._config["enabled"] = True
                self._spawn_watchdog_locked()
                self._save()
                return self.get()
            self._fd_restart_pending = False
            self._config["enabled"] = True
            self._drop_mail_proxy()
            self._logs = []
            metrics = self._pool_metrics()
            openai_register.config.update({k: self._config[k] for k in REGISTER_RUNTIME_KEYS})
            try:
                proxy_metrics = openai_register.prepare_proxy_pool()
            except Exception as error:
                message = f"注册代理不可用: {error}"
                self._config["enabled"] = False
                self._config["stats"] = {"success": 0, "fail": 0, "done": 0, "running": 0, "threads": self._config["threads"], **metrics, "proxy_pool_last_error": str(error), "updated_at": _now()}
                self._save()
                self._append_log(message, "red")
                raise RuntimeError(message) from error
            self._config["stats"] = {"job_id": uuid.uuid4().hex, "success": 0, "fail": 0, "done": 0, "running": 0, "threads": self._config["threads"], **metrics, **proxy_metrics, "current_proxy": "", "started_at": _now(), "updated_at": _now()}
            with openai_register.stats_lock:
                openai_register.stats.update({"done": 0, "success": 0, "fail": 0, "start_time": time.time(), "current_proxy": "", **proxy_metrics})
            self._save()
            self._spawn_runner_locked()
            self._spawn_watchdog_locked()
            self._append_log(f"注册任务启动，模式={self._config['mode']}，线程数={self._config['threads']}", "yellow")
            return self.get()

    def stop(self) -> dict:
        with self._lock:
            self._fd_restart_pending = False
            self._config["enabled"] = False
            self._config["stats"].update({"fd_restart_pending": False, "updated_at": _now()})
            self._save()
            self._append_log("已请求停止注册任务，正在等待当前运行任务结束", "yellow")
            return self.get()

    def reset(self) -> dict:
        with self._lock:
            self._logs = []
            proxy_metrics = openai_register.configure_proxy_pool(fetch_now=False)
            self._config["stats"] = {"success": 0, "fail": 0, "done": 0, "running": 0, "threads": self._config["threads"], "elapsed_seconds": 0, "avg_seconds": 0, "success_rate": 0, **self._pool_metrics(), **proxy_metrics, **openai_register.proxy_pool.proxy_state_metrics(), "current_proxy": "", "updated_at": _now()}
            with openai_register.stats_lock:
                openai_register.stats.update({"done": 0, "success": 0, "fail": 0, "start_time": 0.0, "current_proxy": "", **proxy_metrics})
            self._save()
            return self.get()

    def reset_proxy_blacklist(self) -> dict:
        with self._lock:
            if self._config.get("enabled"):
                raise RuntimeError("注册任务运行中，先停止再重置代理黑名单")
            metrics = openai_register.proxy_pool.reset_proxy_blacklist()
            self._config["stats"].update(metrics)
            self._config["stats"]["updated_at"] = _now()
            self._save()
            self._append_log(f"已重置代理黑名单，清除 {metrics['removed_proxy_state_count']} 条代理状态", "yellow")
            return self.get()

    def reset_outlook_pool(self, scope: str = "all") -> dict:
        scope = str(scope or "all").strip().lower()
        if scope == "unused":
            with self._lock:
                removed = self._prune_unused_outlook_pools()
                openai_register.config.update({k: self._config[k] for k in REGISTER_RUNTIME_KEYS})
                self._save()
                self._append_log(f"已清空 Outlook 邮箱池未使用邮箱，移除 {removed} 个", "yellow")
            return self.get()
        scope = "failed" if str(scope) == "failed" else "all"
        cleared = mail_provider.reset_outlook_token_pool_state(scope)
        with self._lock:
            self._append_log(
                f"已重置 Outlook 邮箱池状态（范围={'仅失败/占用' if scope == 'failed' else '全部'}），清除 {cleared} 条记录",
                "yellow",
            )
        return self.get()

    def _append_log(self, text: str, color: str = "") -> None:
        with self._lock:
            self._logs.append({"time": _now(), "text": str(text), "level": str(color or "info")})
            self._logs = self._logs[-300:]

    def _pool_metrics(self) -> dict:
        items = account_service.list_accounts()
        normal = [item for item in items if item.get("status") == "正常"]
        return {
            "current_quota": sum(int(item.get("quota") or 0) for item in normal if not item.get("image_quota_unknown")),
            "current_available": len(normal),
        }

    def _spawn_runner_locked(self) -> None:
        self._runner = threading.Thread(target=self._run, daemon=True, name="openai-register")
        self._runner.start()

    def _spawn_watchdog_locked(self) -> None:
        if self._watchdog and self._watchdog.is_alive():
            return
        self._watchdog = threading.Thread(target=self._watchdog_loop, daemon=True, name="openai-register-watchdog")
        self._watchdog.start()

    def _ensure_runner_alive_locked(self) -> None:
        if not self._config.get("enabled"):
            return
        if self._runner and self._runner.is_alive():
            return
        self._append_log("检测到注册守护线程已停止，自动重启", "red")
        self._spawn_runner_locked()

    def _fd_count(self) -> int:
        try:
            return len(os.listdir("/proc/self/fd"))
        except Exception:
            return 0

    def _request_fd_restart_locked(self) -> bool:
        if self._fd_restart_pending or not self._config.get("enabled"):
            return False
        if not self._runner or not self._runner.is_alive():
            return False
        fd_count = self._fd_count()
        if fd_count < REGISTER_FD_RESTART_THRESHOLD:
            return False
        now = time.monotonic()
        if self._fd_restart_last_at and now - self._fd_restart_last_at < REGISTER_FD_RESTART_COOLDOWN_SECONDS:
            return False
        self._fd_restart_pending = True
        self._fd_restart_last_at = now
        self._config["enabled"] = False
        self._config["stats"].update({
            "fd_count": fd_count,
            "fd_restart_threshold": REGISTER_FD_RESTART_THRESHOLD,
            "fd_restart_pending": True,
            "updated_at": _now(),
        })
        self._save()
        self._append_log(
            f"检测到注册进程 fd={fd_count} 超过阈值 {REGISTER_FD_RESTART_THRESHOLD}，自动停止并准备重启注册任务",
            "red",
        )
        return True

    def _complete_fd_restart_locked(self) -> int:
        interval = max(1, int(self._config.get("check_interval") or 5))
        if self._runner and self._runner.is_alive():
            return interval
        self._fd_restart_pending = False
        self._config["stats"].update({"fd_restart_pending": False, "updated_at": _now()})
        try:
            gc.collect()
            self.start()
        except Exception as error:
            self._config["enabled"] = False
            self._config["stats"].update({"last_error": f"fd 自动重启失败: {error}", "updated_at": _now()})
            self._save()
            self._append_log(f"fd 自动重启失败: {error}", "red")
            return 0
        self._append_log("fd 超阈值自动重启注册任务完成", "yellow")
        return interval

    def _watchdog_tick(self) -> int:
        with self._lock:
            if self._fd_restart_pending:
                return self._complete_fd_restart_locked()
            if not self._config.get("enabled"):
                return 0
            if self._request_fd_restart_locked():
                return max(1, int(self._config.get("check_interval") or 5))
            self._ensure_runner_alive_locked()
            return max(1, int(self._config.get("check_interval") or 5))

    def _watchdog_loop(self) -> None:
        while True:
            interval = self._watchdog_tick()
            if interval <= 0:
                return
            time.sleep(interval)

    def _target_reached(self, cfg: dict, submitted: int) -> bool:
        mode = str(cfg.get("mode") or "total")
        metrics = self._pool_metrics()
        self._bump(**metrics)
        if mode == "quota":
            reached = metrics["current_quota"] >= int(cfg.get("target_quota") or 1)
            self._append_log(f"检查号池：当前正常账号={metrics['current_available']}，当前剩余额度={metrics['current_quota']}，目标额度={cfg.get('target_quota')}，{'跳过注册' if reached else '继续注册'}", "yellow")
            return reached
        if mode == "available":
            reached = metrics["current_available"] >= int(cfg.get("target_available") or 1)
            self._append_log(f"检查号池：当前正常账号={metrics['current_available']}，目标账号={cfg.get('target_available')}，当前剩余额度={metrics['current_quota']}，{'跳过注册' if reached else '继续注册'}", "yellow")
            return reached
        return submitted >= int(cfg.get("total") or 1)

    def _bump(self, **updates) -> None:
        with self._lock:
            self._config["stats"].update(updates)
            stats = self._config["stats"]
            started_at = str(stats.get("started_at") or "")
            if started_at:
                try:
                    elapsed = max(0.0, (datetime.now(timezone.utc) - datetime.fromisoformat(started_at)).total_seconds())
                except Exception:
                    elapsed = 0.0
                done = int(stats.get("done") or 0)
                success = int(stats.get("success") or 0)
                fail = int(stats.get("fail") or 0)
                stats["elapsed_seconds"] = round(elapsed, 1)
                stats["avg_seconds"] = round(elapsed / success, 1) if success else 0
                stats["success_rate"] = round(success * 100 / max(1, success + fail), 1)
            self._config["stats"]["updated_at"] = _now()
            self._save()

    def _auto_reset_proxy_blacklist_if_due(self, cfg: dict, last_reset_at: float) -> float:
        interval = max(10, int(cfg.get("proxy_refresh_interval") or 120))
        now = time.monotonic()
        if now - last_reset_at < interval:
            return last_reset_at
        try:
            metrics = openai_register.proxy_pool.reset_proxy_blacklist()
            self._bump(**metrics)
            removed = int(metrics.get("removed_proxy_state_count") or 0)
            if removed:
                self._append_log(f"已自动重置代理黑名单，清除 {removed} 条代理状态", "yellow")
        except Exception as error:
            self._append_log(f"自动重置代理黑名单失败: {error}", "red")
        return now

    def _run(self) -> None:
        while True:
            try:
                self._run_loop()
                return
            except Exception as error:
                with self._lock:
                    enabled = bool(self._config.get("enabled"))
                    self._config["stats"].update({"running": 0, "runner_alive": False, "last_error": str(error), "updated_at": _now()})
                    try:
                        self._save()
                    except Exception:
                        pass
                    interval = max(1, int(self._config.get("check_interval") or 5))
                self._append_log(f"注册守护线程异常: {error}", "red")
                if not enabled:
                    return
                time.sleep(interval)

    def _run_loop(self) -> None:
        threads = int(self.get()["threads"])
        submitted, done, success, fail = 0, 0, 0, 0
        waiting_for_target_drop = False
        last_proxy_blacklist_reset = time.monotonic()
        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures = set()
            while True:
                cfg = self.get()
                last_proxy_blacklist_reset = self._auto_reset_proxy_blacklist_if_due(cfg, last_proxy_blacklist_reset)
                target_reached = self._target_reached(cfg, submitted)
                if target_reached and str(cfg.get("mode") or "total") in {"quota", "available"}:
                    waiting_for_target_drop = True
                while self.get()["enabled"] and not target_reached and len(futures) < threads:
                    if waiting_for_target_drop:
                        proxy_metrics = openai_register.reset_proxy_pool_cycle()
                        self._bump(**proxy_metrics)
                        self._append_log("号池低于目标，已沿用当前黑名单并从代理列表开头重新评估", "yellow")
                        waiting_for_target_drop = False
                    submitted += 1
                    futures.add(executor.submit(openai_register.worker, submitted))
                    cfg = self.get()
                    target_reached = self._target_reached(cfg, submitted)
                self._bump(running=len(futures), done=done, success=success, fail=fail)
                if not futures and (not self.get()["enabled"] or str(cfg.get("mode") or "total") == "total"):
                    break
                if not futures:
                    time.sleep(max(1, int(cfg.get("check_interval") or 5)))
                    continue
                finished, futures = wait(
                    futures,
                    timeout=max(1, int(cfg.get("check_interval") or 5)),
                    return_when=FIRST_COMPLETED,
                )
                for future in finished:
                    done += 1
                    try:
                        result = future.result()
                        success += 1 if result.get("ok") else 0
                        fail += 0 if result.get("ok") else 1
                    except Exception:
                        fail += 1
        self._bump(running=0, done=done, success=success, fail=fail, finished_at=_now())
        with self._lock:
            self._config["enabled"] = False
            self._save()
        self._append_log(f"注册任务结束，成功{success}，失败{fail}", "yellow")


register_service = RegisterService(REGISTER_FILE)
