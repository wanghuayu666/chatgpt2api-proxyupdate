from __future__ import annotations

import os
import time
import unittest


os.environ.setdefault("CHATGPT2API_AUTH_KEY", "test-auth")


class OpenAIBackendUserInfoTests(unittest.TestCase):
    def test_get_user_info_stops_before_extra_requests_when_me_fails(self) -> None:
        from services.openai_backend_api import OpenAIBackendAPI

        backend = OpenAIBackendAPI.__new__(OpenAIBackendAPI)
        backend.access_token = "test-token"
        calls: list[str] = []

        def get_me() -> dict:
            calls.append("me")
            time.sleep(0.05)
            raise RuntimeError("token invalid")

        def get_conversation_init() -> dict:
            calls.append("init")
            return {"limits_progress": []}

        def get_default_account() -> dict:
            calls.append("account")
            return {"plan_type": "free"}

        backend._get_me = get_me
        backend._get_conversation_init = get_conversation_init
        backend._get_default_account = get_default_account

        with self.assertRaises(RuntimeError):
            backend.get_user_info()

        self.assertEqual(calls, ["me"])


if __name__ == "__main__":
    unittest.main()
