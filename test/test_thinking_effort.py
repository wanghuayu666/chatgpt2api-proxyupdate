from __future__ import annotations

import os
import unittest


os.environ.setdefault("CHATGPT2API_AUTH_KEY", "test-auth")


class ThinkingEffortTests(unittest.TestCase):
    def test_openai_effort_fields_normalize_to_backend_values(self) -> None:
        from services.protocol.openai_v1_chat_complete import parse_thinking_effort

        self.assertEqual(parse_thinking_effort({"thinking_effort": "low"}), "low")
        self.assertEqual(parse_thinking_effort({"reasoning_effort": "medium"}), "medium")
        self.assertEqual(parse_thinking_effort({"reasoning": {"effort": "high"}}), "high")
        self.assertEqual(parse_thinking_effort({"reasoning_effort": "xhigh"}), "extended")
        self.assertEqual(parse_thinking_effort({"thinking_effort": "extended"}), "extended")
        self.assertEqual(parse_thinking_effort({"thinking_effort": "none"}), "")
        self.assertEqual(parse_thinking_effort({"thinking_effort": ""}), "")
        self.assertEqual(parse_thinking_effort({"thinking_effort": "wild"}), "")

    def test_backend_payload_only_includes_supported_thinking_effort(self) -> None:
        from services.openai_backend_api import OpenAIBackendAPI

        backend = OpenAIBackendAPI.__new__(OpenAIBackendAPI)
        messages = [{"role": "user", "content": "Juice测试提示词"}]

        high_payload = backend._conversation_payload(messages, "auto", "Asia/Shanghai", "high")
        extended_payload = backend._conversation_payload(messages, "auto", "Asia/Shanghai", "extended")
        invalid_payload = backend._conversation_payload(messages, "auto", "Asia/Shanghai", "wild")
        empty_payload = backend._conversation_payload(messages, "auto", "Asia/Shanghai", "")

        self.assertEqual(high_payload.get("thinking_effort"), "high")
        self.assertEqual(extended_payload.get("thinking_effort"), "extended")
        self.assertNotIn("thinking_effort", invalid_payload)
        self.assertNotIn("thinking_effort", empty_payload)

    def test_cache_key_keeps_all_effort_field_shapes_distinct(self) -> None:
        from services.protocol.chat_completion_cache import cache_key

        messages = [{"role": "user", "content": "Juice测试提示词"}]

        low_key = cache_key({"model": "auto", "thinking_effort": "low"}, messages, stream=False)
        high_key = cache_key({"model": "auto", "thinking_effort": "high"}, messages, stream=False)
        reasoning_low_key = cache_key({"model": "auto", "reasoning": {"effort": "low"}}, messages, stream=False)
        reasoning_high_key = cache_key({"model": "auto", "reasoning": {"effort": "high"}}, messages, stream=False)

        self.assertNotEqual(low_key, high_key)
        self.assertNotEqual(reasoning_low_key, reasoning_high_key)


if __name__ == "__main__":
    unittest.main()
