import copy
import functools
import inspect
import json
import os
import re
import threading
import time
from typing import Any, Dict, Optional
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import gradio as gr
from shared.utils.plugins import WAN2GPPlugin


class NotifierSink:
    def emit(self, event: Dict[str, Any]) -> None:
        raise NotImplementedError


class ConsoleNotifierSink(NotifierSink):
    def emit(self, event: Dict[str, Any]) -> None:
        print(event.get("message", ""))


class AppriseNotifierSink(NotifierSink):
    def __init__(self, plugin: "Wan2GPNotifierPlugin"):
        self.plugin = plugin

    def emit(self, event: Dict[str, Any]) -> None:
        self.plugin._emit_to_apprise(event)


class MultiNotifierSink(NotifierSink):
    def __init__(self, sinks):
        self.sinks = sinks

    def emit(self, event: Dict[str, Any]) -> None:
        for sink in self.sinks:
            try:
                sink.emit(event)
            except Exception:
                continue


class Wan2GPNotifierPlugin(WAN2GPPlugin):
    SETTINGS_FILENAME = "settings.json"
    PROVIDER_CHOICES = ["telegram", "discord", "whatsapp", "ifttt", "google_chat"]

    DEFAULT_SETTINGS = {
        "enabled": True,
        "provider": "telegram",
        "providers": {
            "telegram": {
                "bot_token": "",
                "chat_id": "",
            },
            "discord": {
                "webhook_url": "",
            },
            "whatsapp": {
                "token": "",
                "from_phone_id": "",
                "targets": "",
            },
            "ifttt": {
                "webhook_id": "",
                "events": "",
                "query_params": "",
            },
            "google_chat": {
                "webhook_url": "",
            },
        },
    }

    def __init__(self):
        super().__init__()
        self.name = "Queue Notifier"
        self.version = "1.1.0"
        self.description = "Logs queue status and sends notifications through Apprise."
        self._wrapped = False
        self._queue_update_wrapped = False
        self._global_queue_ref_update_wrapped = False
        self._process_tasks_wrapped = False
        self._original_generate_video = None
        self._original_update_queue_data = None
        self._original_update_global_queue_ref = None
        self._original_process_tasks = None

        self._settings_lock = threading.Lock()
        self._progress_lock = threading.Lock()
        self._settings_path = os.path.join(os.path.dirname(__file__), self.SETTINGS_FILENAME)
        self._settings = self._load_settings()
        self._run_total_tasks: Optional[int] = None
        self._completed_tasks_in_run: int = 0
        self._last_known_queue_len: int = 0
        self._debug_enabled: bool = False
        self._debug_counter: int = 0

        self._apprise_import_warned = False
        self._sink = self._build_sink()

    def setup_ui(self):
        self.request_global("generate_video")
        self.request_global("get_gen_info")
        self.request_global("global_queue_ref")
        self.request_global("update_queue_data")
        self.request_global("update_global_queue_ref")
        self.request_global("process_tasks")
        self.add_tab(
            tab_id="notifier",
            label="Notifier",
            component_constructor=self.create_ui,
        )

    def post_ui_setup(self, components):
        self._install_process_tasks_wrapper_if_needed()
        self._install_global_queue_ref_wrapper_if_needed()
        self._install_queue_update_wrapper_if_needed()
        self._install_wrapper_if_needed()
        return {}

    def create_ui(self):
        settings = self._get_settings_snapshot()
        provider = settings.get("provider", "telegram")
        providers = settings.get("providers", {})

        telegram_cfg = providers.get("telegram", {})
        discord_cfg = providers.get("discord", {})
        whatsapp_cfg = providers.get("whatsapp", {})
        ifttt_cfg = providers.get("ifttt", {})
        gchat_cfg = providers.get("google_chat", {})

        with gr.Blocks() as demo:
            gr.Markdown("## Queue Notifier")
            gr.Markdown(
                "Configure notifications via Apprise (Telegram, Discord, WhatsApp, IFTTT, Google Chat)."
            )

            enabled_toggle = gr.Checkbox(
                label="Enable notifications",
                value=bool(settings.get("enabled", True)),
            )
            provider_selector = gr.Dropdown(
                choices=self.PROVIDER_CHOICES,
                value=provider,
                label="Notification channel (Apprise)",
            )

            with gr.Column(visible=provider == "telegram") as telegram_col:
                gr.Markdown("### Telegram")
                telegram_bot_token = gr.Textbox(
                    label="Bot Token",
                    value=telegram_cfg.get("bot_token", ""),
                    type="password",
                )
                telegram_chat_id = gr.Textbox(
                    label="Chat ID",
                    value=telegram_cfg.get("chat_id", ""),
                )

            with gr.Column(visible=provider == "discord") as discord_col:
                gr.Markdown("### Discord")
                discord_webhook_url = gr.Textbox(
                    label="Webhook URL (recommended)",
                    value=discord_cfg.get("webhook_url", ""),
                )

            with gr.Column(visible=provider == "whatsapp") as whatsapp_col:
                gr.Markdown("### WhatsApp")
                whatsapp_token = gr.Textbox(
                    label="Token",
                    value=whatsapp_cfg.get("token", ""),
                    type="password",
                )
                whatsapp_from_phone_id = gr.Textbox(
                    label="From Phone ID",
                    value=whatsapp_cfg.get("from_phone_id", ""),
                )
                whatsapp_targets = gr.Textbox(
                    label="Target phones (comma, space or ;)",
                    value=whatsapp_cfg.get("targets", ""),
                )

            with gr.Column(visible=provider == "ifttt") as ifttt_col:
                gr.Markdown("### IFTTT")
                ifttt_webhook_id = gr.Textbox(
                    label="Webhook ID",
                    value=ifttt_cfg.get("webhook_id", ""),
                )
                ifttt_events = gr.Textbox(
                    label="Events (comma, space, ; or /)",
                    value=ifttt_cfg.get("events", ""),
                )
                ifttt_query = gr.Textbox(
                    label="Query params (optional, e.g.: value1=hello&value2=wan2gp)",
                    value=ifttt_cfg.get("query_params", ""),
                )

            with gr.Column(visible=provider == "google_chat") as gchat_col:
                gr.Markdown("### Google Chat")
                gchat_webhook_url = gr.Textbox(
                    label="Webhook URL (recommended)",
                    value=gchat_cfg.get("webhook_url", ""),
                )

            generated_url = gr.Textbox(
                label="Generated Apprise URL (masked)",
                value=self._preview_url(settings),
                interactive=False,
            )
            status_text = gr.Markdown(value=self._build_status_text(settings))

            with gr.Row():
                save_btn = gr.Button("Save configuration", variant="primary")
                test_btn = gr.Button("Send test")
            feedback_text = gr.Markdown(value="")

            all_inputs = [
                enabled_toggle,
                provider_selector,
                telegram_bot_token,
                telegram_chat_id,
                discord_webhook_url,
                whatsapp_token,
                whatsapp_from_phone_id,
                whatsapp_targets,
                ifttt_webhook_id,
                ifttt_events,
                ifttt_query,
                gchat_webhook_url,
            ]

            all_outputs = [
                status_text,
                generated_url,
                feedback_text,
                telegram_col,
                discord_col,
                whatsapp_col,
                ifttt_col,
                gchat_col,
            ]

            def save_config(*args):
                new_settings = self._build_settings_from_ui(*args)
                self._set_settings_snapshot(new_settings, persist=True)
                visible = self._provider_visibility_updates(new_settings.get("provider", "telegram"))
                return (
                    self._build_status_text(new_settings),
                    self._preview_url(new_settings),
                    "Configuration saved.",
                    *visible,
                )

            def send_test(*args):
                new_settings = self._build_settings_from_ui(*args)
                self._set_settings_snapshot(new_settings, persist=True)
                ok, msg = self._send_apprise_notification(
                    {
                        "type": "task.success",
                        "timestamp": time.time(),
                        "task_id": "test",
                        "progress": {"current": 1, "total": 1},
                        "details": None,
                        "message": "[Notifier] Test message from Wan2GP.",
                    },
                    new_settings,
                )
                visible = self._provider_visibility_updates(new_settings.get("provider", "telegram"))
                test_status = "Test sent successfully." if ok else f"Could not send test: {msg}"
                return (
                    self._build_status_text(new_settings),
                    self._preview_url(new_settings),
                    test_status,
                    *visible,
                )

            save_btn.click(fn=save_config, inputs=all_inputs, outputs=all_outputs)
            test_btn.click(fn=send_test, inputs=all_inputs, outputs=all_outputs)
            provider_selector.change(fn=save_config, inputs=all_inputs, outputs=all_outputs)
            enabled_toggle.change(fn=save_config, inputs=all_inputs, outputs=all_outputs)

        return demo

    def _provider_visibility_updates(self, provider: str):
        provider = provider if provider in self.PROVIDER_CHOICES else "telegram"
        return (
            gr.update(visible=provider == "telegram"),
            gr.update(visible=provider == "discord"),
            gr.update(visible=provider == "whatsapp"),
            gr.update(visible=provider == "ifttt"),
            gr.update(visible=provider == "google_chat"),
        )

    def _build_settings_from_ui(
        self,
        enabled,
        provider,
        telegram_bot_token,
        telegram_chat_id,
        discord_webhook_url,
        whatsapp_token,
        whatsapp_from_phone_id,
        whatsapp_targets,
        ifttt_webhook_id,
        ifttt_events,
        ifttt_query,
        gchat_webhook_url,
    ):
        settings = self._get_settings_snapshot()
        settings["enabled"] = bool(enabled)
        settings["provider"] = provider if provider in self.PROVIDER_CHOICES else "telegram"

        settings["providers"]["telegram"] = {
            "bot_token": str(telegram_bot_token or "").strip(),
            "chat_id": str(telegram_chat_id or "").strip(),
        }

        settings["providers"]["discord"] = {
            "webhook_url": str(discord_webhook_url or "").strip(),
        }

        settings["providers"]["whatsapp"] = {
            "token": str(whatsapp_token or "").strip(),
            "from_phone_id": str(whatsapp_from_phone_id or "").strip(),
            "targets": str(whatsapp_targets or "").strip(),
        }

        settings["providers"]["ifttt"] = {
            "webhook_id": str(ifttt_webhook_id or "").strip(),
            "events": str(ifttt_events or "").strip(),
            "query_params": str(ifttt_query or "").strip(),
        }

        settings["providers"]["google_chat"] = {
            "webhook_url": str(gchat_webhook_url or "").strip(),
        }

        return settings

    def _install_wrapper_if_needed(self):
        if self._wrapped:
            return

        generate_video_fn = getattr(self, "generate_video", None)
        if not callable(generate_video_fn):
            self._emit_system_event("generate_video not available; wrapper not installed.")
            return

        if getattr(generate_video_fn, "_wan2gp_notifier_wrapped", False):
            self._wrapped = True
            self._original_generate_video = getattr(
                generate_video_fn, "_wan2gp_notifier_original", generate_video_fn
            )
            self._emit_system_event("generate_video already wrapped.")
            return

        original_fn = generate_video_fn
        self._original_generate_video = original_fn

        @functools.wraps(original_fn)
        def wrapped_generate_video(task, send_cmd, *args, **kwargs):
            state = kwargs.get("state")
            task_id = self._extract_task_id(task)
            prompt_no, prompts_max, queue_len_before = self._read_queue_progress(state)
            self._debug_log(
                "wrapper.generate_video.start",
                task_id=task_id,
                prompt_no=prompt_no,
                prompts_max=prompts_max,
                queue_len_before=queue_len_before,
                run_total=self._run_total_tasks,
                completed=self._completed_tasks_in_run,
                last_known_queue_len=self._last_known_queue_len,
            )

            try:
                result = original_fn(task, send_cmd, *args, **kwargs)
            except Exception as exc:
                post_prompt_no, post_prompts_max, _ = self._read_queue_progress(state)
                self._debug_log(
                    "wrapper.generate_video.exception",
                    task_id=task_id,
                    error=str(exc),
                    post_prompt_no=post_prompt_no,
                    post_prompts_max=post_prompts_max,
                )
                self._log_event(
                    kind="error",
                    task_id=task_id,
                    prompt_no=post_prompt_no,
                    prompts_max=post_prompts_max,
                    details=str(exc),
                )
                self._update_progress_window_after_call(queue_len_before)
                raise

            post_prompt_no, post_prompts_max, _ = self._read_queue_progress(state)
            self._debug_log(
                "wrapper.generate_video.pre_log",
                task_id=task_id,
                pre_prompt_no=prompt_no,
                pre_prompts_max=prompts_max,
                post_prompt_no=post_prompt_no,
                post_prompts_max=post_prompts_max,
            )
            if result is True:
                self._log_event(
                    kind="success",
                    task_id=task_id,
                    prompt_no=post_prompt_no,
                    prompts_max=post_prompts_max,
                )
            else:
                detail = (
                    "generate_video returned False"
                    if result is False
                    else f"generate_video returned {result!r}"
                )
                self._log_event(
                    kind="failed",
                    task_id=task_id,
                    prompt_no=post_prompt_no,
                    prompts_max=post_prompts_max,
                    details=detail,
                )

            self._debug_log(
                "wrapper.generate_video.end",
                task_id=task_id,
                result=result,
                queue_len_before=queue_len_before,
                run_total=self._run_total_tasks,
                completed=self._completed_tasks_in_run,
                last_known_queue_len=self._last_known_queue_len,
            )
            self._update_progress_window_after_call(queue_len_before)
            return result

        wrapped_generate_video.__signature__ = inspect.signature(original_fn)
        wrapped_generate_video._wan2gp_notifier_wrapped = True
        wrapped_generate_video._wan2gp_notifier_original = original_fn

        self.set_global("generate_video", wrapped_generate_video)
        self._wrapped = True
        self._emit_system_event("generate_video wrapper installed.")
        self._debug_log("wrapper.generate_video.installed")

    def _install_process_tasks_wrapper_if_needed(self):
        if self._process_tasks_wrapped:
            return

        process_tasks_fn = getattr(self, "process_tasks", None)
        if not callable(process_tasks_fn):
            return

        if getattr(process_tasks_fn, "_wan2gp_notifier_process_wrapped", False):
            self._process_tasks_wrapped = True
            self._original_process_tasks = getattr(
                process_tasks_fn,
                "_wan2gp_notifier_process_original",
                process_tasks_fn,
            )
            return

        original_fn = process_tasks_fn
        self._original_process_tasks = original_fn

        @functools.wraps(original_fn)
        def wrapped_process_tasks(state, *args, **kwargs):
            queue_len = 0
            state_prompts_max = None
            state_prompt_no = None
            get_gen_info_fn = getattr(self, "get_gen_info", None)
            if callable(get_gen_info_fn):
                try:
                    gen = get_gen_info_fn(state)
                    queue = gen.get("queue", []) if isinstance(gen, dict) else []
                    queue_len = len(queue) if isinstance(queue, list) else 0
                    state_prompts_max = gen.get("prompts_max") if isinstance(gen, dict) else None
                    state_prompt_no = gen.get("prompt_no") if isinstance(gen, dict) else None
                except Exception:
                    queue_len = 0

            with self._progress_lock:
                self._last_known_queue_len = queue_len
                self._run_total_tasks = queue_len if queue_len > 0 else None
                self._completed_tasks_in_run = 0
                self._debug_log(
                    "wrapper.process_tasks.start",
                    queue_len=queue_len,
                    state_prompt_no=state_prompt_no,
                    state_prompts_max=state_prompts_max,
                    run_total=self._run_total_tasks,
                    completed=self._completed_tasks_in_run,
                    last_known_queue_len=self._last_known_queue_len,
                )

            return original_fn(state, *args, **kwargs)

        wrapped_process_tasks.__signature__ = inspect.signature(original_fn)
        wrapped_process_tasks._wan2gp_notifier_process_wrapped = True
        wrapped_process_tasks._wan2gp_notifier_process_original = original_fn

        self.set_global("process_tasks", wrapped_process_tasks)
        self._process_tasks_wrapped = True
        self._emit_system_event("process_tasks wrapper installed.")
        self._debug_log("wrapper.process_tasks.installed")

    def _install_queue_update_wrapper_if_needed(self):
        if self._queue_update_wrapped:
            return

        update_queue_data_fn = getattr(self, "update_queue_data", None)
        if not callable(update_queue_data_fn):
            return

        if getattr(update_queue_data_fn, "_wan2gp_notifier_queue_wrapped", False):
            self._queue_update_wrapped = True
            self._original_update_queue_data = getattr(
                update_queue_data_fn, "_wan2gp_notifier_queue_original", update_queue_data_fn
            )
            return

        original_fn = update_queue_data_fn
        self._original_update_queue_data = original_fn

        @functools.wraps(original_fn)
        def wrapped_update_queue_data(queue, *args, **kwargs):
            if isinstance(queue, list):
                qlen = len(queue)
                with self._progress_lock:
                    prev_run_total = self._run_total_tasks
                    prev_completed = self._completed_tasks_in_run
                    prev_last_known_queue_len = self._last_known_queue_len
                    self._last_known_queue_len = qlen
                    if self._run_total_tasks is None and qlen > 0:
                        self._run_total_tasks = qlen
                        self._completed_tasks_in_run = 0
                    elif self._run_total_tasks is not None and qlen > self._run_total_tasks:
                        self._run_total_tasks = qlen
                    if qlen <= 0:
                        self._run_total_tasks = None
                        self._completed_tasks_in_run = 0
                    self._debug_log(
                        "wrapper.update_queue_data.call",
                        qlen=qlen,
                        prev_run_total=prev_run_total,
                        prev_completed=prev_completed,
                        prev_last_known_queue_len=prev_last_known_queue_len,
                        run_total=self._run_total_tasks,
                        completed=self._completed_tasks_in_run,
                        last_known_queue_len=self._last_known_queue_len,
                    )
            return original_fn(queue, *args, **kwargs)

        wrapped_update_queue_data.__signature__ = inspect.signature(original_fn)
        wrapped_update_queue_data._wan2gp_notifier_queue_wrapped = True
        wrapped_update_queue_data._wan2gp_notifier_queue_original = original_fn

        self.set_global("update_queue_data", wrapped_update_queue_data)
        self._queue_update_wrapped = True
        self._emit_system_event("update_queue_data wrapper installed.")
        self._debug_log("wrapper.update_queue_data.installed")

    def _install_global_queue_ref_wrapper_if_needed(self):
        if self._global_queue_ref_update_wrapped:
            return

        update_global_queue_ref_fn = getattr(self, "update_global_queue_ref", None)
        if not callable(update_global_queue_ref_fn):
            return

        if getattr(update_global_queue_ref_fn, "_wan2gp_notifier_global_queue_wrapped", False):
            self._global_queue_ref_update_wrapped = True
            self._original_update_global_queue_ref = getattr(
                update_global_queue_ref_fn,
                "_wan2gp_notifier_global_queue_original",
                update_global_queue_ref_fn,
            )
            return

        original_fn = update_global_queue_ref_fn
        self._original_update_global_queue_ref = original_fn

        @functools.wraps(original_fn)
        def wrapped_update_global_queue_ref(queue, *args, **kwargs):
            if isinstance(queue, list):
                qlen = len(queue)
                with self._progress_lock:
                    prev_run_total = self._run_total_tasks
                    prev_completed = self._completed_tasks_in_run
                    prev_last_known_queue_len = self._last_known_queue_len
                    self._last_known_queue_len = qlen
                    if self._run_total_tasks is None and qlen > 0:
                        self._run_total_tasks = qlen
                        self._completed_tasks_in_run = 0
                    elif self._run_total_tasks is not None and qlen > self._run_total_tasks:
                        self._run_total_tasks = qlen
                    if qlen <= 0:
                        self._run_total_tasks = None
                        self._completed_tasks_in_run = 0
                    self._debug_log(
                        "wrapper.update_global_queue_ref.call",
                        qlen=qlen,
                        prev_run_total=prev_run_total,
                        prev_completed=prev_completed,
                        prev_last_known_queue_len=prev_last_known_queue_len,
                        run_total=self._run_total_tasks,
                        completed=self._completed_tasks_in_run,
                        last_known_queue_len=self._last_known_queue_len,
                    )
            return original_fn(queue, *args, **kwargs)

        wrapped_update_global_queue_ref.__signature__ = inspect.signature(original_fn)
        wrapped_update_global_queue_ref._wan2gp_notifier_global_queue_wrapped = True
        wrapped_update_global_queue_ref._wan2gp_notifier_global_queue_original = original_fn

        self.set_global("update_global_queue_ref", wrapped_update_global_queue_ref)
        self._global_queue_ref_update_wrapped = True
        self._emit_system_event("update_global_queue_ref wrapper installed.")
        self._debug_log("wrapper.update_global_queue_ref.installed")

    def _build_sink(self) -> NotifierSink:
        return MultiNotifierSink([ConsoleNotifierSink(), AppriseNotifierSink(self)])

    def _load_settings(self):
        defaults = copy.deepcopy(self.DEFAULT_SETTINGS)
        if not os.path.isfile(self._settings_path):
            return defaults

        try:
            with open(self._settings_path, "r", encoding="utf-8") as reader:
                loaded = json.load(reader)
            if not isinstance(loaded, dict):
                return defaults
            return self._merge_defaults(defaults, loaded)
        except Exception:
            return defaults

    def _merge_defaults(self, defaults: Dict[str, Any], loaded: Dict[str, Any]):
        merged = copy.deepcopy(defaults)
        for key, value in loaded.items():
            if key not in merged:
                continue
            if isinstance(merged[key], dict) and isinstance(value, dict):
                merged[key] = self._merge_defaults(merged[key], value)
            else:
                merged[key] = value
        return merged

    def _set_settings_snapshot(self, settings: Dict[str, Any], persist: bool = False):
        with self._settings_lock:
            self._settings = copy.deepcopy(settings)
            if persist:
                try:
                    with open(self._settings_path, "w", encoding="utf-8") as writer:
                        json.dump(self._settings, writer, indent=2, ensure_ascii=True)
                except Exception as exc:
                    self._emit_system_event(f"Could not save settings.json: {exc}")

    def _get_settings_snapshot(self):
        with self._settings_lock:
            return copy.deepcopy(self._settings)

    def _build_status_text(self, settings: Dict[str, Any]) -> str:
        enabled = bool(settings.get("enabled", True))
        provider = settings.get("provider", "telegram")
        url_preview = self._preview_url(settings)
        apprise_status = "installed" if self._is_apprise_available() else "not installed"
        status = "Enabled" if enabled else "Disabled"
        return (
            f"**Status:** {status}  \n"
            f"**Channel:** {provider}  \n"
            f"**Apprise:** {apprise_status}  \n"
            f"**URL (masked):** `{url_preview}`"
        )

    def _preview_url(self, settings: Dict[str, Any]) -> str:
        url = self._build_apprise_url_for_settings(settings)
        return self._mask_url_for_display(url, settings) if url else "(incomplete configuration)"

    def _mask_secret(self, value: str, keep_start: int = 3, keep_end: int = 2) -> str:
        if value is None:
            return ""
        token = str(value)
        if len(token) <= keep_start + keep_end:
            return "*" * max(1, len(token))
        return f"{token[:keep_start]}{'*' * (len(token) - keep_start - keep_end)}{token[-keep_end:]}"

    def _mask_url_for_display(self, url: str, settings: Dict[str, Any]) -> str:
        provider = str(settings.get("provider", "") or "")
        if not url:
            return ""

        if provider == "telegram":
            # tgram://<bot_token>/<chat>/
            prefix = "tgram://"
            if url.startswith(prefix):
                rest = url[len(prefix) :]
                parts = rest.split("/", 1)
                if parts:
                    parts[0] = self._mask_secret(parts[0])
                return prefix + "/".join(parts)
            return url

        if provider == "discord":
            # Mask token in .../webhooks/<id>/<token>
            parsed = urlparse(url)
            path_parts = parsed.path.split("/")
            if len(path_parts) >= 2:
                path_parts[-1] = self._mask_secret(path_parts[-1])
                masked_path = "/".join(path_parts)
                return urlunparse(parsed._replace(path=masked_path))
            return url

        if provider == "whatsapp":
            # whatsapp://<token>@<from>/<targets>/
            prefix = "whatsapp://"
            if url.startswith(prefix):
                rest = url[len(prefix) :]
                if "@" in rest:
                    token, suffix = rest.split("@", 1)
                    return f"{prefix}{self._mask_secret(token)}@{suffix}"
            return url

        if provider == "ifttt":
            # ifttt://<webhook_id>@<events>/...
            prefix = "ifttt://"
            if url.startswith(prefix):
                rest = url[len(prefix) :]
                if "@" in rest:
                    webhook_id, suffix = rest.split("@", 1)
                    return f"{prefix}{self._mask_secret(webhook_id)}@{suffix}"
            return url

        if provider == "google_chat":
            # https://...?...key=...&token=...
            parsed = urlparse(url)
            qs = parse_qs(parsed.query, keep_blank_values=True)
            changed = False
            for sensitive in ("key", "token"):
                if sensitive in qs and len(qs[sensitive]) > 0:
                    qs[sensitive][0] = self._mask_secret(qs[sensitive][0])
                    changed = True
            if changed:
                masked_query = urlencode(qs, doseq=True)
                return urlunparse(parsed._replace(query=masked_query))
            return url

        # Fallback: avoid accidental full secret exposure.
        return self._mask_secret(url, keep_start=8, keep_end=4)

    def _is_apprise_available(self) -> bool:
        try:
            import apprise  # noqa: F401

            return True
        except Exception:
            return False

    def _emit(self, event: Dict[str, Any]) -> None:
        try:
            self._sink.emit(event)
        except Exception as exc:
            fallback = ConsoleNotifierSink()
            fallback.emit(
                {
                    "type": "system.error",
                    "timestamp": time.time(),
                    "details": str(exc),
                    "message": f"[Notifier] sink error: {exc} | fallback_event={event}",
                }
            )

    def _emit_system_event(self, message: str) -> None:
        event = {
            "type": "system.info",
            "timestamp": time.time(),
            "message": f"[Notifier] {message}",
        }
        self._emit(event)

    def _emit_to_apprise(self, event: Dict[str, Any]) -> None:
        event_type = str(event.get("type", ""))
        if not event_type.startswith("task."):
            return

        ok, msg = self._send_apprise_notification(event)
        if not ok:
            self._emit_system_event(f"Apprise did not send message: {msg}")

    def _send_apprise_notification(self, event: Dict[str, Any], settings: Optional[Dict[str, Any]] = None):
        settings = settings if settings is not None else self._get_settings_snapshot()
        url = self._build_apprise_url_for_settings(settings)
        if not url:
            return False, "Apprise URL is incomplete for the selected channel."

        try:
            import apprise
        except Exception:
            if not self._apprise_import_warned:
                self._apprise_import_warned = True
                self._emit_system_event(
                    "Apprise is not installed. Install dependency 'apprise' for remote notifications."
                )
            return False, "Apprise not installed"

        app = apprise.Apprise()
        if not app.add(url):
            return False, "Invalid Apprise URL"

        title, body = self._build_apprise_message(event, settings)
        sent = bool(app.notify(title=title, body=body))
        if not sent:
            return False, "Apprise returned failure while notifying"
        return True, "ok"

    def _build_apprise_message(self, event: Dict[str, Any], settings: Dict[str, Any]):
        title = "Wan2GP Notifier"
        message = str(event.get("message", "")).strip()
        details = event.get("details")
        progress = event.get("progress")
        next_text = ""
        if isinstance(progress, dict):
            current = progress.get("current")
            total = progress.get("total")
            next_current = progress.get("next_current")
            if (
                current is not None
                and total is not None
                and next_current is not None
                and int(next_current) != int(current)
            ):
                next_text = f"{next_current}/{total}"
        lines = [message]
        if next_text:
            lines.append(f"next: {next_text}")
        if details:
            lines.append(f"details: {details}")
        body = "\n".join(lines)
        return title, body

    def _build_apprise_url_for_settings(self, settings: Dict[str, Any]) -> str:
        provider = settings.get("provider", "telegram")
        providers = settings.get("providers", {})
        cfg = providers.get(provider, {}) if isinstance(providers, dict) else {}

        if provider == "telegram":
            return self._build_telegram_url(cfg)
        if provider == "discord":
            return self._build_discord_url(cfg)
        if provider == "whatsapp":
            return self._build_whatsapp_url(cfg)
        if provider == "ifttt":
            return self._build_ifttt_url(cfg)
        if provider == "google_chat":
            return self._build_google_chat_url(cfg)

        return ""

    def _build_telegram_url(self, cfg: Dict[str, Any]) -> str:
        token = str(cfg.get("bot_token", "")).strip()
        chat_id = str(cfg.get("chat_id", "")).strip()
        if not token:
            return ""

        if chat_id:
            return f"tgram://{token}/{chat_id}/"
        return f"tgram://{token}/"

    def _build_discord_url(self, cfg: Dict[str, Any]) -> str:
        webhook_url = str(cfg.get("webhook_url", "")).strip()
        return webhook_url

    def _build_whatsapp_url(self, cfg: Dict[str, Any]) -> str:
        token = str(cfg.get("token", "")).strip()
        from_phone_id = str(cfg.get("from_phone_id", "")).strip()
        targets_raw = str(cfg.get("targets", "")).strip()

        if not token or not from_phone_id or not targets_raw:
            return ""

        targets = [t for t in re.split(r"[,;\s]+", targets_raw) if t]
        if not targets:
            return ""

        targets_path = "/".join(targets)
        return f"whatsapp://{token}@{from_phone_id}/{targets_path}/"

    def _build_ifttt_url(self, cfg: Dict[str, Any]) -> str:
        webhook_id = str(cfg.get("webhook_id", "")).strip()
        events_raw = str(cfg.get("events", "")).strip()
        query_params = str(cfg.get("query_params", "")).strip()

        if not webhook_id or not events_raw:
            return ""

        events = [e for e in re.split(r"[/,;\s]+", events_raw) if e]
        if not events:
            return ""

        event_path = "/".join(events)
        base = f"ifttt://{webhook_id}@{event_path}/"
        if query_params:
            qp = query_params[1:] if query_params.startswith("?") else query_params
            return f"{base}?{qp}"
        return base

    def _build_google_chat_url(self, cfg: Dict[str, Any]) -> str:
        webhook_url = str(cfg.get("webhook_url", "")).strip()
        return webhook_url

    def _extract_task_id(self, task):
        if isinstance(task, dict):
            return task.get("id", "unknown")
        return "unknown"

    def _read_queue_progress(self, state):
        queue_len = None
        queue_ref = getattr(self, "global_queue_ref", None)
        if isinstance(queue_ref, list):
            queue_len = len(queue_ref)

        state_prompt_no = None
        state_prompts_max = None
        state_queue_len = None
        state_gen = None

        if state is not None:
            get_gen_info_fn = getattr(self, "get_gen_info", None)
            if callable(get_gen_info_fn):
                try:
                    state_gen = get_gen_info_fn(state)
                except Exception:
                    state_gen = None

        if isinstance(state_gen, dict):
            state_prompt_no = state_gen.get("prompt_no")
            state_prompts_max = state_gen.get("prompts_max")
            queue_in_state = state_gen.get("queue")
            if isinstance(queue_in_state, list):
                state_queue_len = len(queue_in_state)

        live_queue_len = state_queue_len if isinstance(state_queue_len, int) else queue_len

        with self._progress_lock:
            total_hints = [self._last_known_queue_len]
            if isinstance(live_queue_len, int):
                total_hints.append(live_queue_len)
            try:
                if state_prompts_max is not None:
                    total_hints.append(int(state_prompts_max))
            except Exception:
                pass

            best_total_hint = max([h for h in total_hints if isinstance(h, int)] + [0])
            if best_total_hint <= 0:
                self._debug_log(
                    "progress.read.fallback_state",
                    queue_len=queue_len,
                    state_queue_len=state_queue_len,
                    state_prompt_no=state_prompt_no,
                    state_prompts_max=state_prompts_max,
                    live_queue_len=live_queue_len,
                    total_hints=total_hints,
                    best_total_hint=best_total_hint,
                    run_total=self._run_total_tasks,
                    completed=self._completed_tasks_in_run,
                    last_known_queue_len=self._last_known_queue_len,
                )
                return state_prompt_no, state_prompts_max, live_queue_len

            if self._run_total_tasks is None:
                self._run_total_tasks = best_total_hint
                self._completed_tasks_in_run = 0
            elif best_total_hint > self._run_total_tasks:
                self._run_total_tasks = best_total_hint

            total = int(self._run_total_tasks)
            current = self._completed_tasks_in_run + 1
            if current < 1:
                current = 1
            if current > total:
                current = total

        self._debug_log(
            "progress.read.derived",
            queue_len=queue_len,
            state_queue_len=state_queue_len,
            state_prompt_no=state_prompt_no,
            state_prompts_max=state_prompts_max,
            live_queue_len=live_queue_len,
            total_hints=total_hints,
            best_total_hint=best_total_hint,
            derived_current=current,
            derived_total=total,
            run_total=self._run_total_tasks,
            completed=self._completed_tasks_in_run,
            last_known_queue_len=self._last_known_queue_len,
        )
        return current, total, live_queue_len

    def _update_progress_window_after_call(self, queue_len_before: Optional[int]):
        with self._progress_lock:
            prev_run_total = self._run_total_tasks
            prev_completed = self._completed_tasks_in_run
            self._completed_tasks_in_run += 1
            if self._run_total_tasks is not None and self._completed_tasks_in_run >= self._run_total_tasks:
                self._run_total_tasks = None
                self._completed_tasks_in_run = 0
            self._debug_log(
                "progress.after_call",
                queue_len_before=queue_len_before,
                prev_run_total=prev_run_total,
                prev_completed=prev_completed,
                run_total=self._run_total_tasks,
                completed=self._completed_tasks_in_run,
                last_known_queue_len=self._last_known_queue_len,
            )

    def _format_progress(self, prompt_no, prompts_max):
        try:
            no = int(prompt_no)
            total = int(prompts_max)
            if no > 0 and total > 0:
                return f"{no}/{total}"
        except Exception:
            pass
        return None

    def _build_task_event(
        self,
        kind: str,
        task_id: Any,
        prompt_no: Optional[int],
        prompts_max: Optional[int],
        details: Optional[str] = None,
    ) -> Dict[str, Any]:
        progress_text = self._format_progress(prompt_no, prompts_max)
        progress_value = None
        if progress_text:
            current = int(prompt_no)
            total = int(prompts_max)
            progress_value = {"current": current, "total": total}
            if current < total:
                progress_value["next_current"] = current + 1

        message_suffix = f": {progress_text}" if progress_text else ""
        task_suffix = f" (task_id={task_id})"

        if kind == "success":
            event_type = "task.success"
            message = f"[Notifier] Video completed{message_suffix}{task_suffix}"
        elif kind == "error":
            event_type = "task.error"
            detail_suffix = f" reason={details}" if details else ""
            message = f"[Notifier] FAILED by exception{message_suffix}{task_suffix}{detail_suffix}"
        else:
            event_type = "task.failed"
            detail_suffix = f" detail={details}" if details else ""
            message = f"[Notifier] Not completed{message_suffix}{task_suffix}{detail_suffix}"

        return {
            "type": event_type,
            "timestamp": time.time(),
            "task_id": task_id,
            "progress": progress_value,
            "details": details,
            "message": message,
        }

    def _log_event(self, kind, task_id, prompt_no, prompts_max, details=None):
        settings = self._get_settings_snapshot()
        if not bool(settings.get("enabled", True)):
            return

        self._debug_log(
            "event.log",
            kind=kind,
            task_id=task_id,
            prompt_no=prompt_no,
            prompts_max=prompts_max,
            details=details,
        )
        event = self._build_task_event(
            kind=kind,
            task_id=task_id,
            prompt_no=prompt_no,
            prompts_max=prompts_max,
            details=details,
        )
        self._emit(event)

    def _debug_log(self, tag: str, **values):
        if not self._debug_enabled:
            return
        self._debug_counter += 1
        parts = [f"[Notifier][DBG#{self._debug_counter:04d}] {tag}"]
        for key, value in values.items():
            parts.append(f"{key}={value!r}")
        print(" | ".join(parts))
