# wan2gp-notifier

Plugin that logs queue task status and can send notifications via Apprise.

## Compatibility

- Current WanGP builds are monitored through saved output registration.
- Completion is detected when WanGP records a saved image, video, or audio output, which covers Gradio callbacks captured before plugin wrappers are installed.
- Older builds that still expose `generate_video` remain supported as a fallback.

## Features

- Queue completion/failure console logs
- Optional generated image/video/audio attachments in completion notifications
- Telegram uploads use extended Apprise socket timeouts and skip attachments above Telegram Bot API's 50 MB upload limit.
- Per-task percentage alerts during generation (configurable, e.g. every 10%)
- Apprise notification delivery
- UI tab (`Notifier`) to:
  - enable/disable notifications
  - enable/disable sending generated media files
  - enable/disable percentage alerts and choose alert interval (1-99%)
  - choose provider (`telegram`, `discord`, `whatsapp`, `ifttt`, `google_chat`)
  - save and test notification config
- Persistent plugin settings in `settings.json`

## Discord
<img width="461" height="343" alt="image" src="https://github.com/user-attachments/assets/edad5b94-d6a5-4295-8d27-8782f61a5378" />

## Telegram
<img width="413" height="316" alt="image" src="https://github.com/user-attachments/assets/d1fca704-546c-4767-9174-e866c7819943" />


## Notes

- If Apprise is missing, install plugin dependencies (`requirements.txt`) in the app environment.
- Use `test_send_generation.py` with the WanGP Python environment to test one media upload without generating a new file.
- This folder is a standalone plugin package. To load it as a local plugin in this repo,
  place it under `plugins/` or install it through the Plugin Manager from a git URL.
