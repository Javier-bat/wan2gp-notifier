# wan2gp-notifier

Plugin that logs queue task status and can send notifications via Apprise.

## Features

- Queue completion/failure console logs
- Apprise notification delivery
- UI tab (`Notifier`) to:
  - enable/disable notifications
  - choose provider (`telegram`, `discord`, `whatsapp`, `ifttt`, `google_chat`)
  - save and test notification config
- Persistent plugin settings in `settings.json`

## Discord

## Telegram


## Notes

- If Apprise is missing, install plugin dependencies (`requirements.txt`) in the app environment.
- This folder is a standalone plugin package. To load it as a local plugin in this repo,
  place it under `plugins/` or install it through the Plugin Manager from a git URL.
