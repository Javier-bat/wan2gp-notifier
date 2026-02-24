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
<img width="461" height="343" alt="image" src="https://github.com/user-attachments/assets/edad5b94-d6a5-4295-8d27-8782f61a5378" />

## Telegram
<img width="413" height="316" alt="image" src="https://github.com/user-attachments/assets/d1fca704-546c-4767-9174-e866c7819943" />


## Notes

- If Apprise is missing, install plugin dependencies (`requirements.txt`) in the app environment.
- This folder is a standalone plugin package. To load it as a local plugin in this repo,
  place it under `plugins/` or install it through the Plugin Manager from a git URL.
