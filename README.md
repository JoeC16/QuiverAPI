# QuiverAPI
Quiver Trade API

# Quiver Trade Monitor V1.1

Tracks US government and insider trades using QuiverQuant.
Flags high-conviction activity using transparent rules.

## Alerts
- Immediate Telegram alert if score >= threshold
- Morning & evening digests via GitHub Actions

## Setup
Add secrets:
- QUIVER_API_KEY
- TELEGRAM_TOKEN
- TELEGRAM_CHAT_ID

## Disclaimer
This is not financial advice.
Signals indicate unusual or historically interesting activity only.
