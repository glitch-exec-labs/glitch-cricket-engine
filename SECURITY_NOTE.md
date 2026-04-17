# SECURITY NOTE — read before reactivating

**Date:** 2026-04-17

This project is currently dormant. Before picking it back up, you MUST create fresh credentials. Do NOT reuse the values that used to live in this repo — they were leaked publicly and are compromised.

## What was leaked

| Secret | Where it used to live | Status |
|---|---|---|
| Telegram `API_ID` + `API_HASH` | `auth_liveline.py` (HEAD) | Removed from HEAD. Still visible in git history. **Treat as compromised.** |
| Sportmonks API key | `WINDOWS_VM_SETUP.md` (historical commits `35a9c81`, `269eac4`, `4ad9c17`) | Already scrubbed from HEAD but still visible in git history. **Treat as compromised.** |

## Reactivation checklist

1. Revoke the old Telegram app at https://my.telegram.org/apps (if not already).
2. Revoke the old Sportmonks key at https://my.sportmonks.com/apis (if not already).
3. Create fresh keys.
4. Put them in a `.env` (gitignored) — never back in source.
5. Required env vars for `auth_liveline.py`:
   - `TELEGRAM_API_ID`
   - `TELEGRAM_API_HASH`
   - `LIVELINE_CHANNEL` (the private channel invite link)
6. Required env var for Sportmonks-using code: `SPORTMONKS_API_KEY`.

## Optional: scrub git history

The old secrets are still visible in the commit history of this public repo. Rotation is what saves you, not the scrub — but if you want to remove them from history too, run:

```bash
pip install git-filter-repo
git filter-repo --path WINDOWS_VM_SETUP.md --invert-paths   # removes the file from all history
# then re-add a clean version of WINDOWS_VM_SETUP.md
git push --force origin --all --tags
```

Then open a GitHub Support ticket asking them to purge cached views.
