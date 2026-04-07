# ICAI Batch Monitor 🎓

Automatically monitors the [ICAI Online Registration Portal](https://www.icaionlineregistration.org/launchbatchdetail.aspx) and sends you an **email alert** the moment a new batch is detected for your Region / PoU / Course combination.

Runs every **10 minutes** via GitHub Actions — completely free on public repos.

---

## How It Works

```
GitHub Actions (every 10 min)
        ↓
scraper.py → navigates ICAI site (Region → PoU → Course → Batches)
        ↓
Compares with last saved state (state.json in repo)
        ↓
If changed → notifier.py sends email alert
        ↓
Updates state.json and commits it back to repo
```

---

## Setup (one-time, ~10 minutes)

### Step 1 — Fork or clone this repo

Create a **public** GitHub repository with these files (public = unlimited free Actions minutes).

### Step 2 — Set your preferences

Open `monitor.py` and edit the three lines at the top:

```python
REGION = "Western"
POU    = "Pune"
COURSE = "AICITSS - Advanced Information Technology"
```

Available courses:
- `Advanced (ICITSS) MCS Course`
- `Advanced (ICITSS) MCS Course - Weekend`
- `AICITSS - Advanced Information Technology`
- `ICITSS - Information Technology`
- `ICITSS - Orientation Course`

To watch **multiple** courses simultaneously, add them to `WATCHLIST`:

```python
WATCHLIST = [
    {"region": "Western", "pou": "Pune", "course": "AICITSS - Advanced Information Technology"},
    {"region": "Western", "pou": "Pune", "course": "Advanced (ICITSS) MCS Course"},
]
```

### Step 3 — Create a Gmail App Password

> This is NOT your Gmail login password. It's a special password for apps.

1. Go to your Google Account → **Security**
2. Enable **2-Step Verification** (if not already on)
3. Go to **Security → App Passwords**
4. Create one → App: `Mail`, Device: `Windows Computer` (label doesn't matter)
5. Google gives you a **16-character code** → copy it

### Step 4 — Add GitHub Secrets

In your GitHub repo → **Settings → Secrets and variables → Actions → New repository secret**

Add these three secrets:

| Secret Name     | Value                                |
|-----------------|--------------------------------------|
| `GMAIL_USER`    | your Gmail address (e.g. `you@gmail.com`) |
| `GMAIL_APP_PASS`| the 16-char App Password from Step 3 |
| `ALERT_EMAIL`   | email to receive alerts (can be same as `GMAIL_USER`, or your phone's SMS-to-email) |

### Step 5 — Test the email

In your repo → **Actions → ICAI Batch Monitor → Run workflow → mode: test-email → Run**

You should receive a test alert email within a minute. If you do, everything is working.

### Step 6 — Let it run

The monitor activates automatically. Every 10 minutes, GitHub Actions will:
1. Scrape the ICAI site for your preferences
2. Compare with the last known state (stored in `state.json`)
3. Email you if anything changed
4. Update and commit `state.json`

---

## Monitoring Multiple People

Each person can fork the repo and set their own preferences + secrets. Or you can add multiple entries to `WATCHLIST` in `monitor.py` — all alerts go to `ALERT_EMAIL`.

---

## Troubleshooting

### "No batches found" but batches exist on the site
The ICAI site uses ASP.NET WebForms — the scraper replicates the dropdown postback chain. If the site changes its HTML structure, the auto-discovery logic may need updating. Open an issue.

### Email not sending
- Double-check the Gmail App Password (not your login password)
- Make sure 2FA is enabled on your Google account
- Run `debug` mode to confirm scraping works: Actions → Run workflow → mode: debug

### Actions not running
- Check that the repo is **public** (private repos have a 2000 min/month limit)
- GitHub occasionally delays scheduled workflows by a few minutes — this is normal

---

## Files

```
icai-monitor/
├── .github/
│   └── workflows/
│       └── monitor.yml      ← GitHub Actions cron (every 10 min)
├── scraper.py               ← ASP.NET postback chain + batch parser
├── notifier.py              ← Gmail SMTP email alert
├── monitor.py               ← Main entry point + your preferences
├── state.json               ← Persisted batch state (auto-updated)
├── requirements.txt
└── README.md
```

---

## Next Steps / Upgrades

- [ ] **Telegram bot** — instead of email, get Telegram push notifications
- [ ] **Multi-user** — build a simple web form where any CA student enters their preferences
- [ ] **WhatsApp** — Twilio or meta-cloud-api integration

---

*Built to solve a real problem: CA students missing batch openings because there's no alert system on the ICAI portal.*
