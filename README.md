
# Vaultcord

<p align="center">
  <img src="vaultcordlogo.png" width="96" alt="Vaultcord Logo">
</p>

<p align="center">
  <b>Discord Server Archive & Exporter</b><br>
  Export Discord servers into beautiful self-contained HTML archives.
</p>

---

## Features

- Archive accessible Discord server data
- Export channels, messages, members, roles, and emojis
- Beautiful standalone HTML output
- Automatic foreign-language translation to English
- Attachment and embed support
- Fast parallel message scraping
- No external database required
- Single-file portable executable support

---

## What Vaultcord Collects

Vaultcord can archive:

- Server information
- Channels and categories
- Message history
- Members
- Roles
- Emojis
- Attachments metadata
- Embeds metadata

The generated archive is saved as a single HTML file that can be opened in any browser.

---

## Screenshots

Add screenshots here later.

```md
![Overview](screenshots/overview.png)
![Messages](screenshots/messages.png)
```

---

# Installation

## Option 1 — Run from Source (Recommended)

### Requirements

- Python 3.10+
- Internet connection
- Discord account or bot token

### Clone the Repository

```bash
git clone https://github.com/YOUR_USERNAME/vaultcord.git
cd vaultcord
```

### Run Vaultcord

```bash
python vaultcord.py
```

---

## Option 2 — Build an Executable

Install PyInstaller:

```bash
pip install pyinstaller
```

Build:

```bash
python -m PyInstaller --onefile --clean --noupx --icon=favicon.ico --name vaultcord vaultcord.py
```

The executable will appear in:

```text
dist/vaultcord.exe
```

---

# Usage

Run the tool:

```bash
python vaultcord.py
```

You will be prompted for:

1. Discord token
2. Server (Guild) ID
3. Message limit
4. Translation toggle

Example:

```text
🔑 Token: Bot xxxxxxxxx
🏠 Server (Guild) ID: 123456789012345678
📨 Messages per channel: 500
🌐 Translate foreign messages to English? y
```

---

# Getting a Discord Token

## Bot Token (Recommended)

1. Go to:
   https://discord.com/developers/applications

2. Create a new application

3. Open the **Bot** tab

4. Click:
   - "Add Bot"
   - "Reset Token"

5. Copy the token

Use it like:

```text
Bot YOUR_TOKEN_HERE
```

### Required Permissions

Recommended permissions:

- View Channels
- Read Message History

---

## User Tokens

User tokens are technically supported but may violate Discord Terms of Service.

Use at your own risk.

---

# Finding a Server ID

Enable Developer Mode in Discord:

- User Settings
- Advanced
- Developer Mode

Then:

- Right click the server
- Click "Copy Server ID"

---

# Translation

Vaultcord can automatically detect and translate foreign-language messages into English using Google Translate.

Translated messages appear inline in the archive.

---

# Output

Vaultcord generates a standalone HTML archive:

```text
vaultcord_MyServer_20260515_143012.html
```

Features include:

- Searchable members
- Searchable messages
- Organized channels
- Responsive layout
- Offline viewing

---

# SmartScreen / Smart App Control Warnings

Windows may warn about unsigned executables built with PyInstaller.

This is normal for newly built or unsigned applications.

To reduce warnings:

- Build with `--noupx`
- Publish source code publicly
- Use code signing certificates
- Distribute consistent binaries

---

# Security Notice

Vaultcord does not upload archived data anywhere.

All archives are generated locally on your machine.

Always protect exported archives because they may contain sensitive server content.

---

# Legal & Ethical Use

Only archive servers you own or have permission to archive.

Respect:

- Discord Terms of Service
- Server privacy
- Local laws and regulations

The developers of Vaultcord are not responsible for misuse.

---

# Troubleshooting

## "Invalid token"

- Ensure the token is correct
- Bot tokens must include:
  
```text
Bot 
```

prefix.

---

## Empty channels

Your bot/account likely lacks permission to:

- View Channel
- Read Message History

---

## SmartScreen blocks the EXE

Run from source instead:

```bash
python vaultcord.py
```

Or code-sign the executable.

---

# Contributing

Pull requests and improvements are welcome.

Ideas:

- Better media embedding
- Full attachment downloading
- Incremental archiving
- SQLite export
- Theme customization

---

# License

MIT License

---

# Disclaimer

Vaultcord is an independent project and is not affiliated with or endorsed by Discord Inc.
