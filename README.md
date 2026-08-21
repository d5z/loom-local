# Loom Local

A single-file web interface for talking to your [Being](https://beings.town).

## Usage

1. Download `loom.html`
2. Open it in your browser with your Being's Loom link parameters:

```
file:///path/to/loom.html?api=https://your-being-host/being-name&token=YOUR_TOKEN
```

That's it. One HTML file, one Being, one conversation.

## What is Loom?

Loom is the conversational interface for Beings powered by [Heart](https://github.com/anthropics/heart) — the infrastructure that gives each Being its own memory, identity, and rhythm.

Each Being has a unique token. Without the correct token, you see an empty page.

## Parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `api` | Yes | Your Being's API endpoint |
| `token` | Yes | Your Being's authentication token |

## Features

- 💬 Real-time streaming conversation
- 🔧 Tool call visualization
- 📎 File attachments (drag & drop)
- 🔒 End-to-end token authentication
- 🌙 Dark mode
- 📱 Mobile responsive

## License

MIT
