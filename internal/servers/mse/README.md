# MediaMTX MSE Player

The MediaMTXMSEPlayer is a JavaScript class that provides an easy-to-use interface for playing fMP4 streams over WebSocket using Media Source Extensions (MSE).

## Features

- Automatic codec detection and fallback
- Built-in error handling and automatic reconnection
- Clean resource management
- Event-driven architecture similar to WebRTC player

## Basic Usage

```javascript
// Create a new MSE player
const player = new MediaMTXMSEPlayer({
  url: 'ws://localhost:8890/mystream',
  videoElement: document.getElementById('video'),
  onError: (err) => {
    console.error('Player error:', err);
  }
});

// Later, close the player to clean up resources
player.close();
```

## Configuration Options

The constructor accepts a configuration object with the following properties:

- `url` (string): WebSocket URL to connect to (e.g., `ws://localhost:8890/streamname`)
- `videoElement` (HTMLVideoElement): The video element where the stream will be played
- `onError` (function): Callback function called when errors occur

## Methods

### `close()`
Closes the player and cleans up all resources including WebSocket connection, MediaSource, and SourceBuffer.

## Error Handling

The player automatically handles various error scenarios:

- WebSocket connection errors
- MediaSource/SourceBuffer errors  
- Codec compatibility issues
- Network disconnections

When an error occurs during normal operation, the player will:
1. Clean up the current connection
2. Wait for a retry period (2 seconds by default)
3. Automatically attempt to reconnect
4. Call the `onError` callback with a descriptive message

## Examples

### Basic Integration
```html
<!DOCTYPE html>
<html>
<head>
  <title>MSE Player Example</title>
</head>
<body>
  <video id="video" controls muted autoplay></video>
  <script src="player.js"></script>
  <script>
    const player = new MediaMTXMSEPlayer({
      url: 'ws://localhost:8890/mystream',
      videoElement: document.getElementById('video'),
      onError: (err) => alert('Error: ' + err)
    });
  </script>
</body>
</html>
```

### Advanced Usage with State Management
```javascript
let player = null;

function connect(streamUrl) {
  if (player) {
    player.close();
  }
  
  player = new MediaMTXMSEPlayer({
    url: streamUrl,
    videoElement: document.getElementById('video'),
    onError: handleError
  });
}

function disconnect() {
  if (player) {
    player.close();
    player = null;
  }
}

function handleError(err) {
  console.error('MSE Player error:', err);
  updateStatus('Error: ' + err);
}

// Clean up on page unload
window.addEventListener('beforeunload', () => {
  if (player) {
    player.close();
  }
});
```

## Browser Compatibility

The player requires:
- WebSocket support
- Media Source Extensions (MSE) support
- Support for fMP4 containers

This covers all modern browsers including Chrome, Firefox, Safari, and Edge.

## Available Demo Pages

- `/` - Simple player that connects to the current path as stream name
- `/example` - Interactive demo with URL input and connection controls

## Codec Support

The player automatically detects and supports:
- H.264/AVC video with AAC audio
- H.265/HEVC video with AAC audio (where supported)
- AV1 video with AAC audio (where supported) 
- VP9 video with AAC audio (where supported)

Falls back to H.264 baseline profile if no other codecs are supported.