'use strict';

/**
 * @callback OnError
 * @param {string} err - error message.
 */

/**
 * @typedef Conf
 * @type {object}
 * @property {string} url - WebSocket URL to connect to.
 * @property {HTMLVideoElement} videoElement - video element to play the stream.
 * @property {OnError} onError - called when there's an error.
 */

/** MSE (fMP4 over WebSocket) player. */
class MediaMTXMSEPlayer {
  /**
   * Create a MediaMTXMSEPlayer.
   * @param {Conf} conf - configuration.
   */
  constructor(conf) {
    this.conf = conf;
    this.state = 'idle';
    this.ws = null;
    this.mediaSource = null;
    this.sourceBuffer = null;
    this.queue = [];
    this.appending = false;
    this.retryPause = 2000;
    this.restartTimeout = null;
    this.pendingMime = null;
    this.retryId = 0; // increases on each retry to invalidate stale handlers
    
    this.start();
  }

  /**
   * Close the player and all its resources.
   */
  close() {
    this.state = 'closed';

    if (this.restartTimeout !== null) {
      clearTimeout(this.restartTimeout);
      this.restartTimeout = null;
    }

    if (this.ws !== null) {
      this.ws.close();
      this.ws = null;
    }

    if (this.mediaSource !== null) {
      if (this.mediaSource.readyState === 'open') {
        this.mediaSource.endOfStream();
      }
      this.mediaSource = null;
    }

    this.sourceBuffer = null;
    this.queue = [];
    this.appending = false;

    if (this.conf.videoElement.src) {
      URL.revokeObjectURL(this.conf.videoElement.src);
      this.conf.videoElement.src = '';
    }
  }

  /**
   * Start the player.
   */
  start() {
    if (this.state === 'closed') {
      return;
    }

    this.state = 'connecting';
    this.#setupMediaSource();
    this.#setupWebSocket();
  }

  #setupMediaSource() {
    try {
      this.mediaSource = new MediaSource();
      this.conf.videoElement.src = URL.createObjectURL(this.mediaSource);

      const myRetryId = this.retryId;

      this.mediaSource.addEventListener('sourceopen', () => {
        if (myRetryId !== this.retryId) {
          return; // stale
        }
        if (this.state === 'closed') {
          return;
        }
        // MediaSource is ready, waiting for WebSocket data
      });

      this.mediaSource.addEventListener('error', (e) => {
        if (myRetryId !== this.retryId) {
          return; // stale
        }
        console.error('MediaSource error', e);
        this.#handleError('MediaSource error', myRetryId);
      });

    } catch (err) {
      this.#handleError(`Failed to create MediaSource: ${err.message}`);
    }
  }

  #setupWebSocket() {
    try {
      this.ws = new WebSocket(this.conf.url);
      this.ws.binaryType = 'arraybuffer';

      const myRetryId = this.retryId;

      this.ws.onopen = () => {
        if (myRetryId !== this.retryId) {
          return; // stale
        }
        if (this.state === 'closed') {
          return;
        }
        this.state = 'running';
      };

      this.ws.onclose = () => {
        if (myRetryId !== this.retryId) {
          return; // stale
        }
        if (this.state === 'closed') {
          return;
        }
        this.#handleError('WebSocket connection closed', myRetryId);
      };

      this.ws.onerror = () => {
        if (myRetryId !== this.retryId) {
          return; // stale
        }
        if (this.state === 'closed') {
          return;
        }
        this.#handleError('WebSocket connection error', myRetryId);
      };

      this.ws.onmessage = (event) => {
        if (myRetryId !== this.retryId) {
          return; // stale
        }
        if (this.state === 'closed') {
          return;
        }
        // First message can be a string with a MIME hint from server
        if (typeof event.data === 'string') {
          const maybeMime = this.#parseMimeFromMessage(event.data);
          if (maybeMime) {
            this.pendingMime = maybeMime;
            // If MediaSource is already open and no SourceBuffer yet, we can
            // create it immediately; otherwise it will be created on first data
            if (!this.sourceBuffer && this.mediaSource && this.mediaSource.readyState === 'open') {
              try {
                this.#createSourceBuffer(this.pendingMime, myRetryId);
              } catch (e) {
                this.#handleError(`Failed to create SourceBuffer: ${e.message}`, myRetryId);
              }
            }
          }
          return;
        }
        // Binary fMP4 data
        this.#handleData(new Uint8Array(event.data));
      };

    } catch (err) {
      this.#handleError(`Failed to create WebSocket: ${err.message}`);
    }
  }

  #handleData(data) {
    try {
      if (!this.sourceBuffer) {
        this.#initializeSourceBuffer(data);
      } else {
        this.#queueData(data);
      }
    } catch (err) {
      this.#handleError(`Data handling error: ${err.message}`);
    }
  }

  #initializeSourceBuffer(initData) {
    if (this.mediaSource.readyState !== 'open') {
      this.#handleError('MediaSource not ready for initialization');
      return;
    }

    // Prefer server-provided MIME if available, otherwise try to detect
    const mime = this.pendingMime || this.#detectCodec(initData);
    
    if (!MediaSource.isTypeSupported(mime)) {
      this.#handleError('Codec not supported by this browser');
      return;
    }

    try {
      this.#createSourceBuffer(mime, this.retryId);

      this.#queueData(initData);
    } catch (err) {
      this.#handleError(`Failed to create SourceBuffer: ${err.message}`);
    }
  }

  #queueData(data) {
    this.queue.push(data);
    this.#appendNext();
    
    // Try to start playback
    if (this.conf.videoElement.paused) {
      this.conf.videoElement.play().catch(() => {
        // Autoplay might be blocked, which is fine
      });
    }
  }

  #appendNext() {
    if (!this.sourceBuffer || this.appending || this.queue.length === 0) {
      return;
    }

    this.appending = true;
    const data = this.queue.shift();
    
    try {
      this.sourceBuffer.appendBuffer(data);
    } catch (err) {
      console.error('appendBuffer error:', err);
      this.appending = false;
      this.#handleError(`Failed to append buffer: ${err.message}`);
    }
  }

  #createSourceBuffer(mime, creationRetryId) {
    if (!this.mediaSource || this.mediaSource.readyState !== 'open') {
      throw new Error('MediaSource not open');
    }
    this.sourceBuffer = this.mediaSource.addSourceBuffer(mime);
    //this.sourceBuffer.mode = 'segments';
    // Use sequence to prevent errors when there are missing timestamps in the video
    this.sourceBuffer.mode = 'sequence';

    const myRetryId = creationRetryId ?? this.retryId;

    this.sourceBuffer.addEventListener('updateend', () => {
      if (myRetryId !== this.retryId) {
        return; // stale
      }
      this.appending = false;
      this.#appendNext();
    });

    this.sourceBuffer.addEventListener('error', (e) => {
      if (myRetryId !== this.retryId) {
        return; // stale
      }
      console.error('SourceBuffer error', e);
      this.#handleError('SourceBuffer error', myRetryId);
    });
  }

  #detectCodec(initData) {
    // Naive detection: attempt common combinations
    // Browsers will reject unsupported ones
    const candidates = [
      'video/mp4; codecs="avc1.64001f, mp4a.40.2"',
      'video/mp4; codecs="avc1.42E01E, mp4a.40.2"',
      'video/mp4; codecs="hvc1.1.6.L93.B0, mp4a.40.2"',
      'video/mp4; codecs="av01.0.08M.08, mp4a.40.2"',
      'video/mp4; codecs="vp09.00.10.08, mp4a.40.2"'
    ];

    for (const candidate of candidates) {
      if (MediaSource.isTypeSupported(candidate)) {
        return candidate;
      }
    }

    // Fallback to a generic baseline
    return 'video/mp4; codecs="avc1.42E01E, mp4a.40.2"';
  }

  #parseMimeFromMessage(message) {
    try {
      const parsed = JSON.parse(message);
      if (parsed && typeof parsed.mime === 'string') {
        return parsed.mime;
      }
    } catch (_) {
      // Not JSON, allow raw MIME string as a fallback
      if (typeof message === 'string' && message.startsWith('video/')) {
        return message;
      }
    }
    return null;
  }

  #handleError(err, eventRetryId = this.retryId) {
    if (this.state === 'closed' || eventRetryId !== this.retryId) {
      return;
    }

    // Clean up current connection
    if (this.ws !== null) {
      this.ws.close();
      this.ws = null;
    }

    if (this.mediaSource !== null && this.mediaSource.readyState === 'open') {
      this.mediaSource.endOfStream();
    }

    this.sourceBuffer = null;
    this.queue = [];
    this.appending = false;

    if (this.state === 'connecting' || this.state === 'running') {
      this.state = 'restarting';

      // Bump retryId so previous handlers become stale
      const scheduledId = this.retryId + 1;
      this.retryId = scheduledId;

      this.restartTimeout = setTimeout(() => {
        // Only restart if still relevant
        if (this.restartTimeout) {
          this.restartTimeout = null;
        }
        if (this.state !== 'closed' && this.retryId === scheduledId) {
          this.start();
        }
      }, this.retryPause);

      if (this.conf.onError) {
        this.conf.onError(`${err}, retrying in ${this.retryPause / 1000} seconds`);
      }
    } else {
      this.state = 'failed';
      if (this.conf.onError) {
        this.conf.onError(err);
      }
    }
  }
}

window.MediaMTXMSEPlayer = MediaMTXMSEPlayer;