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

  #handleError(err) {
    if (this.state === 'closed') {
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

      this.restartTimeout = setTimeout(() => {
        this.restartTimeout = null;
        if (this.state !== 'closed') {
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

  #setupMediaSource() {
    try {
      this.mediaSource = new MediaSource();
      this.conf.videoElement.src = URL.createObjectURL(this.mediaSource);

      this.mediaSource.addEventListener('sourceopen', () => {
        if (this.state === 'closed') {
          return;
        }
        // MediaSource is ready, waiting for WebSocket data
      });

      this.mediaSource.addEventListener('error', () => {
        this.#handleError('MediaSource error');
      });

    } catch (err) {
      this.#handleError(`Failed to create MediaSource: ${err.message}`);
    }
  }

  #setupWebSocket() {
    try {
      this.ws = new WebSocket(this.conf.url);
      this.ws.binaryType = 'arraybuffer';

      this.ws.onopen = () => {
        if (this.state === 'closed') {
          return;
        }
        this.state = 'running';
      };

      this.ws.onclose = () => {
        if (this.state === 'closed') {
          return;
        }
        this.#handleError('WebSocket connection closed');
      };

      this.ws.onerror = () => {
        if (this.state === 'closed') {
          return;
        }
        this.#handleError('WebSocket connection error');
      };

      this.ws.onmessage = (event) => {
        if (this.state === 'closed') {
          return;
        }
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

    const mime = this.#detectCodec(initData);
    
    if (!MediaSource.isTypeSupported(mime)) {
      this.#handleError('Codec not supported by this browser');
      return;
    }

    try {
      this.sourceBuffer = this.mediaSource.addSourceBuffer(mime);
      this.sourceBuffer.mode = 'segments';
      
      this.sourceBuffer.addEventListener('updateend', () => {
        this.appending = false;
        this.#appendNext();
      });

      this.sourceBuffer.addEventListener('error', () => {
        this.#handleError('SourceBuffer error');
      });

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
}

window.MediaMTXMSEPlayer = MediaMTXMSEPlayer;