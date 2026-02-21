
// MuxNet Download Worker
// Runs in parallel to download segments with retry and error handling

'use strict';

const MAX_RETRIES = 3;
const RETRY_DELAY = 1000;

let currentChunk = null;
let downloadedBytes = 0;
let lastSpeedUpdate = Date.now();
let speedBytes = 0;

self.addEventListener('message', async (event) => {
  const { type, chunk } = event.data;

  if (type === 'start') {
    currentChunk = chunk;
    await downloadChunk(chunk);
  }
});

async function downloadChunk(chunk) {
  const { video, audio, startIndex, workerId } = chunk;
  const segments = [];

  try {
    // Download video segments
    for (let i = 0; i < video.length; i++) {
      const url = video[i];
      const index = startIndex + i;

      try {
        const data = await downloadSegmentWithRetry(url, 'video');
        
        segments.push({
          type: 'video',
          index,
          data,
          url
        });

        // Report progress
        downloadedBytes += data.byteLength;
        speedBytes += data.byteLength;

        self.postMessage({
          type: 'segment_complete',
          data: {
            type: 'video',
            index,
            data,
            url
          }
        });

        updateSpeed();

      } catch (error) {
        console.error(`[Worker ${workerId}] Video segment ${index} failed:`, error);
        self.postMessage({
          type: 'segment_failed',
          data: {
            type: 'video',
            index,
            error: error.message
          }
        });
      }
    }

    // Download audio segments
    for (let i = 0; i < audio.length; i++) {
      const url = audio[i];
      const index = startIndex + i;

      try {
        const data = await downloadSegmentWithRetry(url, 'audio');
        
        segments.push({
          type: 'audio',
          index,
          data,
          url
        });

        downloadedBytes += data.byteLength;
        speedBytes += data.byteLength;

        self.postMessage({
          type: 'segment_complete',
          data: {
            type: 'audio',
            index,
            data,
            url
          }
        });

        updateSpeed();

      } catch (error) {
        console.error(`[Worker ${workerId}] Audio segment ${index} failed:`, error);
        self.postMessage({
          type: 'segment_failed',
          data: {
            type: 'audio',
            index,
            error: error.message
          }
        });
      }
    }

    // Report completion
    self.postMessage({
      type: 'complete',
      data: {
        workerId,
        downloadedBytes,
        segmentCount: segments.length
      }
    });

  } catch (error) {
    self.postMessage({
      type: 'error',
      data: {
        message: error.message,
        workerId
      }
    });
  }
}

async function downloadSegmentWithRetry(url, type) {
  let lastError;

  for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
    try {
      const response = await fetch(url, {
        method: 'GET',
        cache: 'no-cache',
        credentials: 'include'
      });

      if (!response.ok) {
        if (response.status === 404) {
          throw new Error(`Segment not found (404): ${url}`);
        }
        if (response.status === 403) {
          throw new Error(`Access denied (403): ${url}`);
        }
        throw new Error(`HTTP ${response.status}`);
      }

      const data = await response.arrayBuffer();
      return data;

    } catch (error) {
      lastError = error;
      console.warn(`[Worker] Attempt ${attempt + 1}/${MAX_RETRIES} failed:`, error.message);

      if (attempt < MAX_RETRIES - 1) {
        await new Promise(resolve => setTimeout(resolve, RETRY_DELAY * (attempt + 1)));
      }
    }
  }

  throw lastError;
}

function updateSpeed() {
  const now = Date.now();
  const elapsed = (now - lastSpeedUpdate) / 1000;

  if (elapsed >= 1) {
    const speed = speedBytes / elapsed;

    self.postMessage({
      type: 'progress',
      data: {
        downloaded: speedBytes,
        speed
      }
    });

    speedBytes = 0;
    lastSpeedUpdate = now;
  }
}
