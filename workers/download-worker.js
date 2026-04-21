// MuxNet Download Worker
// Runs in parallel — downloads assigned video + audio segments with retry.
//
// Fixes applied:
//   - postMessage now uses transfer list for ArrayBuffers (zero memory copy).
//     Old code sent data without transfer: [data], causing a full copy of every
//     segment. For a 754-segment, ~3 GB download this added ~3 GB of extra copies.
//   - segment_complete message now includes segType ('video'|'audio') so engine.js
//     can correctly route segments into videoResult vs audioResult arrays.
//   - Chunk field names aligned with engine.js: videoStartIndex / audioStartIndex
//     (was just startIndex which was ambiguous for mixed video+audio chunks).

'use strict';

const MAX_RETRIES = 3;
const RETRY_DELAY = 1000;

let speedBytes     = 0;
let lastSpeedUpdate = Date.now();

self.addEventListener('message', async (event) => {
  const { type, chunk } = event.data;
  if (type === 'start') await downloadChunk(chunk);
});

async function downloadChunk(chunk) {
  const {
    video,           // string[] — video segment URLs for this worker
    audio,           // string[] — audio segment URLs for this worker
    videoStartIndex, // global index of video[0]
    audioStartIndex, // global index of audio[0]
    workerId
  } = chunk;

  try {
    // ── Download video segments ──────────────────────────────────────────────
    for (let i = 0; i < video.length; i++) {
      const url   = video[i];
      const index = videoStartIndex + i;

      try {
        const data = await downloadWithRetry(url);

        // Fix: transfer the ArrayBuffer — eliminates a full memory copy per segment.
        // The worker relinquishes ownership; engine.js receives it with zero copy.
        self.postMessage(
          {
            type:    'segment_complete',
            data:    { segType: 'video', index, data, url }
          },
          [data] // transfer list
        );

        speedBytes += data.byteLength;
        updateSpeed();

      } catch (error) {
        console.error(`[Worker ${workerId}] Video seg ${index} failed:`, error.message);
        self.postMessage({
          type: 'segment_failed',
          data: { segType: 'video', index, error: error.message }
        });
      }
    }

    // ── Download audio segments ──────────────────────────────────────────────
    for (let i = 0; i < audio.length; i++) {
      const url   = audio[i];
      const index = audioStartIndex + i;

      try {
        const data = await downloadWithRetry(url);

        self.postMessage(
          {
            type: 'segment_complete',
            data: { segType: 'audio', index, data, url }
          },
          [data] // transfer list
        );

        speedBytes += data.byteLength;
        updateSpeed();

      } catch (error) {
        console.error(`[Worker ${workerId}] Audio seg ${index} failed:`, error.message);
        self.postMessage({
          type: 'segment_failed',
          data: { segType: 'audio', index, error: error.message }
        });
      }
    }

    self.postMessage({
      type: 'complete',
      data: { workerId, segmentCount: video.length + audio.length }
    });

  } catch (error) {
    self.postMessage({
      type: 'error',
      data: { message: error.message, workerId }
    });
  }
}

async function downloadWithRetry(url) {
  let lastError;

  for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
    try {
      const response = await fetch(url, {
        method:      'GET',
        cache:       'no-cache',
        credentials: 'include'
      });

      if (!response.ok) {
        if (response.status === 404) throw new Error(`Not found (404): ${url}`);
        if (response.status === 403) throw new Error(`Access denied (403): ${url}`);
        throw new Error(`HTTP ${response.status}`);
      }

      return await response.arrayBuffer();

    } catch (error) {
      lastError = error;
      console.warn(`[Worker] Attempt ${attempt + 1}/${MAX_RETRIES} failed: ${error.message}`);
      if (attempt < MAX_RETRIES - 1) {
        await new Promise(resolve => setTimeout(resolve, RETRY_DELAY * (attempt + 1)));
      }
    }
  }

  throw lastError;
}

function updateSpeed() {
  const now     = Date.now();
  const elapsed = (now - lastSpeedUpdate) / 1000;

  if (elapsed >= 1) {
    const speed = speedBytes / elapsed;
    self.postMessage({
      type: 'progress',
      data: { downloaded: speedBytes, speed }
    });
    speedBytes     = 0;
    lastSpeedUpdate = now;
  }
}
