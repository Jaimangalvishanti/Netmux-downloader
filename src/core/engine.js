// MuxNet Engine - Main Orchestrator
// Handles job initialization, worker coordination, FFmpeg mux, and download flow
//
// Fixes applied:
//   - distributeSegments: was slicing audio at same indices as video (breaks when
//     audio.length ≠ video.length). Now video and audio distributed independently.
//   - muxAndSave: now prepends fMP4 init segments (initSegmentUrl/audioInitUrl)
//     as the first entry in each concat list. Without this, fMP4 streams produce
//     undecodable output — the moov/ftyp box lives in the init segment.
//   - downloadChunk: ArrayBuffers now transferred (not copied) via postMessage
//     transfer list — eliminates ~3 GB of memory copies per download.
//   - muxAndSave: FFmpeg virtual FS cleaned up after use.
//   - muxAndSave: FFmpeg command uses explicit -map flags when both tracks present
//     to avoid FFmpeg auto-selecting wrong streams.
//   - All UI element accesses guarded against null.

export class MuxNetEngine {
  constructor() {
    this.jobId               = null;
    this.jobData             = null;
    this.workers             = [];
    this.workerCount         = Math.min(Math.max(navigator.hardwareConcurrency || 4, 4), 32);
    this.ffmpeg              = null;
    this.startTime           = null;
    this.downloadedBytes     = 0;
    this.totalEstimatedBytes = 0;
    this.speedSamples        = [];
    this.isPaused            = false;
    this.isCancelled         = false;
    this.completedSegments   = 0;
    this.failedSegments      = 0;
  }

  async start() {
    try {
      const params = new URLSearchParams(window.location.search);
      this.jobId   = params.get('job');

      if (!this.jobId) {
        this.showError('No job ID provided. Please start download from extension.');
        return;
      }

      this.log('Initializing MuxNet engine...', 'info');
      this.updateUI({ jobId: `Job: ${this.jobId}` });

      await this.loadJobData();
      await this.initFFmpeg();
      const segments = await this.parseSegments();
      await this.startDownload(segments);

    } catch (error) {
      console.error('[MuxNet] Engine error:', error);
      this.showError(`Engine failed: ${error.message}`);
    }
  }

  // ── Job loading ─────────────────────────────────────────────────────────────

  async loadJobData() {
    this.log('Loading job data from extension...', 'info');

    if (typeof chrome !== 'undefined' && chrome.storage) {
      try {
        const key    = `muxnet_job_${this.jobId}`;
        const result = await chrome.storage.local.get(key);
        if (result[key]) {
          this.jobData = result[key];
          this.log(`Job loaded: "${this.jobData.title || 'Stream'}"`, 'success');
          this.updateUI({
            filename:          this.jobData.title || 'Stream',
            statusTitle:       'Job data loaded',
            statusDescription: `${this.jobData.segments?.video?.length || 0} video + ` +
                               `${this.jobData.segments?.audio?.length || 0} audio segments`
          });
          return;
        }
      } catch (error) {
        console.warn('[MuxNet] chrome.storage unavailable:', error.message);
      }
    }

    // Fallback: localStorage (standalone / test mode)
    try {
      const localData = localStorage.getItem(`muxnet_job_${this.jobId}`);
      if (localData) {
        this.jobData = JSON.parse(localData);
        this.log('Job loaded from localStorage (test mode)', 'info');
        this.updateUI({
          filename:          this.jobData.title || 'Test Stream',
          statusTitle:       'Test mode',
          statusDescription: 'Using localStorage for job data'
        });
        return;
      }
    } catch (error) {
      console.warn('[MuxNet] localStorage fallback failed:', error.message);
    }

    throw new Error('Job data not found. Please restart download from extension.');
  }

  // ── FFmpeg init ─────────────────────────────────────────────────────────────

  async initFFmpeg() {
    this.log('Loading FFmpeg.wasm (~30 MB, cached after first use)...', 'info');
    this.updateUI({
      statusTitle:       'Loading FFmpeg.wasm',
      statusDescription: 'First-time setup, ~30 MB download'
    });

    const { FFmpeg } = FFmpegWASM;
    this.ffmpeg = new FFmpeg();

    this.ffmpeg.on('log', ({ message }) => {
      // Only log non-version lines to avoid console spam
      if (message && !message.startsWith('ffmpeg version')) {
        console.log('[FFmpeg]', message);
      }
    });

    await this.ffmpeg.load({
      coreURL: 'https://cdn.jsdelivr.net/npm/@ffmpeg/core@0.12.6/dist/umd/ffmpeg-core.js',
      wasmURL: 'https://cdn.jsdelivr.net/npm/@ffmpeg/core@0.12.6/dist/umd/ffmpeg-core.wasm'
    });

    this.log('FFmpeg.wasm loaded', 'success');
    this.updateUI({ statusTitle: 'FFmpeg ready', statusDescription: 'Mux engine initialized' });
  }

  // ── Segment parsing ─────────────────────────────────────────────────────────

  async parseSegments() {
    this.log('Parsing segment data...', 'info');

    const { segments } = this.jobData;
    if (!segments) throw new Error('No segments object in job data');

    const videoSegments = segments.video  || [];
    const audioSegments = segments.audio  || [];

    if (videoSegments.length === 0 && audioSegments.length === 0) {
      throw new Error('No segments found in job data');
    }

    this.log(`${videoSegments.length} video + ${audioSegments.length} audio segments`, 'success');
    if (segments.isFMP4) this.log('fMP4 stream — init segments will be prepended', 'info');

    const totalCount = videoSegments.length + audioSegments.length;
    this.totalEstimatedBytes = totalCount * 3 * 1024 * 1024; // 3 MB avg estimate

    this.updateUI({
      segments: `0 / ${videoSegments.length}`,
      fileSize: this.formatBytes(this.totalEstimatedBytes) + ' (estimated)'
    });

    return {
      video:          videoSegments,
      audio:          audioSegments,
      initSegmentUrl: segments.initSegmentUrl || null,
      audioInitUrl:   segments.audioInitUrl   || null,
      isFMP4:         segments.isFMP4         || false
    };
  }

  // ── Download orchestration ──────────────────────────────────────────────────

  async startDownload(segments) {
    this.log(`Starting turbo download with ${this.workerCount} workers...`, 'info');
    this.startTime = Date.now();

    this.updateUI({
      statusTitle:       'Downloading segments',
      statusDescription: `${this.workerCount} parallel workers active`,
      workers:           `${this.workerCount} active`
    });

    this.createWorkers();

    // Fix: distribute video and audio independently.
    // Previously audio was sliced at the same indices as video segments,
    // so audio[50] would go to the same worker as video[50] — correct only
    // when audio.length === video.length. Separate audio tracks always differ.
    const videoChunks = this.distributeUrls(segments.video, 'video');
    const audioChunks = this.distributeUrls(segments.audio, 'audio');

    // Fetch fMP4 init segments before workers start (small, must come first)
    let videoInitData = null;
    let audioInitData = null;

    if (segments.isFMP4 && segments.initSegmentUrl) {
      this.log('Fetching video init segment...', 'info');
      try {
        const res = await fetch(segments.initSegmentUrl, {
          cache: 'no-cache', credentials: 'include'
        });
        if (res.ok) {
          videoInitData = await res.arrayBuffer();
          this.log(`Video init: ${videoInitData.byteLength} bytes`, 'success');
        }
      } catch (e) {
        this.log(`Video init fetch failed: ${e.message}`, 'error');
      }
    }

    if (segments.isFMP4 && segments.audioInitUrl) {
      this.log('Fetching audio init segment...', 'info');
      try {
        const res = await fetch(segments.audioInitUrl, {
          cache: 'no-cache', credentials: 'include'
        });
        if (res.ok) {
          audioInitData = await res.arrayBuffer();
          this.log(`Audio init: ${audioInitData.byteLength} bytes`, 'success');
        }
      } catch (e) {
        this.log(`Audio init fetch failed: ${e.message}`, 'error');
      }
    }

    // Build combined chunks: each worker gets its video slice + audio slice.
    // Workers where one list is empty just skip that track.
    const maxChunks = Math.max(videoChunks.length, audioChunks.length);
    const chunks    = [];
    for (let i = 0; i < maxChunks; i++) {
      chunks.push({
        workerId:        i,
        video:           videoChunks[i]?.urls       || [],
        audio:           audioChunks[i]?.urls       || [],
        videoStartIndex: videoChunks[i]?.startIndex || 0,
        audioStartIndex: audioChunks[i]?.startIndex || 0
      });
    }

    // Ensure enough workers exist
    while (this.workers.length < chunks.length) {
      this.workers.push(new Worker('workers/download-worker.js'));
    }

    const downloadPromises = chunks.map((chunk, i) =>
      this.downloadChunk(this.workers[i], chunk, i)
    );

    try {
      const results = await Promise.all(downloadPromises);

      if (this.isCancelled) {
        this.log('Download cancelled', 'info');
        return;
      }

      // Merge and sort each track independently by global segment index
      const allVideo = results.flatMap(r => r.video).sort((a, b) => a.index - b.index);
      const allAudio = results.flatMap(r => r.audio).sort((a, b) => a.index - b.index);

      this.log(
        `Download complete — ${allVideo.length} video, ${allAudio.length} audio segs. Starting mux...`,
        'success'
      );

      await this.muxAndSave(allVideo, allAudio, videoInitData, audioInitData, segments.isFMP4);

    } catch (error) {
      console.error('[MuxNet] Download error:', error);
      this.showError(`Download failed: ${error.message}`);
    }
  }

  // ── Worker management ───────────────────────────────────────────────────────

  createWorkers() {
    this.workers = [];
    for (let i = 0; i < this.workerCount; i++) {
      this.workers.push(new Worker('workers/download-worker.js'));
    }
    this.createWorkerCards();
  }

  createWorkerCards() {
    const grid = document.getElementById('workersGrid');
    if (!grid) return;
    grid.innerHTML = '';
    for (let i = 0; i < this.workerCount; i++) {
      const card = document.createElement('div');
      card.className = 'worker-card idle';
      card.id        = `worker-${i}`;
      card.innerHTML = `<div class="worker-id">W${i + 1}</div><div class="worker-progress">–</div>`;
      grid.appendChild(card);
    }
  }

  // Distribute URL list evenly across workers. Returns [{startIndex, urls}].
  distributeUrls(urls, _type) {
    if (!urls || urls.length === 0) return [];
    const active        = Math.min(this.workerCount, urls.length);
    const perWorker     = Math.ceil(urls.length / active);
    const chunks        = [];
    for (let i = 0; i < active; i++) {
      const start = i * perWorker;
      const end   = Math.min(start + perWorker, urls.length);
      if (start >= urls.length) break;
      chunks.push({ startIndex: start, urls: urls.slice(start, end) });
    }
    return chunks;
  }

  downloadChunk(worker, chunk, workerId) {
    return new Promise((resolve, reject) => {
      const videoResult = [];
      const audioResult = [];

      worker.postMessage({ type: 'start', chunk });

      worker.onmessage = (e) => {
        const { type, data } = e.data;

        switch (type) {
          case 'progress':
            this.handleWorkerProgress(workerId, data);
            break;

          case 'segment_complete':
            // ArrayBuffer was transferred — zero memory copy
            if (data.segType === 'video') {
              videoResult.push({ index: data.index, data: data.data });
            } else {
              audioResult.push({ index: data.index, data: data.data });
            }
            this.completedSegments++;
            this.updateSegmentCount();
            break;

          case 'segment_failed':
            this.failedSegments++;
            this.log(`Seg ${data.index} (${data.segType}) failed: ${data.error}`, 'error');
            this.updateUI({ failed: this.failedSegments.toString() });
            break;

          case 'complete':
            this.log(`Worker ${workerId + 1} done`, 'success');
            this.markWorkerDone(workerId);
            resolve({ video: videoResult, audio: audioResult });
            break;

          case 'error':
            reject(new Error(data.message));
            break;
        }
      };

      worker.onerror = (err) => reject(err);
    });
  }

  markWorkerDone(workerId) {
    const card = document.getElementById(`worker-${workerId}`);
    if (!card) return;
    card.className = 'worker-card done';
    const prog = card.querySelector('.worker-progress');
    if (prog) prog.textContent = '✓';
  }

  handleWorkerProgress(workerId, data) {
    const { downloaded, speed } = data;

    const card = document.getElementById(`worker-${workerId}`);
    if (card && !card.classList.contains('done')) card.className = 'worker-card active';

    this.downloadedBytes += downloaded;
    this.speedSamples.push(speed);
    if (this.speedSamples.length > 10) this.speedSamples.shift();

    const avgSpeed   = this.speedSamples.reduce((a, b) => a + b, 0) / this.speedSamples.length;
    const percentage = Math.min((this.downloadedBytes / this.totalEstimatedBytes) * 100, 95);
    const remaining  = (this.totalEstimatedBytes - this.downloadedBytes) / (avgSpeed || 1);

    this.updateUI({
      percentage: `${Math.round(percentage)}%`,
      speed:      `${(avgSpeed / 1024 / 1024).toFixed(1)} MB/s`,
      downloaded: this.formatBytes(this.downloadedBytes),
      eta:        this.formatTime(remaining)
    });

    const fill = document.getElementById('progressFill');
    if (fill) fill.style.width = `${percentage}%`;
  }

  updateSegmentCount() {
    const total = (this.jobData?.segments?.video?.length || 0) +
                  (this.jobData?.segments?.audio?.length || 0);
    this.updateUI({ segments: `${this.completedSegments} / ${total}` });
  }

  // ── FFmpeg mux and save ─────────────────────────────────────────────────────

  async muxAndSave(videoSegs, audioSegs, videoInitData, audioInitData, isFMP4) {
    this.log('Starting FFmpeg mux...', 'info');
    this.updateUI({
      statusTitle:       'Muxing video + audio',
      statusDescription: 'FFmpeg — PTS timestamp synchronization'
    });

    const spinner = document.getElementById('spinner');
    if (spinner) spinner.style.display = 'none';

    const ext    = isFMP4 ? 'mp4' : 'ts';
    const ffmpeg = this.ffmpeg;
    const filesToCleanup = [`output.${ext}`];

    try {
      // ── Write video track ──────────────────────────────────────────────────
      let videoList = '';
      if (videoSegs.length > 0) {
        // Fix: prepend init segment for fMP4 — it contains moov/ftyp boxes
        // without it every subsequent segment is undecodable.
        if (isFMP4 && videoInitData) {
          const f = 'vinit.mp4';
          await ffmpeg.writeFile(f, new Uint8Array(videoInitData));
          videoList += `file '${f}'\n`;
          filesToCleanup.push(f);
        }
        for (let i = 0; i < videoSegs.length; i++) {
          const f = `v${i}.${ext}`;
          await ffmpeg.writeFile(f, new Uint8Array(videoSegs[i].data));
          videoList += `file '${f}'\n`;
          filesToCleanup.push(f);
        }
        await ffmpeg.writeFile('video_list.txt', videoList);
        filesToCleanup.push('video_list.txt');
        this.log(`${videoSegs.length} video segs written to FFmpeg FS`, 'info');
      }

      // ── Write audio track ──────────────────────────────────────────────────
      let audioList = '';
      if (audioSegs.length > 0) {
        if (isFMP4 && audioInitData) {
          const f = 'ainit.mp4';
          await ffmpeg.writeFile(f, new Uint8Array(audioInitData));
          audioList += `file '${f}'\n`;
          filesToCleanup.push(f);
        }
        for (let i = 0; i < audioSegs.length; i++) {
          const f = `a${i}.${ext}`;
          await ffmpeg.writeFile(f, new Uint8Array(audioSegs[i].data));
          audioList += `file '${f}'\n`;
          filesToCleanup.push(f);
        }
        await ffmpeg.writeFile('audio_list.txt', audioList);
        filesToCleanup.push('audio_list.txt');
        this.log(`${audioSegs.length} audio segs written to FFmpeg FS`, 'info');
      }

      // ── Run FFmpeg ─────────────────────────────────────────────────────────
      this.log('Running FFmpeg...', 'info');

      if (videoSegs.length > 0 && audioSegs.length > 0) {
        // Two separate tracks — concat each independently then mux together.
        // -map 0:v:0 -map 1:a:0 ensures FFmpeg picks the right streams.
        await ffmpeg.exec([
          '-f', 'concat', '-safe', '0', '-i', 'video_list.txt',
          '-f', 'concat', '-safe', '0', '-i', 'audio_list.txt',
          '-c', 'copy',
          '-map', '0:v:0',
          '-map', '1:a:0',
          `output.${ext}`
        ]);

      } else if (videoSegs.length > 0) {
        // Video only — audio is already muxed in the .ts packets
        await ffmpeg.exec([
          '-f', 'concat', '-safe', '0', '-i', 'video_list.txt',
          '-c', 'copy',
          `output.${ext}`
        ]);

      } else {
        // Audio only
        await ffmpeg.exec([
          '-f', 'concat', '-safe', '0', '-i', 'audio_list.txt',
          '-c', 'copy',
          `output.${ext}`
        ]);
      }

      // ── Read + download ────────────────────────────────────────────────────
      const data     = await ffmpeg.readFile(`output.${ext}`);
      const mimeType = isFMP4 ? 'video/mp4' : 'video/mp2t';
      const blob     = new Blob([data.buffer], { type: mimeType });
      const blobUrl  = URL.createObjectURL(blob);
      const filename = `${this.jobData.title || 'stream'}.${ext}`;

      const a    = document.createElement('a');
      a.href     = blobUrl;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      setTimeout(() => URL.revokeObjectURL(blobUrl), 5000);

      this.log(`Mux complete — ${this.formatBytes(blob.size)} saved as ${filename}`, 'success');

      this.showComplete(blob.size);

    } catch (error) {
      console.error('[MuxNet] Mux error:', error);
      this.showError(`Muxing failed: ${error.message}`);
    } finally {
      // Clean up FFmpeg virtual FS to free memory
      for (const f of filesToCleanup) {
        try { await ffmpeg.deleteFile(f); } catch (_) {}
      }
    }
  }

  // ── Completion + UI ─────────────────────────────────────────────────────────

  showComplete(fileSize) {
    const elapsed  = (Date.now() - this.startTime) / 1000;
    const avgSpeed = fileSize / elapsed;

    this.log(
      `Done! ${this.formatBytes(fileSize)} in ${this.formatTime(elapsed)}, ` +
      `avg ${(avgSpeed / 1024 / 1024).toFixed(1)} MB/s`,
      'success'
    );

    const fill = document.getElementById('progressFill');
    if (fill) fill.style.width = '100%';
    this.updateUI({ percentage: '100%' });

    const modal = document.getElementById('completeModal');
    if (modal) {
      modal.style.display = 'flex';
      const set = (id, val) => { const el = document.getElementById(id); if (el) el.textContent = val; };
      set('completedFilename', this.jobData?.title || 'Stream');
      set('completedSize',     this.formatBytes(fileSize));
      set('completedTime',     this.formatTime(elapsed));
      set('completedSpeed',    `${(avgSpeed / 1024 / 1024).toFixed(1)} MB/s avg`);
    }

    document.getElementById('closeModal')?.addEventListener('click', () => window.close());
  }

  showError(message) {
    this.log(message, 'error');
    this.updateUI({ statusTitle: 'Error', statusDescription: message });
    const spinner = document.getElementById('spinner');
    if (spinner) spinner.style.display = 'none';
  }

  updateUI(updates) {
    for (const [key, value] of Object.entries(updates)) {
      const el = document.getElementById(key);
      if (el) el.textContent = value;
    }
  }

  log(message, level = 'info') {
    console.log(`[MuxNet] ${message}`);
    const container = document.getElementById('logContainer');
    if (!container) return;
    const entry = document.createElement('div');
    entry.className = `log-entry ${level}`;
    entry.textContent = `[${new Date().toLocaleTimeString()}] ${message}`;
    container.appendChild(entry);
    container.scrollTop = container.scrollHeight;
  }

  formatBytes(bytes) {
    if (!bytes || bytes === 0) return '0 B';
    const k = 1024, sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return (bytes / Math.pow(k, i)).toFixed(1) + ' ' + sizes[i];
  }

  formatTime(seconds) {
    if (!isFinite(seconds) || seconds < 0) return 'Calculating...';
    if (seconds < 60) return `${Math.round(seconds)}s`;
    return `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`;
  }
}
