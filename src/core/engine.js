// MuxNet Engine - Main Orchestrator
// Handles job initialization, worker coordination, FFmpeg mux, and download flow
//
// Architecture:
//   Previous: download ALL video → download ALL audio → mux.
//             Problem: sequential, and holds all data in RAM before FFmpeg starts.
//
//   New: video workers + audio workers run SIMULTANEOUSLY via Promise.all.
//        - Video workers fill videoResults[], audio workers fill audioResults[]
//        - Both resolve when done, then FFmpeg muxes them together properly
//        - FFmpeg command: -i video_concat -i audio_concat -c copy -map 0:v -map 1:a
//          This gives true PTS-accurate A/V sync, not segment-level interleaving
//
// Fixes applied:
//   - Parallel download: video pool + audio pool run at same time (Promise.all)
//   - loadJobData: uses chrome.runtime.sendMessage(extId) not chrome.storage (web
//     pages cannot access chrome.storage — extension SW must bridge it)
//   - distributeUrls: video and audio distributed independently (different counts)
//   - muxAndSave: proper -map flags, fMP4 init segments prepended, FS cleanup
//   - ArrayBuffer transfers: zero memory copy via postMessage transfer list
//   - All UI accesses null-guarded

export class MuxNetEngine {
  constructor() {
    this.jobId               = null;
    this.jobData             = null;
    this.videoWorkers        = [];
    this.audioWorkers        = [];
    this.workerCount         = Math.min(Math.max(navigator.hardwareConcurrency || 4, 4), 32);
    this.ffmpeg              = null;
    this.startTime           = null;
    this.downloadedBytes     = 0;
    this.totalEstimatedBytes = 0;
    this.speedSamples        = [];
    this.isCancelled         = false;
    this.completedSegments   = 0;
    this.failedSegments      = 0;
  }

  async start() {
    try {
      const params  = new URLSearchParams(window.location.search);
      this.jobId    = params.get('job');

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
  // Web pages cannot call chrome.storage.local.get() — that API is extension-only.
  // We use chrome.runtime.sendMessage(extId, msg) which IS available to origins
  // listed in externally_connectable. The background SW reads storage and replies.

  async loadJobData() {
    this.log('Loading job data from extension...', 'info');

    const params = new URLSearchParams(window.location.search);
    const extId  = params.get('ext');

    if (extId && typeof chrome !== 'undefined' && chrome.runtime) {
      try {
        const response = await new Promise((resolve, reject) => {
          const timeout = setTimeout(
            () => reject(new Error('Extension message timeout after 8s')), 8000
          );
          chrome.runtime.sendMessage(
            extId,
            { type: 'GET_MUXNET_JOB', jobId: this.jobId },
            (resp) => {
              clearTimeout(timeout);
              if (chrome.runtime.lastError) {
                reject(new Error(chrome.runtime.lastError.message));
              } else {
                resolve(resp);
              }
            }
          );
        });

        if (response?.success && response.jobData) {
          this.jobData = response.jobData;
          const v = this.jobData.segments?.video?.length  || 0;
          const a = this.jobData.segments?.audio?.length  || 0;
          this.log(`Job loaded: "${this.jobData.title || 'Stream'}" — ${v} video + ${a} audio segs`, 'success');
          this.updateUI({
            filename:          this.jobData.title || 'Stream',
            statusTitle:       'Job loaded',
            statusDescription: `${v} video segments + ${a} audio segments`
          });
          return;
        } else {
          throw new Error(response?.error || 'Empty response from extension');
        }

      } catch (error) {
        this.log(`Extension message failed: ${error.message}`, 'error');
        console.warn('[MuxNet] sendMessage failed:', error);
      }
    } else if (!extId) {
      this.log('No ?ext= in URL — cannot contact extension', 'error');
    }

    // Fallback: localStorage (standalone / testing without extension)
    try {
      const raw = localStorage.getItem(`muxnet_job_${this.jobId}`);
      if (raw) {
        this.jobData = JSON.parse(raw);
        this.log('Job loaded from localStorage (test mode)', 'info');
        this.updateUI({
          filename:          this.jobData.title || 'Test Stream',
          statusTitle:       'Test mode',
          statusDescription: 'Using localStorage'
        });
        return;
      }
    } catch (_) {}

    throw new Error('Job data not found. Please restart download from extension.');
  }

  // ── FFmpeg init ─────────────────────────────────────────────────────────────

  async initFFmpeg() {
    this.log('Loading FFmpeg.wasm (~30 MB, cached after first use)...', 'info');
    this.updateUI({
      statusTitle:       'Loading FFmpeg.wasm',
      statusDescription: 'One-time 30 MB download, cached by browser'
    });

    // The @ffmpeg/ffmpeg UMD bundle exposes window.FFmpegWASM = { FFmpeg, fetchFile }.
    // Older builds exposed window.FFmpeg directly. download.html normalises both
    // into window.FFmpegWASM before this module runs.
    if (!window.FFmpegWASM || !window.FFmpegWASM.FFmpeg) {
      throw new Error(
        'FFmpeg.wasm not loaded. Check that the <script crossorigin> tag in download.html ' +
        'loaded successfully and that SharedArrayBuffer is available (requires COOP/COEP headers).'
      );
    }
    const { FFmpeg } = window.FFmpegWASM;
    this.ffmpeg = new FFmpeg();

    this.ffmpeg.on('log', ({ message }) => {
      if (message && !message.startsWith('ffmpeg version') && !message.startsWith('  ')) {
        console.log('[FFmpeg]', message);
      }
    });

    await this.ffmpeg.load({
      coreURL: 'https://cdn.jsdelivr.net/npm/@ffmpeg/core@0.12.6/dist/umd/ffmpeg-core.js',
      wasmURL: 'https://cdn.jsdelivr.net/npm/@ffmpeg/core@0.12.6/dist/umd/ffmpeg-core.wasm'
    });

    this.log('FFmpeg.wasm ready', 'success');
    this.updateUI({ statusTitle: 'FFmpeg ready', statusDescription: 'Mux engine initialized' });
  }

  // ── Segment parsing ─────────────────────────────────────────────────────────

  async parseSegments() {
    this.log('Parsing segment data...', 'info');

    const { segments } = this.jobData;
    if (!segments) throw new Error('No segments in job data');

    const video = segments.video || [];
    const audio = segments.audio || [];

    if (video.length === 0 && audio.length === 0) throw new Error('No segments found');

    this.log(`${video.length} video + ${audio.length} audio segments`, 'success');
    if (segments.isFMP4) this.log('fMP4 stream — init segments will be prepended', 'info');

    this.totalEstimatedBytes = (video.length + audio.length) * 3 * 1024 * 1024;
    this.updateUI({
      segments: `0 / ${video.length}`,
      fileSize: this.formatBytes(this.totalEstimatedBytes) + ' (estimated)'
    });

    return {
      video,
      audio,
      initSegmentUrl: segments.initSegmentUrl || null,
      audioInitUrl:   segments.audioInitUrl   || null,
      isFMP4:         segments.isFMP4         || false
    };
  }

  // ── Download orchestration ──────────────────────────────────────────────────
  //
  // Key design: TWO separate worker pools run in parallel via Promise.all.
  //   videoPool — N workers downloading video segments simultaneously
  //   audioPool — M workers downloading audio segments simultaneously
  //
  // Both pools complete independently. Total wall time = max(video, audio) time.
  // Previously: wall time = video_time + audio_time (sequential).
  //
  // For a 754-video + 755-audio stream at 30 MB/s:
  //   Sequential (old): ~200 seconds
  //   Parallel   (new): ~100 seconds

  async startDownload(segments) {
    this.startTime = Date.now();
    const hasAudio = segments.audio.length > 0;

    this.log(
      `Starting parallel download: ${this.workerCount} workers for video` +
      (hasAudio ? ` + ${Math.ceil(this.workerCount / 2)} for audio` : ''),
      'info'
    );

    this.updateUI({
      statusTitle:       'Downloading',
      statusDescription: 'Video + audio downloading simultaneously'
    });

    // Distribute video and audio independently — they have different counts
    const videoChunks = this.distributeUrls(segments.video, this.workerCount);
    // Use half the workers for audio — audio segments are smaller
    const audioChunks = hasAudio
      ? this.distributeUrls(segments.audio, Math.max(1, Math.ceil(this.workerCount / 2)))
      : [];

    const totalWorkers = videoChunks.length + audioChunks.length;
    this.createWorkerCards(totalWorkers);

    // Fetch fMP4 init segments (tiny, fetch before workers start)
    let videoInitData = null;
    let audioInitData = null;

    if (segments.isFMP4 && segments.initSegmentUrl) {
      try {
        const res = await fetch(segments.initSegmentUrl, { cache:'no-cache', credentials:'include' });
        if (res.ok) { videoInitData = await res.arrayBuffer(); this.log(`Video init: ${videoInitData.byteLength} bytes`, 'success'); }
      } catch (e) { this.log(`Video init fetch failed: ${e.message}`, 'error'); }
    }

    if (segments.isFMP4 && segments.audioInitUrl) {
      try {
        const res = await fetch(segments.audioInitUrl, { cache:'no-cache', credentials:'include' });
        if (res.ok) { audioInitData = await res.arrayBuffer(); this.log(`Audio init: ${audioInitData.byteLength} bytes`, 'success'); }
      } catch (e) { this.log(`Audio init fetch failed: ${e.message}`, 'error'); }
    }

    // Spin up video workers
    this.videoWorkers = videoChunks.map(() => new Worker('workers/download-worker.js'));
    // Spin up audio workers (offset IDs so worker cards don't collide)
    this.audioWorkers = audioChunks.map(() => new Worker('workers/download-worker.js'));

    this.updateUI({ workers: `${totalWorkers} active` });

    // ── Launch both pools simultaneously ──────────────────────────────────
    this.log(`Launching video pool (${videoChunks.length} workers) + audio pool (${audioChunks.length} workers) in parallel`, 'info');

    const videoPoolPromise = Promise.all(
      videoChunks.map((chunk, i) => this.runWorker(this.videoWorkers[i], {
        workerId:        i,
        video:           chunk.urls,
        audio:           [],
        videoStartIndex: chunk.startIndex,
        audioStartIndex: 0,
        trackType:       'video'
      }, i))
    );

    const audioPoolPromise = hasAudio
      ? Promise.all(
          audioChunks.map((chunk, i) => this.runWorker(this.audioWorkers[i], {
            workerId:        videoChunks.length + i,
            video:           [],
            audio:           chunk.urls,
            videoStartIndex: 0,
            audioStartIndex: chunk.startIndex,
            trackType:       'audio'
          }, videoChunks.length + i))
        )
      : Promise.resolve([]);

    try {
      // Promise.all runs both pools concurrently
      const [videoResults, audioResults] = await Promise.all([videoPoolPromise, audioPoolPromise]);

      if (this.isCancelled) { this.log('Cancelled', 'info'); return; }

      // Flatten and sort each track independently
      const allVideo = videoResults.flat().filter(r => r.segType === 'video').sort((a,b) => a.index - b.index);
      const allAudio = (audioResults || []).flat().filter(r => r.segType === 'audio').sort((a,b) => a.index - b.index);

      this.log(
        `Both pools complete — ${allVideo.length} video + ${allAudio.length} audio segments. Starting FFmpeg mux...`,
        'success'
      );

      await this.muxAndSave(allVideo, allAudio, videoInitData, audioInitData, segments.isFMP4);

    } catch (error) {
      console.error('[MuxNet] Download error:', error);
      this.showError(`Download failed: ${error.message}`);
    }
  }

  // ── Distribute URLs evenly across N workers ───────────────────────────────

  distributeUrls(urls, maxWorkers) {
    if (!urls || urls.length === 0) return [];
    const active    = Math.min(maxWorkers, urls.length);
    const perWorker = Math.ceil(urls.length / active);
    const chunks    = [];
    for (let i = 0; i < active; i++) {
      const start = i * perWorker;
      const end   = Math.min(start + perWorker, urls.length);
      if (start >= urls.length) break;
      chunks.push({ startIndex: start, urls: urls.slice(start, end) });
    }
    return chunks;
  }

  // ── Single worker lifecycle ───────────────────────────────────────────────
  // Returns array of {segType, index, data} for all segments this worker downloaded.

  runWorker(worker, chunk, cardId) {
    return new Promise((resolve, reject) => {
      const results = [];

      worker.postMessage({ type: 'start', chunk });

      worker.onmessage = (e) => {
        const { type, data } = e.data;
        switch (type) {
          case 'progress':
            this.handleProgress(cardId, data);
            break;

          case 'segment_complete':
            // ArrayBuffer was transferred — zero memory copy
            results.push({ segType: data.segType, index: data.index, data: data.data });
            this.completedSegments++;
            this.updateSegmentCount();
            break;

          case 'segment_failed':
            this.failedSegments++;
            this.log(`Seg ${data.index} (${data.segType}) failed: ${data.error}`, 'error');
            this.updateUI({ failed: this.failedSegments.toString() });
            break;

          case 'complete':
            this.markWorkerDone(cardId);
            resolve(results);
            break;

          case 'error':
            reject(new Error(data.message));
            break;
        }
      };

      worker.onerror = (err) => reject(err);
    });
  }

  // ── Worker card UI ────────────────────────────────────────────────────────

  createWorkerCards(count) {
    const grid = document.getElementById('workersGrid');
    if (!grid) return;
    grid.innerHTML = '';
    for (let i = 0; i < count; i++) {
      const card = document.createElement('div');
      card.className = 'worker-card idle';
      card.id        = `worker-${i}`;
      card.innerHTML = `<div class="worker-id">W${i + 1}</div><div class="worker-progress">–</div>`;
      grid.appendChild(card);
    }
  }

  markWorkerDone(id) {
    const card = document.getElementById(`worker-${id}`);
    if (!card) return;
    card.className = 'worker-card done';
    const p = card.querySelector('.worker-progress');
    if (p) p.textContent = '✓';
  }

  handleProgress(cardId, data) {
    const { downloaded, speed } = data;
    const card = document.getElementById(`worker-${cardId}`);
    if (card && !card.classList.contains('done')) card.className = 'worker-card active';

    this.downloadedBytes += downloaded;
    this.speedSamples.push(speed);
    if (this.speedSamples.length > 20) this.speedSamples.shift();

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

  // ── FFmpeg mux ────────────────────────────────────────────────────────────
  //
  // Strategy:
  //   1. Write all video segments (+ init if fMP4) to FFmpeg virtual FS
  //   2. Write all audio segments (+ init if fMP4) to FFmpeg virtual FS
  //   3. Run FFmpeg with two concat inputs, -map to pick right streams
  //   4. Read output, create Blob, trigger browser download
  //   5. Clean up all virtual FS files
  //
  // FFmpeg command for video+audio:
  //   ffmpeg -f concat -safe 0 -i video_list.txt
  //          -f concat -safe 0 -i audio_list.txt
  //          -c copy -map 0:v:0 -map 1:a:0
  //          output.{ts|mp4}
  //
  // -map 0:v:0 takes the video stream from input 0 (video concat)
  // -map 1:a:0 takes the audio stream from input 1 (audio concat)
  // -c copy    no re-encoding — pure bitstream copy, very fast

  async muxAndSave(videoSegs, audioSegs, videoInitData, audioInitData, isFMP4) {
    this.log(`Muxing ${videoSegs.length} video + ${audioSegs.length} audio segments with FFmpeg...`, 'info');
    this.updateUI({
      statusTitle:       'Muxing',
      statusDescription: `FFmpeg — combining tracks with PTS sync`
    });

    const spinner = document.getElementById('spinner');
    if (spinner) spinner.style.display = 'none';

    const ext          = isFMP4 ? 'mp4' : 'ts';
    const ffmpeg       = this.ffmpeg;
    const toDelete     = [`output.${ext}`];

    try {
      // ── Write video to FFmpeg FS ─────────────────────────────────────────
      let videoList = '';
      if (videoSegs.length > 0) {
        if (isFMP4 && videoInitData) {
          await ffmpeg.writeFile('vinit.mp4', new Uint8Array(videoInitData));
          videoList += `file 'vinit.mp4'\n`;
          toDelete.push('vinit.mp4');
        }
        for (let i = 0; i < videoSegs.length; i++) {
          const f = `v${i}.${ext}`;
          await ffmpeg.writeFile(f, new Uint8Array(videoSegs[i].data));
          videoList += `file '${f}'\n`;
          toDelete.push(f);
        }
        await ffmpeg.writeFile('vlist.txt', videoList);
        toDelete.push('vlist.txt');
        this.log(`${videoSegs.length} video segs → FFmpeg FS`, 'info');
      }

      // ── Write audio to FFmpeg FS ─────────────────────────────────────────
      let audioList = '';
      if (audioSegs.length > 0) {
        if (isFMP4 && audioInitData) {
          await ffmpeg.writeFile('ainit.mp4', new Uint8Array(audioInitData));
          audioList += `file 'ainit.mp4'\n`;
          toDelete.push('ainit.mp4');
        }
        for (let i = 0; i < audioSegs.length; i++) {
          const f = `a${i}.${ext}`;
          await ffmpeg.writeFile(f, new Uint8Array(audioSegs[i].data));
          audioList += `file '${f}'\n`;
          toDelete.push(f);
        }
        await ffmpeg.writeFile('alist.txt', audioList);
        toDelete.push('alist.txt');
        this.log(`${audioSegs.length} audio segs → FFmpeg FS`, 'info');
      }

      // ── Run FFmpeg mux ───────────────────────────────────────────────────
      this.log('Running FFmpeg...', 'info');
      this.updateUI({ statusDescription: 'FFmpeg combining tracks...' });

      let cmd;
      if (videoSegs.length > 0 && audioSegs.length > 0) {
        // Both tracks — concat each, then mux with explicit stream mapping
        cmd = [
          '-f', 'concat', '-safe', '0', '-i', 'vlist.txt',
          '-f', 'concat', '-safe', '0', '-i', 'alist.txt',
          '-c', 'copy',
          '-map', '0:v:0',   // video from input 0
          '-map', '1:a:0',   // audio from input 1
          `output.${ext}`
        ];
      } else if (videoSegs.length > 0) {
        // Video only (audio already embedded in TS, or video-only stream)
        cmd = [
          '-f', 'concat', '-safe', '0', '-i', 'vlist.txt',
          '-c', 'copy',
          `output.${ext}`
        ];
      } else {
        // Audio only
        cmd = [
          '-f', 'concat', '-safe', '0', '-i', 'alist.txt',
          '-c', 'copy',
          `output.${ext}`
        ];
      }

      await ffmpeg.exec(cmd);

      // ── Read + download ──────────────────────────────────────────────────
      this.log('FFmpeg done — preparing download...', 'success');
      this.updateUI({ statusDescription: 'Preparing file download...' });

      const data     = await ffmpeg.readFile(`output.${ext}`);
      const mimeType = isFMP4 ? 'video/mp4' : 'video/mp2t';
      const blob     = new Blob([data.buffer], { type: mimeType });
      const blobUrl  = URL.createObjectURL(blob);
      const filename = `${this.jobData.title || 'stream'}.${ext}`;

      const a = document.createElement('a');
      a.href  = blobUrl; a.download = filename;
      document.body.appendChild(a); a.click(); document.body.removeChild(a);
      setTimeout(() => URL.revokeObjectURL(blobUrl), 5000);

      this.log(`Saved: ${filename} (${this.formatBytes(blob.size)})`, 'success');
      this.showComplete(blob.size);

    } catch (error) {
      console.error('[MuxNet] FFmpeg error:', error);
      this.showError(`Muxing failed: ${error.message}`);
    } finally {
      // Clean up FFmpeg virtual FS
      for (const f of toDelete) {
        try { await ffmpeg.deleteFile(f); } catch (_) {}
      }
    }
  }

  // ── Completion + error UI ─────────────────────────────────────────────────

  showComplete(fileSize) {
    const elapsed  = (Date.now() - this.startTime) / 1000;
    const avgSpeed = fileSize / elapsed;
    this.log(`Complete! ${this.formatBytes(fileSize)} in ${this.formatTime(elapsed)}, avg ${(avgSpeed/1024/1024).toFixed(1)} MB/s`, 'success');

    const fill = document.getElementById('progressFill');
    if (fill) fill.style.width = '100%';
    this.updateUI({ percentage: '100%' });

    const modal = document.getElementById('completeModal');
    if (modal) {
      modal.style.display = 'flex';
      const set = (id, v) => { const el = document.getElementById(id); if (el) el.textContent = v; };
      set('completedFilename', this.jobData?.title || 'Stream');
      set('completedSize',     this.formatBytes(fileSize));
      set('completedTime',     this.formatTime(elapsed));
      set('completedSpeed',    `${(avgSpeed/1024/1024).toFixed(1)} MB/s avg`);
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
    const c = document.getElementById('logContainer');
    if (!c) return;
    const e = document.createElement('div');
    e.className   = `log-entry ${level}`;
    e.textContent = `[${new Date().toLocaleTimeString()}] ${message}`;
    c.appendChild(e);
    c.scrollTop = c.scrollHeight;
  }

  formatBytes(bytes) {
    if (!bytes || bytes === 0) return '0 B';
    const k = 1024, s = ['B','KB','MB','GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return (bytes / Math.pow(k, i)).toFixed(1) + ' ' + s[i];
  }

  formatTime(seconds) {
    if (!isFinite(seconds) || seconds < 0) return 'Calculating...';
    if (seconds < 60) return `${Math.round(seconds)}s`;
    return `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`;
  }
}
