// MuxNet Engine - Main Orchestrator
// Handles job initialization, worker coordination, and download flow

export class MuxNetEngine {
  constructor() {
    this.jobId = null;
    this.jobData = null;
    this.workers = [];
    this.workerCount = Math.min(navigator.hardwareConcurrency || 8, 32);
    this.ffmpeg = null;
    this.startTime = null;
    this.downloadedBytes = 0;
    this.totalBytes = 0;
    this.speedSamples = [];
    this.isPaused = false;
    this.isCancelled = false;
    this.completedSegments = 0;
    this.failedSegments = 0;
  }

  async start() {
    try {
      // Get job ID from URL
      const params = new URLSearchParams(window.location.search);
      this.jobId = params.get('job');

      if (!this.jobId) {
        this.showError('No job ID provided. Please start download from extension.');
        return;
      }

      this.log('Initializing MuxNet engine...', 'info');
      this.updateUI({ jobId: `Job: ${this.jobId}` });

      // Load job data from extension storage
      await this.loadJobData();

      // Initialize FFmpeg
      await this.initFFmpeg();

      // Parse segments
      const segments = await this.parseSegments();

      // Start download
      await this.startDownload(segments);

    } catch (error) {
      console.error('[MuxNet] Engine error:', error);
      this.showError(`Engine failed: ${error.message}`);
    }
  }

  async loadJobData() {
    this.log('Loading job data from extension...', 'info');
    
    try {
      // Try chrome.storage if available (extension context)
      if (typeof chrome !== 'undefined' && chrome.storage) {
        const key = `muxnet_job_${this.jobId}`;
        const result = await chrome.storage.local.get(key);
        
        if (result[key]) {
          this.jobData = result[key];
          this.log('Job data loaded from extension', 'success');
          this.updateUI({
            filename: this.jobData.title || 'Stream',
            statusTitle: 'Job data loaded',
            statusDescription: `${this.jobData.segments?.video?.length || 0} video segments detected`
          });
          return;
        }
      }
    } catch (error) {
      console.warn('[MuxNet] Chrome storage not available:', error);
    }

    // Fallback: Try localStorage (for testing without extension)
    try {
      const localData = localStorage.getItem(`muxnet_job_${this.jobId}`);
      if (localData) {
        this.jobData = JSON.parse(localData);
        this.log('Job data loaded from localStorage (test mode)', 'info');
        this.updateUI({
          filename: this.jobData.title || 'Test Stream',
          statusTitle: 'Test mode active',
          statusDescription: 'Using localStorage for job data'
        });
        return;
      }
    } catch (error) {
      console.warn('[MuxNet] localStorage fallback failed:', error);
    }

    throw new Error('Job data not found. Please restart download from extension.');
  }

  async initFFmpeg() {
    this.log('Loading FFmpeg.wasm (30 MB, one-time download)...', 'info');
    this.updateUI({
      statusTitle: 'Loading FFmpeg.wasm',
      statusDescription: 'First-time setup, ~30 MB download'
    });

    const { FFmpeg } = FFmpegWASM;
    this.ffmpeg = new FFmpeg();

    // Progress callback
    this.ffmpeg.on('log', ({ message }) => {
      console.log('[FFmpeg]', message);
    });

    await this.ffmpeg.load({
      coreURL: 'https://cdn.jsdelivr.net/npm/@ffmpeg/core@0.12.6/dist/umd/ffmpeg-core.js',
      wasmURL: 'https://cdn.jsdelivr.net/npm/@ffmpeg/core@0.12.6/dist/umd/ffmpeg-core.wasm'
    });

    this.log('FFmpeg.wasm loaded successfully', 'success');
    this.updateUI({
      statusTitle: 'FFmpeg ready',
      statusDescription: 'Muxing engine initialized'
    });
  }

  async parseSegments() {
    this.log('Parsing segment URLs...', 'info');

    const { segments } = this.jobData;

    if (!segments || !segments.video || segments.video.length === 0) {
      throw new Error('No video segments found in job data');
    }

    const videoSegments = segments.video;
    const audioSegments = segments.audio || [];

    this.log(`Found ${videoSegments.length} video + ${audioSegments.length} audio segments`, 'success');

    // Estimate total size (rough estimate: 2-5 MB per segment)
    const avgSegmentSize = 3 * 1024 * 1024; // 3 MB
    this.totalBytes = (videoSegments.length + audioSegments.length) * avgSegmentSize;

    this.updateUI({
      segments: `0 / ${videoSegments.length}`,
      fileSize: this.formatBytes(this.totalBytes) + ' (estimated)'
    });

    return { video: videoSegments, audio: audioSegments };
  }

  async startDownload(segments) {
    this.log('Starting turbo download...', 'info');
    this.startTime = Date.now();

    this.updateUI({
      statusTitle: 'Downloading segments',
      statusDescription: `Using ${this.workerCount} parallel workers`
    });

    // Create worker pool
    this.createWorkers();

    // Distribute segments across workers
    const chunks = this.distributeSegments(segments);

    // Download all chunks in parallel
    const downloadPromises = chunks.map((chunk, index) => 
      this.downloadChunk(this.workers[index], chunk, index)
    );

    try {
      const results = await Promise.all(downloadPromises);

      if (this.isCancelled) {
        this.log('Download cancelled by user', 'info');
        return;
      }

      // Flatten results
      const allChunks = results.flat();

      this.log('All segments downloaded, starting mux...', 'success');
      await this.muxAndSave(allChunks);

    } catch (error) {
      console.error('[MuxNet] Download error:', error);
      this.showError(`Download failed: ${error.message}`);
    }
  }

  createWorkers() {
    this.log(`Creating ${this.workerCount} workers...`, 'info');

    for (let i = 0; i < this.workerCount; i++) {
      const worker = new Worker('workers/download-worker.js');
      this.workers.push(worker);
    }

    this.updateUI({ workers: `${this.workerCount} active` });

    // Create worker visualization
    this.createWorkerCards();
  }

  createWorkerCards() {
    const grid = document.getElementById('workersGrid');
    if (!grid) return;

    grid.innerHTML = '';

    for (let i = 0; i < this.workerCount; i++) {
      const card = document.createElement('div');
      card.className = 'worker-card idle';
      card.id = `worker-${i}`;
      card.innerHTML = `
        <div class="worker-id">Worker ${i + 1}</div>
        <div class="worker-progress">0%</div>
      `;
      grid.appendChild(card);
    }
  }

  distributeSegments(segments) {
    const { video, audio } = segments;
    const totalSegments = video.length;
    const segmentsPerWorker = Math.ceil(totalSegments / this.workerCount);

    const chunks = [];

    for (let i = 0; i < this.workerCount; i++) {
      const start = i * segmentsPerWorker;
      const end = Math.min(start + segmentsPerWorker, totalSegments);

      if (start >= totalSegments) break;

      chunks.push({
        workerId: i,
        video: video.slice(start, end),
        audio: audio.slice(start, end),
        startIndex: start,
        endIndex: end
      });
    }

    this.log(`Distributed ${totalSegments} segments across ${chunks.length} workers`, 'info');

    return chunks;
  }

  downloadChunk(worker, chunk, workerId) {
    return new Promise((resolve, reject) => {
      const downloadedChunks = [];

      worker.postMessage({
        type: 'start',
        chunk: chunk
      });

      worker.onmessage = (e) => {
        const { type, data } = e.data;

        switch (type) {
          case 'progress':
            this.handleWorkerProgress(workerId, data);
            break;

          case 'segment_complete':
            downloadedChunks.push(data);
            this.completedSegments++;
            this.updateSegmentCount();
            break;

          case 'segment_failed':
            this.failedSegments++;
            this.log(`Segment ${data.index} failed: ${data.error}`, 'error');
            this.updateUI({ failed: this.failedSegments.toString() });
            break;

          case 'complete':
            this.log(`Worker ${workerId + 1} completed`, 'success');
            resolve(downloadedChunks);
            break;

          case 'error':
            reject(new Error(data.message));
            break;
        }
      };

      worker.onerror = (error) => {
        reject(error);
      };
    });
  }

  handleWorkerProgress(workerId, data) {
    const { downloaded, speed } = data;

    // Update worker card
    const card = document.getElementById(`worker-${workerId}`);
    if (card) {
      card.className = 'worker-card active';
      const progress = card.querySelector('.worker-progress');
      if (progress) {
        progress.textContent = `${Math.round((downloaded / this.totalBytes) * 100)}%`;
      }
    }

    // Update global stats
    this.downloadedBytes += downloaded;
    this.speedSamples.push(speed);

    // Keep only last 10 samples
    if (this.speedSamples.length > 10) {
      this.speedSamples.shift();
    }

    const avgSpeed = this.speedSamples.reduce((a, b) => a + b, 0) / this.speedSamples.length;
    const percentage = Math.min((this.downloadedBytes / this.totalBytes) * 100, 100);
    const remaining = (this.totalBytes - this.downloadedBytes) / avgSpeed;

    this.updateUI({
      percentage: `${Math.round(percentage)}%`,
      speed: `${(avgSpeed / 1024 / 1024).toFixed(1)} MB/s`,
      downloaded: this.formatBytes(this.downloadedBytes),
      eta: this.formatTime(remaining)
    });

    // Update progress bar
    const progressFill = document.getElementById('progressFill');
    if (progressFill) {
      progressFill.style.width = `${percentage}%`;
    }
  }

  updateSegmentCount() {
    const total = this.jobData.segments.video.length;
    this.updateUI({
      segments: `${this.completedSegments} / ${total}`
    });
  }

  async muxAndSave(chunks) {
    this.log('Starting FFmpeg mux...', 'info');
    this.updateUI({
      statusTitle: 'Muxing video + audio',
      statusDescription: 'FFmpeg is synchronizing tracks'
    });

    // Hide spinner, show progress
    const spinner = document.getElementById('spinner');
    if (spinner) spinner.style.display = 'none';

    try {
      // Sort chunks by index
      chunks.sort((a, b) => a.index - b.index);

      // Write segments to FFmpeg virtual filesystem
      const videoSegments = chunks.filter(c => c.type === 'video');
      const audioSegments = chunks.filter(c => c.type === 'audio');

      this.log(`Writing ${videoSegments.length} video + ${audioSegments.length} audio segments`, 'info');

      // Create concat lists
      let videoList = '';
      let audioList = '';

      for (let i = 0; i < videoSegments.length; i++) {
        const filename = `v${i}.ts`;
        await this.ffmpeg.writeFile(filename, new Uint8Array(videoSegments[i].data));
        videoList += `file '${filename}'\n`;
      }

      for (let i = 0; i < audioSegments.length; i++) {
        const filename = `a${i}.ts`;
        await this.ffmpeg.writeFile(filename, new Uint8Array(audioSegments[i].data));
        audioList += `file '${filename}'\n`;
      }

      // Write concat lists
      await this.ffmpeg.writeFile('video_list.txt', videoList);
      await this.ffmpeg.writeFile('audio_list.txt', audioList);

      // Concatenate and mux
      this.log('Running FFmpeg mux command...', 'info');

      await this.ffmpeg.exec([
        '-f', 'concat',
        '-safe', '0',
        '-i', 'video_list.txt',
        '-f', 'concat',
        '-safe', '0',
        '-i', 'audio_list.txt',
        '-c', 'copy',
        'output.ts'
      ]);

      // Read output
      const data = await this.ffmpeg.readFile('output.ts');

      this.log('Mux complete, preparing download...', 'success');

      // Create blob and download
      const blob = new Blob([data.buffer], { type: 'video/mp2t' });
      const url = URL.createObjectURL(blob);
      const filename = `${this.jobData.title || 'stream'}.ts`;

      const a = document.createElement('a');
      a.href = url;
      a.download = filename;
      a.click();

      URL.revokeObjectURL(url);

      // Show completion modal
      this.showComplete(blob.size);

    } catch (error) {
      console.error('[MuxNet] Mux error:', error);
      this.showError(`Muxing failed: ${error.message}`);
    }
  }

  showComplete(fileSize) {
    const elapsed = (Date.now() - this.startTime) / 1000;
    const avgSpeed = fileSize / elapsed;

    this.log(`Download complete! ${this.formatBytes(fileSize)} in ${this.formatTime(elapsed)}`, 'success');

    const modal = document.getElementById('completeModal');
    if (modal) {
      modal.style.display = 'flex';
      document.getElementById('completedFilename').textContent = this.jobData.title || 'Stream';
      document.getElementById('completedSize').textContent = this.formatBytes(fileSize);
      document.getElementById('completedTime').textContent = this.formatTime(elapsed);
      document.getElementById('completedSpeed').textContent = `${(avgSpeed / 1024 / 1024).toFixed(1)} MB/s avg`;
    }

    document.getElementById('closeModal')?.addEventListener('click', () => {
      window.close();
    });
  }

  showError(message) {
    this.log(message, 'error');
    this.updateUI({
      statusTitle: 'Error',
      statusDescription: message
    });

    const spinner = document.getElementById('spinner');
    if (spinner) spinner.style.display = 'none';
  }

  updateUI(updates) {
    for (const [key, value] of Object.entries(updates)) {
      const el = document.getElementById(key);
      if (el) {
        el.textContent = value;
      }
    }
  }

  log(message, level = 'info') {
    console.log(`[MuxNet] ${message}`);

    const logContainer = document.getElementById('logContainer');
    if (logContainer) {
      const entry = document.createElement('div');
      entry.className = `log-entry ${level}`;
      const timestamp = new Date().toLocaleTimeString();
      entry.textContent = `[${timestamp}] ${message}`;
      logContainer.appendChild(entry);
      logContainer.scrollTop = logContainer.scrollHeight;
    }
  }

  formatBytes(bytes) {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return (bytes / Math.pow(k, i)).toFixed(1) + ' ' + sizes[i];
  }

  formatTime(seconds) {
    if (!isFinite(seconds) || seconds < 0) return 'Calculating...';
    if (seconds < 60) return `${Math.round(seconds)}s`;
    const minutes = Math.floor(seconds / 60);
    const secs = Math.round(seconds % 60);
    return `${minutes}m ${secs}s`;
  }
}
