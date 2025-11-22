type StrategyResponse = { available_strategies?: string[]; default_strategy?: string };
type ApiResult = { results?: any[]; mode?: string } & Record<string, any>;

const els = {
  question: document.getElementById('question') as HTMLTextAreaElement,
  btnSubmit: document.getElementById('btnSubmit') as HTMLButtonElement,
  messages: document.getElementById('messages') as HTMLUListElement,
  // Removed hidden controls, using inline buttons
  toastContainer: document.getElementById('toastContainer') as HTMLDivElement,
  loadingOverlay: document.getElementById('loadingOverlay') as HTMLDivElement,
  modeToggle: document.getElementById('modeToggle') as HTMLButtonElement,
  strategyDropdown: document.getElementById('strategyDropdown') as HTMLDivElement,
  strategyButton: document.getElementById('strategyButton') as HTMLButtonElement,
  strategyMenu: document.getElementById('strategyMenu') as HTMLUListElement,
  strategyLabel: document.getElementById('strategyLabel') as HTMLSpanElement,
  stepBuilder: document.getElementById('stepBuilder') as HTMLDivElement,
  stepsList: document.getElementById('stepsList') as HTMLDivElement,
  addStepBtn: document.getElementById('addStepBtn') as HTMLButtonElement,
  stepCount: document.getElementById('stepCount') as HTMLSpanElement,
};

let currentMode: 'read' | 'write' = 'read';
let currentStrategy: 'postgres' | 'mongodb' = 'mongodb';
let manualSteps: Array<{ id: number; question: string; database: 'mongodb' | 'postgres' }> = [];
let stepIdCounter = 0;
// Enhanced chat logic: avatars, typing indicator, timestamps.

function getMode(): 'read' | 'write' { return currentMode; }

function clearChat() {
  els.messages.innerHTML = '';
}

function addMessage(role: 'user' | 'assistant', content: string, opts: { html?: boolean; typing?: boolean; error?: boolean } = {}) {
  const li = document.createElement('li');
  li.className = 'message';
  const row = document.createElement('div');
  row.className = 'row ' + (role === 'assistant' ? 'assistant' : 'user');
  const dot = document.createElement('div');
  dot.className = 'dot';
  const bubble = document.createElement('div');
  bubble.className = 'bubble' + (opts.error ? ' error' : '');
  if (opts.typing) {
    bubble.innerHTML = `<div class="typing"><span class="typing-dot"></span><span class="typing-dot"></span><span class="typing-dot"></span></div>`;
  } else if (opts.html) {
    bubble.innerHTML = content;
  } else {
    bubble.textContent = content;
  }
  if (role === 'user') {
    row.appendChild(dot);
    row.appendChild(bubble);
  } else {
    row.appendChild(bubble);
    row.appendChild(dot);
  }
  li.appendChild(row);
  els.messages.appendChild(li);
  li.scrollIntoView({ behavior: 'smooth', block: 'end' });
  return bubble;
}

function renderResultBubble(raw: any) {
  const data: ApiResult = raw as ApiResult;
  const results = Array.isArray(data?.results) ? data.results : (Array.isArray(data) ? data : null);
  if (!results || !results.length) return `<pre>${escapeHtml(JSON.stringify(raw, null, 2))}</pre>`;
  if (isArrayOfObjects(results)) return renderTable(results, data.mode || undefined);
  const rows = results.map((v: unknown) => ({ value: v }));
  return renderTable(rows, data.mode || undefined);
}

function isArrayOfObjects(arr: any[]): boolean {
  return arr.every(v => v && typeof v === 'object' && !Array.isArray(v));
}

function renderTable(rows: Record<string, any>[], caption?: string): string {
  // Collect all keys across rows
  const allKeys = Array.from(new Set(rows.flatMap(r => Object.keys(r))));
  if (allKeys.length === 0) return '<div class="placeholder">(empty)</div>';
  const header = allKeys.map(k => `<th>${escapeHtml(k)}</th>`).join('');
  const body = rows.map(r => `<tr>${allKeys.map(k => `<td>${escapeHtml(fmtValue(r[k]))}</td>`).join('')}</tr>`).join('');
  return `<table>${caption ? `<caption style="caption-side:top;text-align:left;font-weight:600;padding:4px 0">${escapeHtml(caption)}</caption>` : ''}<thead><tr>${header}</tr></thead><tbody>${body}</tbody></table>`;
}

function fmtValue(v: any): string {
  if (v === null || v === undefined) return '';
  if (typeof v === 'object') return JSON.stringify(v);
  return String(v);
}

function escapeHtml(str: string): string {
  return str
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

async function fetchJSON<T>(url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(url, { headers: { 'Content-Type': 'application/json' }, ...init });
  if (!res.ok) throw new Error(`HTTP ${res.status}: ${await res.text()}`);
  return res.json();
}

function setStrategies(list: string[], defaultStrategy?: string) {
  if (!els.strategyMenu || !els.strategyLabel) return;
  const unique = Array.from(new Set(list.filter(v => v === 'postgres' || v === 'mongodb')));
  if (!unique.length) unique.push('mongodb', 'postgres');
  if (defaultStrategy === 'postgres' || defaultStrategy === 'mongodb') currentStrategy = defaultStrategy;
  els.strategyMenu.innerHTML = unique.map(v => {
    const cap = v === 'postgres' ? 'Postgres' : 'MongoDB';
    const file = v === 'postgres' ? 'postgres' : 'mongodb';
    const selected = v === currentStrategy;
    return `<li role="option" data-value="${v}" aria-selected="${selected}"><img src="/assets/${file}.svg" alt="" class="logo" /><span>${cap}</span></li>`;
  }).join('');
  updateStrategyUI();
}

async function loadStrategies() {
  try {
    const data = await fetchJSON<StrategyResponse>('/api/query/strategies');
    setStrategies(data.available_strategies ?? [], data.default_strategy);
  } catch (err) {
    console.error('Failed to load strategies', err);
    const fallback = ['postgres', 'mongodb'];
    setStrategies(fallback, 'postgres');
    // System message fallback rendered as assistant
    addMessage('assistant', escapeHtml('Using fallback strategies (backend error): ' + fallback.join(', ')));
  }
}

async function submit() {
  const mode = getMode();
  const rawText = els.question.value.trim();
  
  let body: any;
  if (mode === 'write') {
    // Use manual steps if any exist, otherwise parse text
    if (manualSteps.length > 0) {
      const steps = manualSteps.map(s => ({ question: s.question, database: s.database }));
      body = { steps };
    } else if (rawText) {
      body = buildWritePayload(rawText);
    } else {
      toast('Enter a message or add steps', 'error');
      return;
    }
  } else {
    if (!rawText) {
      toast('Enter a message', 'error');
      return;
    }
    body = { question: rawText, database: currentStrategy };
  }
  
  const displayText = manualSteps.length > 0 
    ? `${manualSteps.length} saga steps` 
    : rawText;
  addMessage('user', escapeHtml(displayText));
  els.btnSubmit.disabled = true;
  const thinkingBubble = addMessage('assistant', '', { typing: true });
  const url = mode === 'write' ? '/api/query/write' : '/api/query';
  try {
    showLoading(true);
    const data = await fetchJSON<any>(url, { method: 'POST', body: JSON.stringify(body) });
    thinkingBubble.classList.remove('error');
    const contentHtml = renderResultBubble(data);
    const trimmed = contentHtml.trim();
    thinkingBubble.innerHTML = trimmed;
    // If assistant reply is just a table, drop bubble styling
    if (trimmed.startsWith('<table')) {
      thinkingBubble.classList.remove('bubble');
      thinkingBubble.classList.remove('error');
      thinkingBubble.classList.add('assistant-table');
    }
    toast('Done','success');
    // Clear manual steps after successful submit
    if (mode === 'write' && manualSteps.length > 0) {
      manualSteps = [];
      renderSteps();
    }
  } catch (err) {
    thinkingBubble.classList.add('error');
    thinkingBubble.innerHTML = '<pre>' + escapeHtml(String(err)) + '</pre>';
    toast('Failed','error');
  } finally {
    showLoading(false);
  }
  els.question.value = '';
  autosizeTextarea();
  els.btnSubmit.disabled = false;
}

function init() {
  els.btnSubmit.addEventListener('click', submit);
  // Mode quick toggle button
  els.modeToggle?.addEventListener('click', () => {
    toggleMode();
  });
  updateModeToggle();
  // Strategy dropdown interactions
  els.strategyButton?.addEventListener('click', () => {
    toggleStrategyMenu();
  });
  els.strategyMenu?.addEventListener('click', (e) => {
    const li = (e.target as HTMLElement).closest('li[data-value]');
    if (!li) return;
    const val = li.getAttribute('data-value') as 'postgres' | 'mongodb';
    if (val && val !== currentStrategy) {
      currentStrategy = val;
      toast('Strategy: ' + (val === 'postgres' ? 'Postgres' : 'MongoDB'), 'info');
      updateStrategyUI();
    }
    closeStrategyMenu();
  });
  // Step builder controls
  els.addStepBtn?.addEventListener('click', addStep);
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') closeStrategyMenu();
  });
  document.addEventListener('click', (e) => {
    if (!els.strategyDropdown) return;
    if (!els.strategyDropdown.contains(e.target as Node)) closeStrategyMenu();
  });
  loadStrategies();
  renderSteps();
}

clearChat();
init();

function toast(message: string, type: 'success' | 'error' | 'info' = 'info', timeout = 3000) {
  const el = document.createElement('div');
  el.className = `toast ${type}`;
  el.innerHTML = `<span>${escapeHtml(message)}</span><button class="close" aria-label="Close">×</button>`;
  const close = () => { el.remove(); };
  el.querySelector('.close')?.addEventListener('click', close);
  els.toastContainer.appendChild(el);
  setTimeout(close, timeout);
}

function showLoading(on: boolean) { els.loadingOverlay.classList.toggle('hidden', !on); }

// History functionality removed.

// Auto resize textarea
function autosizeTextarea() {
  els.question.style.height = 'auto';
  els.question.style.height = Math.min(160, els.question.scrollHeight) + 'px';
}
els.question.addEventListener('input', autosizeTextarea);
els.question.addEventListener('keydown', (e: KeyboardEvent) => {
  if ((e.key === 'Enter' && e.ctrlKey) || (e.key === 'Enter' && e.metaKey)) {
    e.preventDefault(); submit();
  }
});

// Removed toggleStrategy & updateDbToggle; using select instead.
function updateStrategyUI() {
  if (!els.strategyLabel || !els.strategyButton || !els.strategyMenu) return;
  const cap = currentStrategy === 'postgres' ? 'Postgres' : 'MongoDB';
  const file = currentStrategy === 'postgres' ? 'postgres' : 'mongodb';
  els.strategyLabel.textContent = cap;
  els.strategyButton.querySelector('img.logo')?.setAttribute('src', `/assets/${file}.svg`);
  els.strategyButton.title = 'Strategy: ' + cap;
  els.strategyButton.setAttribute('aria-label', 'Database strategy: ' + cap);
  els.strategyMenu.querySelectorAll('li[role="option"]').forEach(li => {
    const val = li.getAttribute('data-value');
    li.setAttribute('aria-selected', val === currentStrategy ? 'true' : 'false');
  });
}

function toggleStrategyMenu() {
  if (!els.strategyDropdown) return;
  const expanded = els.strategyDropdown.getAttribute('aria-expanded') === 'true';
  els.strategyDropdown.setAttribute('aria-expanded', expanded ? 'false' : 'true');
}
function closeStrategyMenu() {
  if (!els.strategyDropdown) return;
  els.strategyDropdown.setAttribute('aria-expanded', 'false');
}

function toggleMode() {
  currentMode = currentMode === 'read' ? 'write' : 'read';
  updateModeToggle();
  toast('Mode: ' + currentMode, 'info');
}

function updateModeToggle() {
  if (!els.modeToggle) return;
  els.modeToggle.setAttribute('data-mode', currentMode);
  els.modeToggle.title = 'Mode: ' + currentMode + ' (click to switch)';
  els.modeToggle.setAttribute('aria-label', 'Mode: ' + currentMode);
  // Dynamic placeholder based on mode
  if (currentMode === 'read') {
    els.question.placeholder = 'type your read message here';
    // Show strategy dropdown for read mode
    els.strategyDropdown?.classList.remove('hidden');
    // Hide step builder
    els.stepBuilder?.classList.add('hidden');
  } else {
    els.question.placeholder = 'type your write message here (e.g. "in mongodb insert ... in postgres insert ...")';
    // Hide strategy dropdown in write mode
    els.strategyDropdown?.classList.add('hidden');
    // Show step builder
    els.stepBuilder?.classList.remove('hidden');
  }
}

// Step builder functions
function addStep() {
  if (manualSteps.length >= 10) {
    toast('Maximum 10 steps allowed', 'error');
    return;
  }
  const id = stepIdCounter++;
  manualSteps.push({ id, question: '', database: 'mongodb' });
  renderSteps();
  toast('Step added', 'info');
}

function removeStep(id: number) {
  manualSteps = manualSteps.filter(s => s.id !== id);
  renderSteps();
  toast('Step removed', 'info');
}

function updateStepQuestion(id: number, question: string) {
  const step = manualSteps.find(s => s.id === id);
  if (step) step.question = question;
}

function updateStepDatabase(id: number, database: 'mongodb' | 'postgres') {
  const step = manualSteps.find(s => s.id === id);
  if (step) step.database = database;
  renderSteps();
}

function renderSteps() {
  if (!els.stepsList || !els.stepCount || !els.addStepBtn) return;
  els.stepCount.textContent = String(manualSteps.length);
  els.addStepBtn.disabled = manualSteps.length >= 10;
  
  if (manualSteps.length === 0) {
    els.stepsList.innerHTML = '<div style="text-align:center;color:var(--ctp-overlay1);font-size:13px;padding:20px;">No steps yet. Click "+ Add Step" or type naturally.</div>';
    return;
  }
  
  els.stepsList.innerHTML = manualSteps.map((step, idx) => `
    <div class="step-item" data-id="${step.id}">
      <div class="step-number">${idx + 1}</div>
      <div class="step-content">
        <textarea class="step-input" placeholder="Enter query..." data-id="${step.id}">${escapeHtml(step.question)}</textarea>
        <div class="step-db-select">
          <div class="step-db-option ${step.database === 'mongodb' ? 'active' : ''}" data-id="${step.id}" data-db="mongodb">
            <img src="/assets/mongodb.svg" alt="" />
            <span>MongoDB</span>
          </div>
          <div class="step-db-option ${step.database === 'postgres' ? 'active' : ''}" data-id="${step.id}" data-db="postgres">
            <img src="/assets/postgres.svg" alt="" />
            <span>Postgres</span>
          </div>
        </div>
      </div>
      <button class="step-remove" data-id="${step.id}">×</button>
    </div>
  `).join('');
  
  // Attach event listeners
  els.stepsList.querySelectorAll('.step-input').forEach(textarea => {
    const id = parseInt((textarea as HTMLElement).dataset.id || '0');
    textarea.addEventListener('input', (e) => {
      updateStepQuestion(id, (e.target as HTMLTextAreaElement).value);
    });
  });
  
  els.stepsList.querySelectorAll('.step-db-option').forEach(option => {
    option.addEventListener('click', () => {
      const id = parseInt((option as HTMLElement).dataset.id || '0');
      const db = (option as HTMLElement).dataset.db as 'mongodb' | 'postgres';
      updateStepDatabase(id, db);
    });
  });
  
  els.stepsList.querySelectorAll('.step-remove').forEach(btn => {
    btn.addEventListener('click', () => {
      const id = parseInt((btn as HTMLElement).dataset.id || '0');
      removeStep(id);
    });
  });
}

function buildWritePayload(text: string): any {
  const steps: { question: string; database: 'mongodb' | 'postgres' }[] = [];
  
  // Fuzzy match for typos
  const fuzzyMatch = (word: string): 'mongodb' | 'postgres' | undefined => {
    const w = word.toLowerCase();
    if (/^(mongodb?|mong)$/i.test(w)) return 'mongodb';
    if (/^(postgres|postgresql|pg|postgre)$/i.test(w)) return 'postgres';
    if (levenshtein(w, 'mongo') <= 2 || levenshtein(w, 'mongodb') <= 2) return 'mongodb';
    if (levenshtein(w, 'postgres') <= 2 || levenshtein(w, 'pg') <= 1) return 'postgres';
    return undefined;
  };

  // Split by common separators that indicate different operations
  const rawParts = text.split(/\s+(?:and|;)\s+(?=\w)/gi);
  
  for (const part of rawParts) {
    const trimmed = part.trim();
    if (!trimmed) continue;
    
    // Find database mention in this part
    const dbPattern = /\b(?:in|into|on)\s+(\S+)/i;
    const match = dbPattern.exec(trimmed);
    
    if (match) {
      const db = fuzzyMatch(match[1]);
      if (db) {
        // Remove the database mention from query
        let query = trimmed.substring(0, match.index).trim() + ' ' + trimmed.substring(match.index + match[0].length).trim();
        query = query.trim();
        
        if (query) {
          steps.push({ question: query, database: db });
        }
      }
    }
  }

  if (steps.length > 10) {
    toast('Max 10 steps allowed, truncating','error');
    steps.splice(10);
  }
  if (steps.length) {
    toast('Saga steps: ' + steps.length, 'info');
    return { steps };
  }
  return { question: text };
}

// Levenshtein distance for typo tolerance
function levenshtein(a: string, b: string): number {
  const matrix: number[][] = [];
  for (let i = 0; i <= b.length; i++) matrix[i] = [i];
  for (let j = 0; j <= a.length; j++) matrix[0][j] = j;
  for (let i = 1; i <= b.length; i++) {
    for (let j = 1; j <= a.length; j++) {
      if (b.charAt(i - 1) === a.charAt(j - 1)) {
        matrix[i][j] = matrix[i - 1][j - 1];
      } else {
        matrix[i][j] = Math.min(
          matrix[i - 1][j - 1] + 1,
          matrix[i][j - 1] + 1,
          matrix[i - 1][j] + 1
        );
      }
    }
  }
  return matrix[b.length][a.length];
}

// Removed action bar & raw toggle for minimal UI.
