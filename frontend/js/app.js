// Cloudfloe Frontend Application
class CloudfloeApp {
    constructor() {
        this.editor = null;
        this.queryHistory = [];
        this.currentConnection = null;
        this.mockMode = false; // Backend is now connected!
        this.backendUrl = ''; // Use relative paths with nginx proxy
        this.init();
    }

    init() {
        this.initEditor();
        this.bindEvents();
        this.loadQueryHistory();
    }

    initEditor() {
        const editorElement = document.getElementById('sqlEditor');
        this.editor = CodeMirror(editorElement, {
            mode: 'text/x-sql',
            theme: 'monokai',
            lineNumbers: true,
            autofocus: true,
            extraKeys: {
                'Ctrl-Enter': () => this.runQuery(),
                'Cmd-Enter': () => this.runQuery(),
                'Ctrl-Space': 'autocomplete'
            },
            value: '-- Welcome to Cloudfloe!\n-- Connect to your data source and click a sample query to get started\n\n-- Or write your own SQL query here...'
        });
    }

    bindEvents() {
        // Connection events
        document.getElementById('storageType').addEventListener('change', (e) => {
            this.updateConnectionFields(e.target.value);
        });

        document.getElementById('testConnectionBtn').addEventListener('click', () => {
            this.testConnection();
        });

        // Query events
        document.getElementById('runQueryBtn').addEventListener('click', () => {
            this.runQuery();
        });

        document.getElementById('formatBtn').addEventListener('click', () => {
            this.formatSQL();
        });



        // Tab switching
        document.querySelectorAll('.tab-btn').forEach(btn => {
            btn.addEventListener('click', (e) => {
                this.switchTab(e.target.dataset.tab);
            });
        });

        // Sample queries
        document.querySelectorAll('.query-card').forEach(card => {
            card.addEventListener('click', (e) => {
                const query = e.currentTarget.dataset.query;
                this.editor.setValue(query);
            });
        });

        // Export
        document.getElementById('exportBtn').addEventListener('click', () => {
            this.exportResults();
        });
    }

    updateConnectionFields(storageType) {
        const sessionTokenGroup = document.getElementById('sessionTokenGroup');
        const endpoint = document.getElementById('endpoint');

        switch(storageType) {
            case 's3':
                sessionTokenGroup.style.display = 'block';
                endpoint.placeholder = 's3://bucket/path/to/table';
                break;
            case 'r2':
                sessionTokenGroup.style.display = 'none';
                endpoint.placeholder = 'https://account.r2.cloudflarestorage.com';
                break;
            case 'minio':
                sessionTokenGroup.style.display = 'none';
                endpoint.placeholder = 'http://localhost:9000';
                break;
        }
    }

    async testConnection() {
        const connection = {
            storageType: document.getElementById('storageType').value,
            endpoint: document.getElementById('endpoint').value,
            accessKey: document.getElementById('accessKey').value,
            secretKey: document.getElementById('secretKey').value,
            sessionToken: document.getElementById('sessionToken').value,
            region: document.getElementById('region').value,
            tablePath: document.getElementById('tablePath').value
        };

        const statusDiv = document.getElementById('connectionStatus');
        statusDiv.innerHTML = '<div class="status-message">Testing connection...</div>';

        try {
            if (this.mockMode) {
                // Mock response
                await this.sleep(1000);
                this.currentConnection = connection;
                statusDiv.innerHTML = '<div class="status-message status-success">✓ Connection successful</div>';
                this.addRecentConnection(connection);
            } else {
                const response = await fetch(`${this.backendUrl}/api/connect/test`, {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({connection})
                });

                if (response.ok) {
                    const data = await response.json();
                    this.currentConnection = connection;
                    statusDiv.innerHTML = '<div class="status-message status-success">✓ Connection successful</div>';
                    this.addRecentConnection(connection);

                    // If we got table info, show it in the editor
                    if (data.tableInfo && data.tableInfo.suggestedQuery) {
                        this.editor.setValue(data.tableInfo.suggestedQuery);
                        statusDiv.innerHTML += `<div style="margin-top: 0.5rem; color: #10b981; font-size: 0.875rem;">Table found: ${data.tableInfo.path}<br>Sample query loaded in editor</div>`;
                    }
                } else {
                    throw new Error('Connection failed');
                }
            }
        } catch (error) {
            statusDiv.innerHTML = `<div class="status-message status-error">✗ ${error.message}</div>`;
        }
    }

    async runQuery() {
        const query = this.editor.getValue();
        if (!query.trim()) return;

        if (!this.currentConnection && !this.mockMode) {
            alert('Please connect to a data source first');
            return;
        }

        const queryStatus = document.getElementById('queryStatus');
        queryStatus.textContent = 'Running query...';

        const startTime = Date.now();

        try {
            let results;
            if (this.mockMode) {
                // Mock results
                await this.sleep(1500);
                results = this.generateMockResults(query);
            } else {
                const response = await fetch(`${this.backendUrl}/api/query`, {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({
                        sql: query,
                        connection: this.currentConnection,
                        rowLimit: 1000
                    })
                });

                if (!response.ok) {
                    const error = await response.json();
                    throw new Error(error.detail || 'Query failed');
                }

                results = await response.json();
            }

            const endTime = Date.now();

            this.displayResults(results);

            // Handle both mock and real backend responses
            const stats = results.stats || {};
            this.updateStats({
                execTime: stats.executionTimeMs ? `${stats.executionTimeMs}ms` : `${endTime - startTime}ms`,
                bytesScanned: this.formatBytes(stats.bytesScanned || results.bytesScanned || 1024000),
                rowsReturned: stats.rowsReturned || results.rows?.length || 0
            });

            queryStatus.textContent = `Query completed in ${endTime - startTime}ms`;
            this.addToHistory(query);

        } catch (error) {
            queryStatus.textContent = `Error: ${error.message}`;
            this.displayError(error.message);
        }
    }

    generateMockResults(query) {
        // Generate mock data based on query
        const isCount = query.toLowerCase().includes('count(');
        const hasLimit = query.toLowerCase().includes('limit');

        if (isCount) {
            return {
                columns: ['count'],
                rows: [[42567]],
                bytesScanned: 5242880
            };
        }

        return {
            columns: ['id', 'title', 'year', 'rating', 'genre'],
            rows: [
                [1, 'The Shawshank Redemption', 1994, 9.3, 'Drama'],
                [2, 'The Godfather', 1972, 9.2, 'Crime'],
                [3, 'The Dark Knight', 2008, 9.0, 'Action'],
                [4, 'Pulp Fiction', 1994, 8.9, 'Crime'],
                [5, 'Forrest Gump', 1994, 8.8, 'Drama'],
                [6, 'Inception', 2010, 8.8, 'Sci-Fi'],
                [7, 'The Matrix', 1999, 8.7, 'Sci-Fi'],
                [8, 'Goodfellas', 1990, 8.7, 'Crime'],
                [9, 'Se7en', 1995, 8.6, 'Thriller'],
                [10, 'The Silence of the Lambs', 1991, 8.6, 'Thriller']
            ].slice(0, hasLimit ? 5 : 10),
            bytesScanned: 2097152
        };
    }

    displayResults(results) {
        const resultsTable = document.getElementById('resultsTable');
        const rowCount = document.getElementById('rowCount');

        if (!results.rows || results.rows.length === 0) {
            resultsTable.innerHTML = '<div class="empty-state"><p>No results returned</p></div>';
            rowCount.textContent = '0 rows';
            return;
        }

        let html = '<table><thead><tr>';
        results.columns.forEach(col => {
            html += `<th>${col}</th>`;
        });
        html += '</tr></thead><tbody>';

        results.rows.forEach(row => {
            html += '<tr>';
            row.forEach(cell => {
                html += `<td>${cell !== null ? cell : 'NULL'}</td>`;
            });
            html += '</tr>';
        });

        html += '</tbody></table>';
        resultsTable.innerHTML = html;
        rowCount.textContent = `${results.rows.length} rows`;
    }

    displayError(message) {
        const resultsTable = document.getElementById('resultsTable');
        resultsTable.innerHTML = `<div class="empty-state"><p style="color: var(--error)">Error: ${message}</p></div>`;
    }

    updateStats(stats) {
        document.getElementById('execTime').textContent = stats.execTime;
        document.getElementById('bytesScanned').textContent = stats.bytesScanned;
        document.getElementById('rowsReturned').textContent = stats.rowsReturned;
    }

    switchTab(tab) {
        document.querySelectorAll('.tab-btn').forEach(btn => {
            btn.classList.toggle('active', btn.dataset.tab === tab);
        });

        document.getElementById('resultsTab').style.display = tab === 'results' ? 'block' : 'none';
        document.getElementById('statsTab').style.display = tab === 'stats' ? 'block' : 'none';
    }


    formatSQL() {
        // Basic SQL formatting
        let sql = this.editor.getValue();

        // Simple formatting rules
        sql = sql.replace(/\s+/g, ' '); // Normalize whitespace
        sql = sql.replace(/,/g, ',\n    '); // New line after commas
        sql = sql.replace(/\sFROM\s/gi, '\nFROM ');
        sql = sql.replace(/\sWHERE\s/gi, '\nWHERE ');
        sql = sql.replace(/\sGROUP BY\s/gi, '\nGROUP BY ');
        sql = sql.replace(/\sORDER BY\s/gi, '\nORDER BY ');
        sql = sql.replace(/\sLIMIT\s/gi, '\nLIMIT ');
        sql = sql.replace(/\sAND\s/gi, '\n  AND ');
        sql = sql.replace(/\sOR\s/gi, '\n  OR ');

        this.editor.setValue(sql);
    }


    addToHistory(query) {
        this.queryHistory.unshift({
            query,
            timestamp: Date.now()
        });

        // Keep only last 20 queries
        if (this.queryHistory.length > 20) {
            this.queryHistory.pop();
        }

        localStorage.setItem('queryHistory', JSON.stringify(this.queryHistory));
        this.renderHistory();
    }

    loadQueryHistory() {
        this.queryHistory = JSON.parse(localStorage.getItem('queryHistory') || '[]');
        this.renderHistory();
    }

    renderHistory() {
        const historyDiv = document.getElementById('queryHistory');

        if (this.queryHistory.length === 0) {
            historyDiv.innerHTML = '<p style="color: var(--text-muted); font-size: 0.875rem;">No queries yet</p>';
            return;
        }

        let html = '';
        this.queryHistory.slice(0, 10).forEach(item => {
            const time = new Date(item.timestamp).toLocaleTimeString();
            html += `
                <div class="history-item" onclick="app.editor.setValue(\`${item.query.replace(/`/g, '\\`')}\`)">
                    <div class="history-time">${time}</div>
                    <div class="history-query">${item.query}</div>
                </div>
            `;
        });

        historyDiv.innerHTML = html;
    }

    addRecentConnection(connection) {
        const recent = JSON.parse(localStorage.getItem('recentConnections') || '[]');

        // Add to beginning, remove duplicates
        const filtered = recent.filter(c => c.endpoint !== connection.endpoint);
        filtered.unshift({
            endpoint: connection.endpoint,
            storageType: connection.storageType,
            timestamp: Date.now()
        });

        // Keep only 5 recent
        if (filtered.length > 5) filtered.pop();

        localStorage.setItem('recentConnections', JSON.stringify(filtered));
        this.renderRecentConnections();
    }

    renderRecentConnections() {
        const recent = JSON.parse(localStorage.getItem('recentConnections') || '[]');
        const listDiv = document.getElementById('recentConnectionsList');

        if (recent.length === 0) {
            listDiv.innerHTML = '<p style="color: var(--text-muted); font-size: 0.875rem;">No recent connections</p>';
            return;
        }

        let html = '';
        recent.forEach(conn => {
            html += `<div class="recent-connection" style="padding: 0.5rem 0; cursor: pointer; font-size: 0.875rem;">
                ${conn.storageType.toUpperCase()}: ${conn.endpoint}
            </div>`;
        });

        listDiv.innerHTML = html;
    }

    exportResults() {
        const table = document.querySelector('.results-table table');
        if (!table) {
            alert('No results to export');
            return;
        }

        let csv = '';

        // Headers
        const headers = Array.from(table.querySelectorAll('thead th'))
            .map(th => th.textContent);
        csv += headers.join(',') + '\n';

        // Rows
        table.querySelectorAll('tbody tr').forEach(tr => {
            const row = Array.from(tr.querySelectorAll('td'))
                .map(td => `"${td.textContent}"`);
            csv += row.join(',') + '\n';
        });

        // Download
        const blob = new Blob([csv], { type: 'text/csv' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `cloudfloe-results-${Date.now()}.csv`;
        a.click();
        URL.revokeObjectURL(url);
    }

    formatBytes(bytes) {
        if (bytes === 0) return '0 B';
        const k = 1024;
        const sizes = ['B', 'KB', 'MB', 'GB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    }

    sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}

// Initialize app when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    window.app = new CloudfloeApp();
});