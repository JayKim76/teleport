let metricsInterval;

document.addEventListener("DOMContentLoaded", () => {
    const setupForm = document.getElementById('setup-form');
    const btnReplication = document.getElementById('btn-start-replication');
    const btnPlanning = document.getElementById('btn-proceed-planning');
    const btnProceed = document.getElementById('btn-proceed-execution');
    
    // Step 1 -> Step 2
    if (setupForm) {
        setupForm.addEventListener('submit', async (e) => {
            e.preventDefault();
            
            const btn = setupForm.querySelector('button');
            btn.innerText = "Connecting...";
            btn.disabled = true;

            setTimeout(() => {
                document.getElementById('connection-setup').style.display = 'none';
                document.getElementById('assessment-view').style.display = 'block';
                
                document.querySelector('.pipeline li:nth-child(1)').classList.remove('active', 'border-glow');
                document.querySelector('.pipeline li:nth-child(2)').classList.add('active', 'border-glow');
            }, 800);
        });
    }

    // Step 2 -> Step 3
    if (btnReplication) {
        btnReplication.addEventListener('click', () => {
            document.getElementById('assessment-view').style.display = 'none';
            document.getElementById('schema-view').style.display = 'block';
            
            document.querySelector('.pipeline li:nth-child(2)').classList.remove('active', 'border-glow');
            document.querySelector('.pipeline li:nth-child(3)').classList.add('active', 'border-glow');
            
            simulateSchemaReplication();
        });
    }

    // Step 3 -> Step 4
    if (btnPlanning) {
        btnPlanning.addEventListener('click', () => {
            document.getElementById('schema-view').style.display = 'none';
            document.getElementById('planning-view').style.display = 'block';
            
            document.querySelector('.pipeline li:nth-child(3)').classList.remove('active', 'border-glow');
            document.querySelector('.pipeline li:nth-child(4)').classList.add('active', 'border-glow');
        });
    }

    // Step 4 -> Step 5
    if (btnProceed) {
        btnProceed.addEventListener('click', async () => {
            btnProceed.innerText = "Starting...";
            btnProceed.disabled = true;

            try {
                // Call backend API to start migration simulation
                const res = await fetch('/api/start', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ 
                        host: document.getElementById('host') ? document.getElementById('host').value : "",
                    })
                });
                
                if (res.ok) {
                    // Switch to Dashboard View
                    document.getElementById('planning-view').style.display = 'none';
                    document.getElementById('dashboard-view').style.display = 'flex';
                    
                    // Update sidebar active state (Step 4 -> Step 5)
                    document.querySelector('.pipeline li:nth-child(4)').classList.remove('active', 'border-glow');
                    document.querySelector('.pipeline li:nth-child(5)').classList.add('active', 'border-glow');

                    // Start metrics polling
                    fetchMetrics();
                    metricsInterval = setInterval(fetchMetrics, 2000);
                } else {
                    throw new Error("Failed to start migration");
                }
            } catch (err) {
                console.error("Failed to start", err);
                alert("Failed to start migration simulation.");
                btnProceed.innerText = "Approve Plan & Proceed to Execution";
                btnProceed.disabled = false;
            }
        });
    }

    // Step 5 -> Step 6
    const btnValidation = document.getElementById('btn-proceed-validation');
    if (btnValidation) {
        btnValidation.addEventListener('click', () => {
            clearInterval(metricsInterval);
            document.getElementById('dashboard-view').style.display = 'none';
            document.getElementById('validation-view').style.display = 'block';
            
            document.querySelector('.pipeline li:nth-child(5)').classList.remove('active', 'border-glow');
            document.querySelector('.pipeline li:nth-child(6)').classList.add('active', 'border-glow');
            
            simulateValidation();
        });
    }

    // Step 6 -> Step 7
    const btnCutover = document.getElementById('btn-proceed-cutover');
    if (btnCutover) {
        btnCutover.addEventListener('click', () => {
            document.getElementById('validation-view').style.display = 'none';
            document.getElementById('cutover-view').style.display = 'block';
            
            document.querySelector('.pipeline li:nth-child(6)').classList.remove('active', 'border-glow');
            document.querySelector('.pipeline li:nth-child(7)').classList.add('active', 'border-glow');
        });
    }

    // Cutover & Fallback Actions
    const optCutover = document.getElementById('opt-cutover');
    const optFallback = document.getElementById('opt-fallback');
    const finalStatus = document.getElementById('final-status');
    const finalMessage = document.getElementById('final-message');

    if (optCutover && optFallback) {
        optCutover.addEventListener('click', () => {
            optCutover.style.display = 'none';
            optFallback.style.display = 'none';
            finalStatus.style.display = 'block';
            finalMessage.style.color = 'var(--neon-cyan)';
            finalMessage.innerText = 'Cutover Completed Successfully! 🎉';
            document.querySelector('.pipeline li:nth-child(7)').classList.remove('border-glow');
            document.querySelector('.pipeline li:nth-child(7)').style.color = '#00ffcc';
        });

        optFallback.addEventListener('click', () => {
            optCutover.style.display = 'none';
            optFallback.style.display = 'none';
            finalStatus.style.display = 'block';
            finalMessage.style.color = 'var(--neon-purple)';
            finalMessage.innerText = 'Fallback Executed Successfully. Reverted to Source DB.';
            document.querySelector('.pipeline li:nth-child(7)').classList.remove('border-glow');
            document.querySelector('.pipeline li:nth-child(7)').style.color = '#bc13fe';
        });
    }
});

function simulateSchemaReplication() {
    const logs = [
        "Connecting to Source Oracle DB...",
        "Extracting Data Dictionary...",
        "Found 1240 tables, 350 views.",
        "=> Converting Oracle NUMBER to PostgreSQL NUMERIC...",
        "=> Converting Oracle VARCHAR2 to VARCHAR...",
        "=> Converting Oracle DATE to TIMESTAMP...",
        "Applying DDL to Target DB...",
        "Creating Table TELEPORT_SRC... [OK]",
        "Creating Table user_auth... [OK]",
        "Creating Table db_prod_main... [OK]",
        "Creating Indexes... [OK]",
        "Creating Foreign Keys... [OK]",
        "Schema Replication Completed Successfully."
    ];
    
    const logContainer = document.getElementById('schema-logs');
    const progressBar = document.getElementById('schema-progress-bar');
    const progressText = document.getElementById('schema-progress-text');
    const proceedBtn = document.getElementById('btn-proceed-planning');
    
    let step = 0;
    const interval = setInterval(() => {
        if (step < logs.length) {
            const li = document.createElement('li');
            li.textContent = `> ${logs[step]}`;
            if (step === logs.length - 1) {
                li.style.color = '#00ffcc'; // neon cyan for success
            }
            logContainer.appendChild(li);
            logContainer.scrollTop = logContainer.scrollHeight;
            
            const progress = Math.min(100, Math.round(((step + 1) / logs.length) * 100));
            progressBar.style.width = `${progress}%`;
            progressText.innerText = `${progress}%`;
            
            step++;
        } else {
            clearInterval(interval);
            proceedBtn.disabled = false;
            proceedBtn.classList.add('neon-cyan'); // Highlight when done
        }
    }, 450); // Faster log steps
}

async function fetchMetrics() {
    try {
        const response = await fetch('/api/metrics');
        const data = await response.json();
        
        // Update top cards
        document.getElementById('val-total').innerText = data.total_tables.toLocaleString();
        document.getElementById('val-rate').innerText = data.transfer_rate;
        document.getElementById('val-time').innerText = data.elapsed_time;
        
        // Update circular progress
        const circle = document.getElementById('progress-circle');
        const text = document.getElementById('val-progress');
        const circumference = 2 * Math.PI * 45; // r=45
        const offset = circumference - (data.progress / 100) * circumference;
        circle.style.strokeDashoffset = offset;
        text.innerText = `${data.progress}%`;
        
        if (data.progress >= 100) {
            const btnVal = document.getElementById('btn-proceed-validation');
            if (btnVal) btnVal.style.display = 'block';
        }
        
        // Update Table
        const tbody = document.getElementById('table-body');
        tbody.innerHTML = '';
        data.syncing_tables.forEach(table => {
            const tr = document.createElement('tr');
            
            // Status color logic
            let statusClass = table.status === 'Completed' ? 'neon-cyan' : 'text-primary';
            
            tr.innerHTML = `
                <td><strong>${table.name}</strong></td>
                <td class="${statusClass}">${table.status}</td>
                <td>${table.rows}</td>
                <td>${table.speed}</td>
                <td style="width:150px">
                    <div class="bar-wrap">
                        <div class="bar-fill" style="width: ${table.progress}%"></div>
                    </div>
                </td>
            `;
            tbody.appendChild(tr);
        });
        
    } catch (err) {
        console.error("Error fetching metrics:", err);
    }
}

function simulateValidation() {
    const logs = [
        "Starting post-migration validation...",
        "Checking Row Counts [Source vs Target]...",
        "Row count match: 100% (45,210,000 / 45,210,000)",
        "Calculating SHA-256 data checksums on sample blocks...",
        "Data checksums: Verified",
        "Validating Object status (Constraints, Indexes, Triggers)...",
        "Invalid objects detected: 0",
        "Performance Index Testing... [OK]",
        "Validation Sequence Completed Successfully."
    ];
    
    const logContainer = document.getElementById('validation-logs');
    const btnCutover = document.getElementById('btn-proceed-cutover');
    
    let step = 0;
    const interval = setInterval(() => {
        if (step < logs.length) {
            const li = document.createElement('li');
            li.textContent = `> ${logs[step]}`;
            if (step === logs.length - 1) {
                li.style.color = '#00ffcc'; // neon cyan for success
                document.getElementById('val-row-match').innerText = "100%";
                document.getElementById('val-checksum').innerText = "Verified";
                document.getElementById('val-invalid').innerText = "0";
            }
            logContainer.appendChild(li);
            logContainer.scrollTop = logContainer.scrollHeight;
            step++;
        } else {
            clearInterval(interval);
            btnCutover.disabled = false;
        }
    }, 600);
}

