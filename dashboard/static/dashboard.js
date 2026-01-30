// ========================== UPLOAD HANDLERS ==========================
function setLoading(buttonId, isLoading) {
    const btn = document.getElementById(buttonId);
    const text = btn.querySelector('.btn-text');
    const spinner = btn.querySelector('.spinner-border');
    if (isLoading) {
        btn.classList.add('disabled');
        text.textContent = 'Uploading...';
        spinner.classList.remove('d-none');
        showProgress(0);
    } else {
        btn.classList.remove('disabled');
        spinner.classList.add('d-none');
        hideProgress();
    }
}

function showProgress(percent) {
    const wrapper = document.getElementById('uploadProgressWrapper');
    const bar = document.getElementById('uploadProgressBar');
    wrapper.classList.remove('d-none');
    bar.style.width = percent + '%';
    bar.textContent = percent + '%';
}

function hideProgress() {
    setTimeout(() => document.getElementById('uploadProgressWrapper').classList.add('d-none'), 800);
}

function validateFiles(files, allowedExt) {
    for (const f of files) {
        const ext = f.name.split('.').pop().toLowerCase();
        if (!allowedExt.includes(ext) || f.size === 0) {
            alert(`Invalid or empty file: ${f.name}`);
            return false;
        }
    }
    return true;
}

async function uploadFiles(files, endpoint, buttonId) {
    const fd = new FormData();
    for (const f of files) fd.append('files[]', f);
    setLoading(buttonId, true);

    try {
        const res = await fetch(`/upload/${endpoint}`, { method: 'POST', body: fd });
        setLoading(buttonId, false);
        if (res.ok) {
            const data = await res.json();
            let msg = `Files processed: ${data.summary.files_total}\nSuccess: ${data.summary.files_ok}\nFailed: ${data.summary.files_error}\nRows imported: ${data.summary.rows_total}\n`;
            if (data.summary.min_date && data.summary.max_date) msg += `Date range: ${data.summary.min_date} → ${data.summary.max_date}\n`;
            msg += "\nDetails:\n";
            for (const r of data.results) msg += (r.status==="ok"?"✔ ":"❌ ")+`${r.file} — ${r.message}\n`;
            document.getElementById("uploadReportContent").textContent = msg;
            new bootstrap.Modal(document.getElementById("uploadReportModal")).show();
        } else alert('Upload failed');
    } catch (err) {
        setLoading(buttonId, false);
        alert('Upload error: ' + err);
    }
}

// Attach upload events
document.getElementById('bankUpload').addEventListener('change', e => {
    const files = e.target.files;
    if (!validateFiles(files,['csv'])) e.target.value=''; else uploadFiles(files,'bank','bankBtn');
});
document.getElementById("salesUpload").addEventListener("change", function () {
    const files = Array.from(this.files);

    const invalid = files.filter(f =>
        !f.name.startsWith("Vendas") || !f.name.toLowerCase().endsWith(".pdf")
    );

    if (invalid.length > 0) {
        alert(
            "Invalid file(s):\n" +
            invalid.map(f => f.name).join("\n") +
            "\n\nOnly Vendas*.pdf files are allowed."
        );
        this.value = "";
        return;
    }

    uploadFiles("sales", files);
});


// ========================== DEBIT CLASSIFICATIONS AJAX ==========================
const debitModal = new bootstrap.Modal(document.getElementById('debitModal'));
const debitForm = document.getElementById('debitForm');
let currentAction = 'add'; // add or edit

// Open Add Modal
document.getElementById('addDebitBtn').addEventListener('click', () => {
    currentAction = 'add';
    document.getElementById('debitModalTitle').textContent = 'Add Classification';
    document.getElementById('debitId').value = '';
    document.getElementById('debitDescription').value = '';
    document.getElementById('debitCategory').value = '';
    debitModal.show();
});

// Edit handler
function attachEditEvents() {
    document.querySelectorAll('.edit-btn').forEach(btn => {
        btn.onclick = () => {
            currentAction = 'edit';
            document.getElementById('debitModalTitle').textContent = 'Edit Classification';
            document.getElementById('debitId').value = btn.dataset.id;
            document.getElementById('debitDescription').value = btn.dataset.description;
            document.getElementById('debitCategory').value = btn.dataset.category;
            debitModal.show();
        };
    });
}

// Delete handler
function deleteHandler(e) {
    if (!confirm('Delete this classification?')) return;
    const id = e.target.dataset.id;
    fetch(`/delete_classification/${id}`, { method:'POST' }).then(res => {
        if (res.ok) {
            document.getElementById(`row-${id}`).remove();
            autoSortDebitTable(); // keep table sorted
        } else alert('Failed to delete');
    });
}

// Attach delete events
function attachDeleteEvents() {
    document.querySelectorAll('.delete-btn').forEach(btn => btn.onclick = deleteHandler);
}

// Submit Add/Edit
debitForm.addEventListener('submit', async e => {
    e.preventDefault();
    const id = document.getElementById('debitId').value;
    const description = document.getElementById('debitDescription').value;
    const category = document.getElementById('debitCategory').value;

    const url = currentAction === 'add' ? '/add_classification' : `/edit_classification/${id}`;
    const fd = new FormData();
    fd.append('description_pattern', description);
    fd.append('category', category);

    const res = await fetch(url, { method:'POST', body: fd });
    if (res.ok) {
        const data = await res.json();
        if (currentAction === 'add') {
            const tbody = document.querySelector('#debitTable tbody');
            const tr = document.createElement('tr');
            tr.id = `row-${data.id}`;
            tr.innerHTML = `
                <td>${data.id}</td>
                <td class="desc">${data.description_pattern}</td>
                <td class="cat">${data.category}</td>
                <td>
                    <button class="btn btn-sm btn-warning edit-btn" data-id="${data.id}" data-description="${data.description_pattern}" data-category="${data.category}">Edit</button>
                    <button class="btn btn-sm btn-danger delete-btn" data-id="${data.id}">Delete</button>
                </td>`;
            tbody.appendChild(tr);
            attachEditEvents();
            attachDeleteEvents();
        } else {
            const row = document.getElementById(`row-${id}`);
            row.querySelector('.desc').textContent = description;
            row.querySelector('.cat').textContent = category;
            row.querySelector('.edit-btn').dataset.description = description;
            row.querySelector('.edit-btn').dataset.category = category;
        }
        debitModal.hide();
        autoSortDebitTable(); // keep table always sorted
    } else alert('Failed to save classification');
});

// Initial attach
attachEditEvents();
attachDeleteEvents();

// ========================== TABLE SORTING ==========================
document.querySelectorAll('.sortable').forEach(header => {
    header.addEventListener('click', () => {
        const key = header.dataset.key; // 'desc' or 'cat'
        const table = document.getElementById('debitTable');
        const tbody = table.querySelector('tbody');
        const rows = Array.from(tbody.querySelectorAll('tr'));
        const ascending = !header.classList.contains('asc');
        header.classList.toggle('asc', ascending);
        header.classList.toggle('desc', !ascending);
        rows.sort((a,b) => {
            const aText = a.querySelector(`.${key}`).textContent.toLowerCase();
            const bText = b.querySelector(`.${key}`).textContent.toLowerCase();
            return ascending ? aText.localeCompare(bText) : bText.localeCompare(aText);
        });
        rows.forEach(row => tbody.appendChild(row));
    });
});

// ========================== AUTO MULTI-COLUMN SORT ==========================
function autoSortDebitTable() {
    const table = document.getElementById('debitTable');
    const tbody = table.querySelector('tbody');
    const rows = Array.from(tbody.querySelectorAll('tr'));
    rows.sort((a,b) => {
        const catA = a.querySelector('.cat').textContent.toLowerCase();
        const catB = b.querySelector('.cat').textContent.toLowerCase();
        if (catA !== catB) return catA.localeCompare(catB);
        const descA = a.querySelector('.desc').textContent.toLowerCase();
        const descB = b.querySelector('.desc').textContent.toLowerCase();
        return descA.localeCompare(descB);
    });
    rows.forEach(row => tbody.appendChild(row));
}

// Run auto-sort on page load
document.addEventListener('DOMContentLoaded', autoSortDebitTable);
