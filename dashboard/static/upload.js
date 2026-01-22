document.addEventListener("DOMContentLoaded", () => {

    setupUploader({
        dropZoneId: "bankDropZone",
        inputId: "bankUpload",
        fileType: "bank",
        filenameRegex: /^CGD_Conta_Extracto.*\.csv$/i
    });

    setupUploader({
        dropZoneId: "salesDropZone",
        inputId: "salesUpload",
        fileType: "sales",
        filenameRegex: /^Vendas.*\.xlsx$/i
    });

});


function setupUploader({ dropZoneId, inputId, fileType, filenameRegex }) {

    const dropZone = document.getElementById(dropZoneId);
    const input = document.getElementById(inputId);

    // Click -> open file picker
    dropZone.addEventListener("click", () => input.click());

    // Picker selection
    input.addEventListener("change", () => handleFiles(input.files));

    // Drag & drop
    dropZone.addEventListener("dragover", e => {
        e.preventDefault();
        dropZone.classList.add("border", "border-dark");
    });

    dropZone.addEventListener("dragleave", () => {
        dropZone.classList.remove("border", "border-dark");
    });

    dropZone.addEventListener("drop", e => {
        e.preventDefault();
        dropZone.classList.remove("border", "border-dark");
        handleFiles(e.dataTransfer.files);
    });

    function handleFiles(files) {
        if (!files.length) return;

        for (const f of files) {
            if (!filenameRegex.test(f.name)) {
                alert(`Invalid filename:\n${f.name}`);
                return;
            }
        }

        uploadFiles(files);
    }

    function uploadFiles(files) {
        const formData = new FormData();
        for (const f of files) {
            formData.append("files[]", f); // ✅ IMPORTANT
        }

        const xhr = new XMLHttpRequest();
        xhr.open("POST", `/upload/${fileType}`);

        xhr.upload.onprogress = updateProgress;

        xhr.onload = () => {
            try {
                const response = JSON.parse(xhr.responseText);
                renderReport(response);
            } catch (e) {
                alert("Unexpected server response");
            }
            resetProgress();
        };

        xhr.onerror = () => {
            alert("Upload failed");
            resetProgress();
        };

        showProgress();
        xhr.send(formData);
    }
}


/* ---------- UI helpers ---------- */

function showProgress() {
    document.getElementById("uploadProgressWrapper").classList.remove("d-none");
}

function resetProgress() {
    const bar = document.getElementById("uploadProgressBar");
    bar.style.width = "0%";
    bar.textContent = "0%";
}

function updateProgress(e) {
    if (!e.lengthComputable) return;
    const percent = Math.round((e.loaded / e.total) * 100);
    const bar = document.getElementById("uploadProgressBar");
    bar.style.width = percent + "%";
    bar.textContent = percent + "%";
}

function renderReport(data) {
    let output = "";

    if (data.status === "success") {
        const s = data.summary;
        output += `✔ Upload completed\n\n`;
        output += `Files processed: ${s.files_total}\n`;
        output += `Successful: ${s.files_ok}\n`;
        output += `Errors: ${s.files_error}\n`;
        output += `Rows imported: ${s.rows_total}\n`;
        if (s.min_date) output += `From: ${s.min_date}\n`;
        if (s.max_date) output += `To: ${s.max_date}\n`;
        output += `\n--- File details ---\n`;

        data.results.forEach(r => {
            if (r.status === "ok") {
                output += `✔ ${r.file} (${r.rows} rows)\n`;
            } else {
                output += `✖ ${r.file}: ${r.error}\n`;
            }
        });
    } else {
        output = `✖ Upload failed\n\n${data.message}`;
    }

    document.getElementById("uploadReportContent").textContent = output;
    new bootstrap.Modal(document.getElementById("uploadReportModal")).show();
}
