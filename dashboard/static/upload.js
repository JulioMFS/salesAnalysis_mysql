document.addEventListener("DOMContentLoaded", () => {

    setupUploader({
        dropZoneId: "bankDropZone",
        inputId: "bankUpload",
        fileType: "bank",
        filenameRegex: /^CGD_Conta_Extracto.*\.csv$/i,
        errorContainerId: "bankUploadErrors"
    });

    setupUploader({
        dropZoneId: "salesDropZone",
        inputId: "salesUpload",
        fileType: "sales",
        filenameRegex: /^Vendas.*\.pdf$/i,
        errorContainerId: "salesUploadErrors"
    });

    setupUploader({
        dropZoneId: "tpaDropZone",
        inputId: "tpaUpload",
        fileType: "tpa",
        filenameRegex: /^TPA_Consulta_Movimento.*\.csv$/i,
        errorContainerId: "tpaUploadErrors"
    });
});

function setupUploader({ dropZoneId, inputId, fileType, filenameRegex, errorContainerId }) {

    const dropZone = document.getElementById(dropZoneId);
    const input = document.getElementById(inputId);
    const errorContainer = errorContainerId ? document.getElementById(errorContainerId) : null;

    dropZone.addEventListener("click", () => input.click());

    input.addEventListener("change", () => handleFiles(input.files));

    dropZone.addEventListener("dragover", e => {
        e.preventDefault();
        dropZone.classList.add("border", "border-dark");
    });

    dropZone.addEventListener("dragleave", () => {
        dropZone.classList.remove("border", "border-dark", "border-danger", "bg-danger", "text-white");
        if (errorContainer) errorContainer.innerHTML = "";
    });

    dropZone.addEventListener("drop", e => {
        e.preventDefault();
        dropZone.classList.remove("border", "border-dark", "border-danger", "bg-danger", "text-white");
        handleFiles(e.dataTransfer.files);
    });

    dropZone.addEventListener("dragenter", e => {
        e.preventDefault();
        const items = e.dataTransfer.items;
        let hasInvalid = false;
        if (items) {
            for (let i = 0; i < items.length; i++) {
                const fileName = items[i].getAsFile()?.name || "";
                if (!filenameRegex.test(fileName)) {
                    hasInvalid = true;
                    break;
                }
            }
        }
        if (hasInvalid) {
            dropZone.classList.add("border-danger", "bg-danger", "text-white");
            if (errorContainer) errorContainer.innerHTML = "❌ Invalid file(s) detected!";
        } else {
            dropZone.classList.add("border-dark");
            if (errorContainer) errorContainer.innerHTML = "";
        }
    });

    function handleFiles(files) {
        if (!files.length) return;

        const invalidFiles = [];

        for (const f of files) {
            if (!filenameRegex.test(f.name)) {
                invalidFiles.push(f.name);
            }
        }

        if (invalidFiles.length > 0) {
            // Show alert like Sales uploader
            alert(
                `Invalid file(s):\n${invalidFiles.join("\n")}\n\n` +
                `Only ${filenameRegex.toString().replace(/^\/\^|\$\/i$/g, '')} files are allowed.`
            );

            // Reset input so nothing is uploaded
            input.value = "";

            // Do NOT update inline error container
            return; // Stop upload
        }

        // All files valid → proceed to upload
        uploadFiles(files);
    }

    function uploadFiles(files) {
        const formData = new FormData();
        for (const f of files) formData.append("files[]", f);

        const xhr = new XMLHttpRequest();
        xhr.open("POST", `/upload/${fileType}`);
        xhr.upload.onprogress = updateProgress;

        xhr.onload = () => {
            try {
                const response = JSON.parse(xhr.responseText);
                renderReport(response);
            } catch {
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
                output += `✖ ${r.file}: ${r.message}\n`;
            }
        });
    } else {
        output = `✖ Upload failed\n\n${data.message}`;
    }

    document.getElementById("uploadReportContent").textContent = output;
    new bootstrap.Modal(document.getElementById("uploadReportModal")).show();
}