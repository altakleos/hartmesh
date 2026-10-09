function mount(root, context) {
  let disposed = false, request = null, intent = null, busy = false;
  const box = document.createElement("section");
  const title = document.createElement("h2");
  title.textContent = "Supply quote details";
  const note = document.createElement("p");
  note.textContent = "Optional field-format check. Commercial facts await AI employee assessment; submitting does not resume Work.";
  const fallback = document.createElement("a");
  fallback.href = "/workspace/attention";
  fallback.textContent = "Open generic Attention controls";
  const status = document.createElement("p");
  status.setAttribute("role", "status");
  const fields = {};
  for (const [key, label, max] of [["request_id", "Request ID", 64], ["supplier", "Supplier", 120], ["delivery", "Delivery date (YYYY-MM-DD)", 10], ["currency", "Currency", 3], ["quoted_total", "Quoted total", 16]]) {
    const wrap = document.createElement("label");
    wrap.textContent = label;
    const input = document.createElement("input");
    input.maxLength = max;
    input.name = key;
    input.setAttribute("aria-label", label);
    wrap.append(input);
    fields[key] = input;
    box.append(wrap, document.createElement("br"));
  }
  const question = document.createElement("p");
  const load = document.createElement("button");
  load.textContent = "Load request";
  const submit = document.createElement("button");
  submit.textContent = "Supply response";
  submit.disabled = true;
  const update = () => {
    load.disabled = busy || intent !== null;
    submit.disabled = busy || request === null;
    for (const input of Object.values(fields)) input.disabled = busy || intent !== null;
  };
  fields.request_id.oninput = () => {
    if (intent || busy) return;
    request = null; question.textContent = ""; fallback.href = "/workspace/attention"; update();
  };
  load.onclick = async () => {
    if (busy || intent) return;
    busy = true; request = null; question.textContent = ""; fallback.href = "/workspace/attention"; update();
    const id = fields.request_id.value;
    try {
      const result = await context.callBackend("get", {request_id: id});
      if (disposed || context.signal.aborted) return;
      question.textContent = result.question + " — " + result.reason;
      fallback.href = "/workspace/attention?request=" + encodeURIComponent(result.id);
      if (result.purpose !== "information" || result.can_respond !== true || result.state === "closed") {
        status.textContent = "Use Attention for the current request and permitted actions.";
      } else {
        request = result;
        status.textContent = "Request loaded. Check its question before supplying details.";
      }
    } catch {
      if (!disposed) status.textContent = "Specialized validation unavailable. Use Attention; no validation or resolution is implied.";
    } finally { busy = false; if (!disposed) update(); }
  };
  submit.onclick = async () => {
    if (busy || !request) return;
    if (!intent) intent = {
      request_id: request.id,
      operation_id: crypto.randomUUID().replaceAll("-", ""),
      expected_request_revision: request.request_revision,
      expected_assignment_revision: request.assignment_revision,
      ...Object.fromEntries(["supplier", "delivery", "currency", "quoted_total"].map(key => [key, fields[key].value])),
    };
    busy = true; update();
    try {
      const result = await context.callBackend("respond", intent);
      if (disposed || context.signal.aborted) return;
      if (result.status === "invalid") {
        status.textContent = result.message + ". Nothing was submitted; correct the fields.";
        intent = null; submit.textContent = "Supply response"; return;
      }
      if (result.status !== "supplied") throw new Error("Unknown result");
      status.textContent = "Response supplied — awaiting check. " + result.validation;
      intent = null; request = null; submit.textContent = "Supply response";
    } catch {
      if (!disposed) {
        status.textContent = "Response unconfirmed. Retry this exact response, or inspect Attention before taking another action. Specialized validation is not confirmed.";
        submit.textContent = "Retry exact response";
      }
    } finally { busy = false; if (!disposed) update(); }
  };
  root.append(title, note, box, load, question, submit, status, fallback);
  return {dispose() { disposed = true; load.onclick = null; submit.onclick = null; fields.request_id.oninput = null; root.replaceChildren(); }};
}

export default {apiVersion: 1, module: "work-input.v1", surfaces: [{id: "quote", slot: "page", title: "Quote response", navigation: {label: "Quote response example", icon: "file-text"}, mount}]};
