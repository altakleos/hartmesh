function present({ artifact }) {
  if (!artifact.projected) throw new Error("A server projection is required.");
  return JSON.parse(artifact.content);
}

export default {
  apiVersion: 1,
  module: "office-preview.v1",
  artifactApiVersion: 1,
  artifacts: [
    { id: "workbook", title: "Workbook", kind: "passive", present },
    { id: "document", title: "Document", kind: "passive", present },
    { id: "deck", title: "Slide deck", kind: "passive", present },
  ],
};
