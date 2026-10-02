import mermaid from "mermaid";

mermaid.initialize({
  startOnLoad: false,
  securityLevel: "strict",
  theme: "neutral",
  fontFamily: "Inter, sans-serif",
});
for (const node of document.querySelectorAll(".mermaid")) {
  try {
    await mermaid.run({ nodes: [node] });
  } catch {
    node.textContent =
      "Diagram preview unavailable. Expand the source below to read the diagram.";
  }
}
