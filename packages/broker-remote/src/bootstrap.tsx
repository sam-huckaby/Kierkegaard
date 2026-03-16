import React from "react";
import { createRoot } from "react-dom/client";
import { DevtoolsApp } from "./devtools/DevtoolsApp";

const container = document.getElementById("root");
if (!container) {
  throw new Error("Missing #root element.");
}

createRoot(container).render(
  <React.StrictMode>
    <DevtoolsApp />
  </React.StrictMode>
);

