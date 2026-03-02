import { RegistryProvider } from "@effect/atom-react";
import * as React from "react";
import * as ReactDOM from "react-dom/client";
import { App } from "./App.js";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <RegistryProvider>
      <App />
    </RegistryProvider>
  </React.StrictMode>,
);
