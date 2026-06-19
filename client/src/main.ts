import { VoxwireClient, type ConnectionStatus } from "./ws";

const connectBtn = document.getElementById("connect") as HTMLButtonElement;
const pingBtn = document.getElementById("ping") as HTMLButtonElement;
const dot = document.getElementById("dot") as HTMLSpanElement;
const statusText = document.getElementById("statusText") as HTMLSpanElement;
const logEl = document.getElementById("log") as HTMLDivElement;

function log(text: string, kind: "in" | "out" | "sys" = "sys"): void {
  const line = document.createElement("div");
  line.className = `log-${kind}`;
  const time = new Date().toLocaleTimeString();
  line.textContent = `[${time}] ${text}`;
  logEl.appendChild(line);
  logEl.scrollTop = logEl.scrollHeight;
}

function setStatus(status: ConnectionStatus): void {
  dot.className = `dot ${status}`;
  statusText.textContent = status;
  const connected = status === "connected";
  pingBtn.disabled = !connected;
  connectBtn.textContent = connected ? "Disconnect" : "Connect";
  connectBtn.disabled = status === "connecting";
}

let client: VoxwireClient | null = null;

connectBtn.addEventListener("click", () => {
  if (client) {
    client.disconnect();
    client = null;
    return;
  }
  client = new VoxwireClient({
    onStatus: (status) => {
      setStatus(status);
      if (status === "connected") log(`connected (session ${client?.id})`, "sys");
      if (status === "disconnected") log("disconnected", "sys");
      if (status === "error") log("connection error", "sys");
    },
    onMessage: (data) => log(`<- ${JSON.stringify(data)}`, "in"),
  });
  client.connect();
});

pingBtn.addEventListener("click", () => {
  if (!client) return;
  client.ping();
  log("-> ping", "out");
});

setStatus("disconnected");
