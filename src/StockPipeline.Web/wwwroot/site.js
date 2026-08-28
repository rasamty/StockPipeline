const MAX_POINTS = 40;
const TICK_INTERVAL_MS = 500; // "fast" — two ticks a second

const rawSeries = [];
const processedSeries = [];
const labels = [];

const ctx = document.getElementById("stockChart").getContext("2d");
const chart = new Chart(ctx, {
  type: "line",
  data: {
    labels,
    datasets: [
      {
        label: "Raw (browser-generated)",
        data: rawSeries,
        borderColor: "#1f77b4", backgroundColor: "transparent",
        borderWidth: 2, pointRadius: 0, tension: 0.25,
      },
      {
        label: "Processed (+constant, via RabbitMQ)",
        data: processedSeries,
        borderColor: "#d62728", backgroundColor: "transparent",
        borderWidth: 2, pointRadius: 0, tension: 0.25,
      },
    ],
  },
  options: { animation: false, responsive: true, scales: { y: { beginAtZero: false } } },
});

const connection = new signalR.HubConnectionBuilder()
  .withUrl("/hubs/stock")
  .withAutomaticReconnect()
  .build();

connection.on("ReceiveProcessedPrice", (processedPrice) => {
  processedSeries.push(processedPrice);
  if (processedSeries.length > MAX_POINTS) processedSeries.shift();
  chart.update();
});

let tickTimer = null;

function pushRawPoint(price) {
  rawSeries.push(price);
  labels.push(new Date().toLocaleTimeString());
  if (rawSeries.length > MAX_POINTS) rawSeries.shift();
  if (labels.length > MAX_POINTS) labels.shift();
  // Keep the processed series the same length as labels so Chart.js aligns
  // both lines on the same x-axis even while processed values are still
  // arriving asynchronously.
  while (processedSeries.length < labels.length) processedSeries.unshift(null);
  while (processedSeries.length > labels.length) processedSeries.shift();
  chart.update();
}

async function startFeed() {
  await connection.start();
  document.getElementById("status").textContent = "Connected";
  document.getElementById("startButton").disabled = true;
  document.getElementById("stopButton").disabled = false;

  tickTimer = setInterval(async () => {
    // Simulate a stock price: a random walk between roughly 90 and 110.
    const price = Math.round((100 + (Math.random() * 20 - 10)) * 100) / 100;
    pushRawPoint(price);
    try {
      await connection.invoke("SendRawPrice", price);
    } catch (err) {
      console.error("Failed to send price to backend:", err);
    }
  }, TICK_INTERVAL_MS);
}

async function stopFeed() {
  if (tickTimer) clearInterval(tickTimer);
  await connection.stop();
  document.getElementById("status").textContent = "Stopped";
  document.getElementById("startButton").disabled = false;
  document.getElementById("stopButton").disabled = true;
}

document.getElementById("startButton").addEventListener("click", startFeed);
document.getElementById("stopButton").addEventListener("click", stopFeed);

fetch("/api/environment")
  .then((r) => r.json())
  .then((data) => {
    const badge = document.getElementById("env-badge");
    badge.textContent = data.environment;
    badge.className = "env-badge env-" + data.environment.toLowerCase();
  })
  .catch(() => {
    document.getElementById("env-badge").textContent = "UNKNOWN";
  });
