import React, { useEffect, useState } from "react";
import { createRoot } from "react-dom/client";
import {
  LineChart,
  Line,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
} from "recharts";
import "./styles.css";

const API_BASE = "http://localhost:8000";

function formatMoney(value) {
  if (value === undefined || value === null) return "-";
  return `$${Number(value).toFixed(2)}`;
}

function App() {
  const [metrics, setMetrics] = useState([]);
  const [ticks, setTicks] = useState([]);
  const [anomalies, setAnomalies] = useState([]);
  const [selectedSymbol, setSelectedSymbol] = useState("AAPL");
  const [symbolMetrics, setSymbolMetrics] = useState([]);

  async function loadData() {
    const [metricsRes, ticksRes, anomaliesRes, symbolRes] = await Promise.all([
      fetch(`${API_BASE}/metrics/latest`),
      fetch(`${API_BASE}/ticks/latest?limit=20`),
      fetch(`${API_BASE}/anomalies/latest?limit=10`),
      fetch(`${API_BASE}/metrics/${selectedSymbol}?limit=30`),
    ]);

    setMetrics(await metricsRes.json());
    setTicks(await ticksRes.json());
    setAnomalies(await anomaliesRes.json());

    const symbolData = await symbolRes.json();
    setSymbolMetrics(
      symbolData
        .reverse()
        .map((row) => ({
          time: new Date(row.window_end).toLocaleTimeString(),
          avg_price: Number(row.avg_price),
        }))
    );
  }

  useEffect(() => {
    loadData();
    const interval = setInterval(loadData, 3000);
    return () => clearInterval(interval);
  }, [selectedSymbol]);

  const symbols = [...new Set(metrics.map((m) => m.symbol))].sort();

  return (
    <main className="container">
      <header className="header">
        <div>
          <h1>Real-Time Financial Analytics</h1>
          <p>Live market stream processing with Alpha Vantage, Kafka, Spark, PostgreSQL, FastAPI, and React.</p>
        </div>
        <select value={selectedSymbol} onChange={(e) => setSelectedSymbol(e.target.value)}>
          {(symbols.length ? symbols : ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "TSLA"]).map((symbol) => (
            <option key={symbol} value={symbol}>{symbol}</option>
          ))}
        </select>
      </header>

      <section className="grid">
        {metrics.map((metric) => (
          <div className="card" key={metric.symbol}>
            <h2>{metric.symbol}</h2>
            <p className="price">{formatMoney(metric.avg_price)}</p>
            <p>Volume: {Number(metric.total_volume || 0).toLocaleString()}</p>
            <p>Range: {formatMoney(metric.min_price)} - {formatMoney(metric.max_price)}</p>
          </div>
        ))}
      </section>

      <section className="panel">
        <h2>{selectedSymbol} Rolling Average Price</h2>
        <ResponsiveContainer width="100%" height={320}>
          <LineChart data={symbolMetrics}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="time" />
            <YAxis domain={["auto", "auto"]} />
            <Tooltip />
            <Line type="monotone" dataKey="avg_price" strokeWidth={2} dot={false} />
          </LineChart>
        </ResponsiveContainer>
      </section>

      <section className="two-column">
        <div className="panel">
          <h2>Latest Market Events</h2>
          <table>
            <thead>
              <tr>
                <th>Symbol</th>
                <th>Price</th>
                <th>Volume</th>
              </tr>
            </thead>
            <tbody>
              {ticks.map((tick, index) => (
                <tr key={`${tick.symbol}-${tick.event_time}-${index}`}>
                  <td>{tick.symbol}</td>
                  <td>{formatMoney(tick.price)}</td>
                  <td>{Number(tick.volume).toLocaleString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <div className="panel">
          <h2>Anomalies</h2>
          <table>
            <thead>
              <tr>
                <th>Symbol</th>
                <th>Price</th>
                <th>Deviation</th>
              </tr>
            </thead>
            <tbody>
              {anomalies.map((row, index) => (
                <tr key={`${row.symbol}-${row.event_time}-${index}`}>
                  <td>{row.symbol}</td>
                  <td>{formatMoney(row.price)}</td>
                  <td>{(Number(row.pct_deviation) * 100).toFixed(2)}%</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </main>
  );
}

createRoot(document.getElementById("root")).render(<App />);
