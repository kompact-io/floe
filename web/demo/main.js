// main.js: wires the demo form to `WasmClient` (built from
// `crates/client/src/wasm.rs`, see `flake.nix`'s `floe-client-wasm`
// package, copied in under `pkg/` at `web/demo`'s build time). Request/
// response bodies are plain JSON strings, matching `floe_dto_http::query`'s
// own wire shape exactly (hex strings for hashes -- see
// `floe_dto_http::ser_hex`/`de_hex_array`), not anything bespoke to this
// page.

import init, { WasmClient, decodeAddress } from './pkg/floe_client.js';

const rpcSelect = document.getElementById('rpc');
const runButton = document.getElementById('run');
const resultEl = document.getElementById('result');
const wasmBadge = document.getElementById('wasm-badge');
const fieldGroups = document.querySelectorAll('.rpc-fields');

function showFieldsFor(rpc) {
  fieldGroups.forEach((el) => {
    el.style.display = el.dataset.rpc === rpc ? '' : 'none';
  });
}

function parseKeys(text) {
  return text
    .split(',')
    .map((s) => s.trim())
    .filter((s) => s.length > 0)
    .map((pair) => {
      const [hash, index] = pair.split(':');
      return { hash: hash.trim(), index: Number(index) };
    });
}

function buildRequest(rpc) {
  switch (rpc) {
    case 'readUtxos':
      return { keys: parseKeys(document.getElementById('read-utxos-keys').value) };
    case 'searchUtxos': {
      const address = document.getElementById('search-address').value.trim();
      let payHash = document.getElementById('search-pay-hash').value.trim() || null;
      let stakeHash = document.getElementById('search-stake-hash').value.trim() || null;
      if (address) {
        const decoded = JSON.parse(decodeAddress(address));
        payHash = decoded.pay_hash ?? null;
        stakeHash = decoded.stake_hash ?? null;
      }

      const sinceSlot = document.getElementById('search-since-slot').value.trim();
      const sinceHash = document.getElementById('search-since-hash').value.trim();
      const startToken = document.getElementById('search-start-token').value.trim();
      const maxItems = document.getElementById('search-max-items').value.trim();
      return {
        pay_hash: payHash,
        stake_hash: stakeHash,
        since_slot: sinceSlot ? Number(sinceSlot) : null,
        since_hash: sinceHash || null,
        start_token: startToken || null,
        max_items: maxItems ? Number(maxItems) : null,
      };
    }
    case 'readTx':
      return { hash: document.getElementById('read-tx-hash').value.trim() };
    case 'readData':
      return { key: document.getElementById('read-data-key').value.trim() };
    default:
      throw new Error(`unknown rpc: ${rpc}`);
  }
}

function showResult(text, isError) {
  resultEl.textContent = text;
  resultEl.classList.toggle('error', isError);
}

async function run() {
  const rpc = rpcSelect.value;
  runButton.disabled = true;
  showResult('running…', false);
  try {
    const req = buildRequest(rpc);
    const responseJson = await window.floeClient[rpc](JSON.stringify(req));
    showResult(JSON.stringify(JSON.parse(responseJson), null, 2), false);
  } catch (err) {
    showResult(String(err), true);
  } finally {
    runButton.disabled = false;
  }
}

async function main() {
  await init();
  // `""` = same-origin root; Caddy proxies `/query/*` from there to
  // floe-server. Loading the wasm module says nothing about whether
  // that proxy/server is actually reachable -- only a RUN attempt does.
  window.floeClient = new WasmClient('');
  wasmBadge.remove();
  runButton.disabled = false;

  showFieldsFor(rpcSelect.value);
  rpcSelect.addEventListener('change', () => showFieldsFor(rpcSelect.value));
  runButton.addEventListener('click', run);
}

main().catch((err) => {
  wasmBadge.textContent = 'WASM FAILED';
  showResult(String(err), true);
});
