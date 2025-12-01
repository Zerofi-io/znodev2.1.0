import path from 'path';
import { execFile } from 'child_process';
import axios from 'axios';

class RPCManager {
  constructor({ url, scriptPath } = {}) {
    this.url = url;
    this._timer = null;
    const defaultScript = path.join(process.cwd(), 'scripts', 'start-monero-rpc.sh');
    this.scriptPath = scriptPath || process.env.MONERO_RPC_START_SCRIPT || defaultScript;
  }

  async restart(monero, lastWallet) {
    const script = this.scriptPath;
    let scriptOk = true;
    let readyOk = true;
    if (!script) {
      console.warn('[RPCManager] No Monero RPC start script configured; skipping restart.');
    } else {
      scriptOk = await new Promise((resolve) => {
        const child = execFile('bash', [script], { stdio: 'ignore' });
        const timeout = setTimeout(() => {
          try {
            child.kill('SIGTERM');
          } catch {}
          console.error('[RPCManager] Monero RPC start script timed out.');
          resolve(false);
        }, 30000);

        child.on('exit', (code) => {
          clearTimeout(timeout);
          if (code === 0) {
            console.log('[RPCManager] Monero wallet RPC restart script completed.');
            resolve(true);
          } else {
            console.error(
              `[RPCManager] Monero wallet RPC restart script exited with code ${code}.`,
            );
            resolve(false);
          }
        });

        child.on('error', (err) => {
          clearTimeout(timeout);
          console.error(
            '[RPCManager] Failed to execute Monero RPC start script:',
            err.message || err,
          );
          resolve(false);
        });
      });
    }

    if (monero && monero.url) {
      console.log('[RPCManager] Waiting for Monero RPC to become ready...');
      readyOk = false;
      const maxRetries = Number(process.env.RPC_READY_RETRIES || 60);
      const retryInterval = Number(process.env.RPC_READY_INTERVAL_MS || 1000);

      for (let i = 0; i < maxRetries; i++) {
        try {
          await axios.post(
            `${monero.url}/json_rpc`,
            {
              jsonrpc: '2.0',
              id: '0',
              method: 'get_version',
              params: {},
            },
            {
              timeout: 5000,
              auth:
                monero.user && monero.password
                  ? {
                      username: monero.user,
                      password: monero.password,
                    }
                  : undefined,
            },
          );
          console.log('[RPCManager] Monero RPC is ready');
          readyOk = true;
          break;
        } catch {
          if (i === maxRetries - 1) {
            console.error(
              `[RPCManager] Monero RPC did not become ready within ${(maxRetries * retryInterval) / 1000}s`,
            );
          }
          await new Promise((r) => setTimeout(r, retryInterval));
        }
      }
    }

    if (!scriptOk || (monero && monero.url && !readyOk)) {
      throw new Error('Monero RPC restart failed: start script or readiness check failed');
    }

    if (lastWallet) {
      const filename = typeof lastWallet === 'string' ? lastWallet : lastWallet.filename;
      const password =
        typeof lastWallet === 'object' && lastWallet.password != null
          ? lastWallet.password
          : process.env.MONERO_WALLET_PASSWORD || '';

      if (!filename) {
        console.warn(
          '[RPCManager] lastWallet provided but filename is missing; skipping wallet reopen.',
        );
      } else {
        try {
          await monero.call('open_wallet', { filename, password }, 180000);
          console.log('[RPCManager] Reopened wallet after RPC restart:', filename);
        } catch (e) {
          console.error(
            '[RPCManager] Failed to reopen wallet after RPC restart:',
            e && e.message ? e.message : e,
          );
        }
      }
    }

    return true;
  }

  startHealthWatch(monero) {
    if (this._timer) return;

    const baseUrl = (this.url || (monero && monero.url) || '').replace(/\/+$/, '');
    if (!baseUrl) {
      console.warn('[RPCManager] No Monero RPC URL configured for health watch; skipping');
      return;
    }

    const auth =
      monero && monero.user && monero.password
        ? { username: monero.user, password: monero.password }
        : undefined;

    const intervalMs = Number(process.env.RPC_HEALTH_INTERVAL_MS || 60000);
    const timeoutMs = Number(process.env.RPC_HEALTH_TIMEOUT_MS || 30000);

    this._timer = setInterval(async () => {
      try {
        await axios.post(
          baseUrl + '/json_rpc',
          {
            jsonrpc: '2.0',
            id: 'health',
            method: 'get_version',
            params: {},
          },
          {
            timeout: timeoutMs,
            auth,
          },
        );
      } catch (e) {
        const status = e && e.response && e.response.status;
        if (status === 401) {
          console.warn(
            '[RPCManager] Health check unauthorized (401); disabling health watch for this node',
          );
          clearInterval(this._timer);
          this._timer = null;
          return;
        }
        let msg = e && e.message ? e.message : '';
        if (!msg && e && e.code) {
          msg = String(e.code);
        }
        if (!msg && status) {
          msg = `HTTP ${status}`;
        }
        if (!msg) {
          try {
            msg = JSON.stringify(e);
          } catch {
            msg = String(e);
          }
        }
        console.error('[RPCManager] Monero RPC health check failed:', msg);
      }
    }, intervalMs);
  }
}

export default RPCManager;
