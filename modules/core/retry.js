export async function withRetry(fn, options = {}) {
  const maxAttempts = options.maxAttempts || 3;
  const baseDelayMs = options.baseDelayMs || 10000;
  const maxDelayMs = options.maxDelayMs || 60000;
  const label = options.label || 'operation';
  const cleanup = options.cleanup;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      console.log(`[Retry] ${label} - attempt ${attempt}/${maxAttempts}`);
      const result = await fn();
      if (result === true || (result && result.success !== false)) {
        return result;
      }
      console.log(`[Retry] ${label} - attempt ${attempt} returned falsy result`);
    } catch (e) {
      console.log(`[Retry] ${label} - attempt ${attempt} failed: ${e.message || String(e)}`);
    }

    if (attempt < maxAttempts) {
      const delay = Math.min(baseDelayMs * Math.pow(2, attempt - 1), maxDelayMs);
      console.log(`[Retry] Waiting ${delay / 1000}s before retry...`);
      await new Promise((r) => setTimeout(r, delay));

      if (typeof cleanup === 'function') {
        try {
          await cleanup();
        } catch (cleanupErr) {
          console.log(`[Retry] Cleanup error: ${cleanupErr.message}`);
        }
      }
    }
  }

  console.log(`[Retry] ${label} - all ${maxAttempts} attempts failed`);
  return false;
}
