import crypto from 'crypto';

export function computeRoundDigest(payloads) {
  if (!Array.isArray(payloads) || payloads.length === 0) {
    return null;
  }
  try {
    const hash = crypto.createHash('sha256');
    for (const p of payloads) {
      if (p != null) {
        hash.update(String(p));
      }
    }
    const digest = hash.digest('hex');
    return '0x' + digest;
  } catch (e) {
    console.log('  [WARN] Failed to compute round digest:', e.message || String(e));
    return null;
  }
}

export function canonicalizeMembers(members) {
  const list = Array.isArray(members) ? members.slice() : [];
  const decorated = list.map((a) => ({ lower: (a || '').toLowerCase(), orig: a }));
  decorated.sort((x, y) => x.lower.localeCompare(y.lower));
  return decorated.map((x) => x.orig);
}
