class Logger {
  constructor(context = 'ZNode') {
    this.context = context;
    this.logLevel = this.parseLogLevel(process.env.LOG_LEVEL || 'info');
    this.metrics = {
      debug: 0,
      info: 0,
      warn: 0,
      error: 0,
    };
    this.startTime = Date.now();
  }

  parseLogLevel(level) {
    const levels = {
      debug: 0,
      info: 1,
      warn: 2,
      error: 3,
    };
    return levels[level.toLowerCase()] || 1;
  }

  shouldLog(level) {
    const levels = {
      debug: 0,
      info: 1,
      warn: 2,
      error: 3,
    };
    return levels[level] >= this.logLevel;
  }

  formatMessage(level, message, meta = {}) {
    const redactedMeta = this.redactSensitive(meta);
    const metaStr = Object.keys(redactedMeta).length > 0 ? ` ${JSON.stringify(redactedMeta)}` : '';

    if (this.context === 'Console') {
      return `${message}${metaStr}`;
    }

    const timestamp = new Date().toISOString();
    return `[${timestamp}] [${level.toUpperCase()}] [${this.context}] ${message}${metaStr}`;
  }

  debug(message, meta = {}) {
    this.metrics.debug++;
    if (this.shouldLog('debug')) {
      const base = globalThis.__ZNODE_ORIGINAL_CONSOLE__ || console;
      base.log(this.formatMessage('debug', message, meta));
    }
  }

  info(message, meta = {}) {
    this.metrics.info++;
    if (this.shouldLog('info')) {
      const base = globalThis.__ZNODE_ORIGINAL_CONSOLE__ || console;
      base.log(this.formatMessage('info', message, meta));
    }
  }

  warn(message, meta = {}) {
    this.metrics.warn++;
    if (this.shouldLog('warn')) {
      const base = globalThis.__ZNODE_ORIGINAL_CONSOLE__ || console;
      base.warn(this.formatMessage('warn', message, meta));
    }
  }

  error(message, error = null, meta = {}) {
    this.metrics.error++;
    if (this.shouldLog('error')) {
      const errorMeta = error
        ? {
            ...meta,
            error: error.message,
            stack: error.stack,
          }
        : meta;
      const base = globalThis.__ZNODE_ORIGINAL_CONSOLE__ || console;
      base.error(this.formatMessage('error', message, errorMeta));
    }
  }

  getMetrics() {
    const uptime = Math.floor((Date.now() - this.startTime) / 1000);
    return {
      context: this.context,
      uptime_seconds: uptime,
      log_counts: { ...this.metrics },
      total_logs: Object.values(this.metrics).reduce((a, b) => a + b, 0),
    };
  }

  logMetrics() {
    const metrics = this.getMetrics();
    this.info('Logger metrics', metrics);
  }

  redactSensitive(obj) {
    if (!obj || typeof obj !== 'object') {
      return obj;
    }

    const sensitiveKeys = [
      'password',
      'privatekey',
      'private_key',
      'secret',
      'mnemonic',
      'seed',
      'passphrase',
      'backup_passphrase',
      'wallet_backup_passphrase',
      'rpc_login',
      'login',
      'wallet_password',
      'monero_wallet_password',
      'rpc_user',
      'rpc_password',
      'monero_wallet_rpc_user',
      'monero_wallet_rpc_password',
      'key',
      'api_key',
      'apikey',
      'bearer',
      'authorization',
      'cookie',
    ];
    const sensitivePatterns = ['token', 'auth', 'credential', 'pass'];
    const redacted = Array.isArray(obj) ? [...obj] : { ...obj };

    for (const key of Object.keys(redacted)) {
      const lowerKey = key.toLowerCase();

      const isExactMatch = sensitiveKeys.includes(lowerKey);
      const isPatternMatch = sensitivePatterns.some(
        (pattern) =>
          lowerKey === pattern || lowerKey.endsWith(pattern) || lowerKey.startsWith(pattern),
      );

      if (isExactMatch || isPatternMatch) {
        redacted[key] = '[REDACTED]';
      } else if (typeof redacted[key] === 'object' && redacted[key] !== null) {
        redacted[key] = this.redactSensitive(redacted[key]);
      }
    }

    return redacted;
  }

  createChild(context) {
    return new Logger(`${this.context}:${context}`);
  }
}

export default Logger;
