import Logger from './logger.js';

const logger = new Logger('Console');

const originalConsole = {
  log: console.log.bind(console),
  warn: console.warn.bind(console),
  error: console.error.bind(console),
  debug: console.debug.bind(console),
};

globalThis.__ZNODE_ORIGINAL_CONSOLE__ = originalConsole;

console.log = function (...args) {
  if (args.length === 0) {
    logger.info('');
  } else if (args.length === 1 && typeof args[0] === 'string') {
    logger.info(args[0]);
  } else if (args.length === 1 && typeof args[0] === 'object') {
    logger.info('console.log', args[0]);
  } else {
    const message = args.map(arg => {
      if (arg === null) return 'null';
      if (arg === undefined) return 'undefined';
      if (typeof arg === 'object') {
        try { return JSON.stringify(arg); } catch { return String(arg); }
      }
      return String(arg);
    }).join(' ');
    logger.info(message);
  }
};

console.warn = function (...args) {
  if (args.length === 0) {
    logger.warn('');
  } else if (args.length === 1 && typeof args[0] === 'string') {
    logger.warn(args[0]);
  } else if (args.length === 1 && typeof args[0] === 'object') {
    logger.warn('console.warn', args[0]);
  } else {
    const message = args.map(arg => {
      if (arg === null) return 'null';
      if (arg === undefined) return 'undefined';
      if (typeof arg === 'object') {
        try { return JSON.stringify(arg); } catch { return String(arg); }
      }
      return String(arg);
    }).join(' ');
    logger.warn(message);
  }
};

console.error = function (...args) {
  if (args.length === 0) {
    logger.error('');
  } else if (args.length === 1 && typeof args[0] === 'string') {
    logger.error(args[0]);
  } else if (args.length === 1 && args[0] instanceof Error) {
    logger.error(args[0].message || 'Error', { stack: args[0].stack, name: args[0].name });
  } else if (args.length === 1 && typeof args[0] === 'object') {
    logger.error('console.error', args[0]);
  } else {
    const message = args.find((arg) => typeof arg === 'string') || 'console.error';
    const errorArgs = args.filter((arg) => arg instanceof Error);
    const objectArgs = args.filter((arg) => typeof arg === 'object' && arg !== null && !(arg instanceof Error));
    const otherArgs = args.filter(
      (arg) => typeof arg !== 'string' && (typeof arg !== 'object' || arg === null),
    );

    const meta = {};
    if (errorArgs.length > 0) {
      meta.errors = errorArgs.map(e => ({ name: e.name, message: e.message, stack: e.stack }));
    }
    if (objectArgs.length > 0) {
      meta.objects = objectArgs;
    }
    if (otherArgs.length > 0) {
      meta.other = otherArgs.map(String);
    }

    logger.error(message, meta);
  }
};

console.debug = function (...args) {
  if (args.length === 0) {
    logger.debug('');
  } else if (args.length === 1 && typeof args[0] === 'string') {
    logger.debug(args[0]);
  } else if (args.length === 1 && typeof args[0] === 'object') {
    logger.debug('console.debug', args[0]);
  } else {
    const message = args.map(arg => {
      if (arg === null) return 'null';
      if (arg === undefined) return 'undefined';
      if (typeof arg === 'object') {
        try { return JSON.stringify(arg); } catch { return String(arg); }
      }
      return String(arg);
    }).join(' ');
    logger.debug(message);
  }
};

export { originalConsole };
export default logger;
