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
    const message = args.find((arg) => typeof arg === 'string') || 'console.log';
    const objectArgs = args.filter((arg) => typeof arg === 'object' && arg !== null);
    const otherArgs = args.filter(
      (arg) => typeof arg !== 'string' && (typeof arg !== 'object' || arg === null),
    );

    const meta = {};
    if (objectArgs.length > 0) {
      meta.objects = objectArgs;
    }
    if (otherArgs.length > 0) {
      meta.other = otherArgs.map(String);
    }

    logger.info(message, meta);
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
    const message = args.find((arg) => typeof arg === 'string') || 'console.warn';
    const objectArgs = args.filter((arg) => typeof arg === 'object' && arg !== null);
    const otherArgs = args.filter(
      (arg) => typeof arg !== 'string' && (typeof arg !== 'object' || arg === null),
    );

    const meta = {};
    if (objectArgs.length > 0) {
      meta.objects = objectArgs;
    }
    if (otherArgs.length > 0) {
      meta.other = otherArgs.map(String);
    }

    logger.warn(message, meta);
  }
};

console.error = function (...args) {
  if (args.length === 0) {
    logger.error('');
  } else if (args.length === 1 && typeof args[0] === 'string') {
    logger.error(args[0]);
  } else if (args.length === 1 && typeof args[0] === 'object') {
    logger.error('console.error', args[0]);
  } else {
    const message = args.find((arg) => typeof arg === 'string') || 'console.error';
    const objectArgs = args.filter((arg) => typeof arg === 'object' && arg !== null);
    const otherArgs = args.filter(
      (arg) => typeof arg !== 'string' && (typeof arg !== 'object' || arg === null),
    );

    const meta = {};
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
    const message = args.find((arg) => typeof arg === 'string') || 'console.debug';
    const objectArgs = args.filter((arg) => typeof arg === 'object' && arg !== null);
    const otherArgs = args.filter(
      (arg) => typeof arg !== 'string' && (typeof arg !== 'object' || arg === null),
    );

    const meta = {};
    if (objectArgs.length > 0) {
      meta.objects = objectArgs;
    }
    if (otherArgs.length > 0) {
      meta.other = otherArgs.map(String);
    }

    logger.debug(message, meta);
  }
};

export { originalConsole };
export default logger;
