export function resolveRuntimeConfig(env) {
  const TEST_MODE = env.TEST_MODE === '1';
  const DRY_RUN = TEST_MODE ? env.DRY_RUN !== '0' : env.DRY_RUN === '1';

  let EFFECTIVE_RPC_URL = null;
  let usedDemoRpc = false;

  if (env.RPC_URL || env.ETH_RPC_URL) {
    EFFECTIVE_RPC_URL = env.RPC_URL || env.ETH_RPC_URL;
  } else if (TEST_MODE) {
    EFFECTIVE_RPC_URL = 'https://eth-sepolia.g.alchemy.com/v2/demo';
    usedDemoRpc = true;
  }

  return {
    TEST_MODE,
    DRY_RUN,
    EFFECTIVE_RPC_URL,
    usedDemoRpc,
  };
}
