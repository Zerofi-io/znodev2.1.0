export function isValidMoneroAddress(address) {
  if (!address || typeof address !== 'string') {
    return false;
  }
  if (address.length === 95 && (address.startsWith('4') || address.startsWith('8'))) {
    return true;
  }
  if (address.length === 106 && address.startsWith('4')) {
    return true;
  }
  return false;
}
