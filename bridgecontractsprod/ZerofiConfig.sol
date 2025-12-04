// SPDX-License-Identifier: MIT
pragma solidity 0.8.20;

import "@openzeppelin/contracts/access/Ownable.sol";

contract ZerofiConfig is Ownable {
    address public registry;
    address public staking;
    address public zfiToken;
    address public bridge;
    address public feePool;
    address public zxmrToken;

    event RegistryUpdated(address indexed newAddress);
    event StakingUpdated(address indexed newAddress);
    event ZfiTokenUpdated(address indexed newAddress);
    event BridgeUpdated(address indexed newAddress);
    event FeePoolUpdated(address indexed newAddress);
    event ZxmrTokenUpdated(address indexed newAddress);

    constructor(
        address _registry,
        address _staking,
        address _zfiToken
    ) Ownable(msg.sender) {
        require(_registry != address(0), "Invalid registry");
        require(_staking != address(0), "Invalid staking");
        require(_zfiToken != address(0), "Invalid zfi token");
        registry = _registry;
        staking = _staking;
        zfiToken = _zfiToken;
    }

    function setRegistry(address _registry) external onlyOwner {
        require(_registry != address(0), "Invalid address");
        registry = _registry;
        emit RegistryUpdated(_registry);
    }

    function setStaking(address _staking) external onlyOwner {
        require(_staking != address(0), "Invalid address");
        staking = _staking;
        emit StakingUpdated(_staking);
    }

    function setZfiToken(address _zfiToken) external onlyOwner {
        require(_zfiToken != address(0), "Invalid address");
        zfiToken = _zfiToken;
        emit ZfiTokenUpdated(_zfiToken);
    }

    function setBridge(address _bridge) external onlyOwner {
        require(_bridge != address(0), "Invalid address");
        bridge = _bridge;
        emit BridgeUpdated(_bridge);
    }

    function setFeePool(address _feePool) external onlyOwner {
        require(_feePool != address(0), "Invalid address");
        feePool = _feePool;
        emit FeePoolUpdated(_feePool);
    }

    function setZxmrToken(address _zxmrToken) external onlyOwner {
        require(_zxmrToken != address(0), "Invalid address");
        zxmrToken = _zxmrToken;
        emit ZxmrTokenUpdated(_zxmrToken);
    }

    function getConfig() external view returns (
        address _registry,
        address _staking,
        address _zfiToken,
        address _bridge,
        address _feePool,
        address _zxmrToken
    ) {
        return (registry, staking, zfiToken, bridge, feePool, zxmrToken);
    }
}
