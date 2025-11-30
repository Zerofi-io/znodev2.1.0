// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import "@openzeppelin/contracts/access/Ownable.sol";
import "@openzeppelin/contracts/utils/ReentrancyGuard.sol";
import "@openzeppelin/contracts/utils/cryptography/EIP712.sol";
import "@openzeppelin/contracts/utils/cryptography/ECDSA.sol";

interface IClusterRegistry {
    function recordStage1Slash(address node) external returns (bool isThirdSlash);
}

/**
 * @title ZFIStaking - Decentralized Staking with Permissionless Slashing
 * @notice No centralized oracle - slashing uses EIP-712 signed heartbeats
 */
contract ZFIStaking is Ownable, ReentrancyGuard, EIP712 {
    using SafeERC20 for IERC20;
    using ECDSA for bytes32;

    IERC20 public zfiToken;
    IERC20 public zxmrToken;
    
    uint256 public constant STAKE_AMOUNT = 1_000_000 * 10**18;
    uint256 public constant LOCK_PERIOD = 365 days;
    
    uint256 public constant GRACE_PERIOD_48H = 48 hours;
    uint256 public constant GRACE_PERIOD_72H = 72 hours;
    uint256 public constant GRACE_PERIOD_96H = 96 hours;
    
    uint256 public constant TOTAL_REWARD_POOL = 400_000_000 * 10**18;
    uint256 public constant DISSOLUTION_REWARD = 50_000 * 10**18;
    
    // EIP-712 typed data for signed heartbeats: Heartbeat(address node,uint256 timestamp)
    bytes32 public constant HEARTBEAT_TYPEHASH = keccak256("Heartbeat(address node,uint256 timestamp)");
    
    // Last heartbeat timestamp proved via on-chain slashing with proof
    mapping(address => uint256) public lastProvedHeartbeat;
    
    uint256 public deploymentTime;
    uint256 public totalStaked;
    uint256 public dissolutionPool;
    
    uint256 public currentVersion = 1;
    uint256 public versionUpdateDeadline;
    uint256 public constant VERSION_UPDATE_PERIOD = 30 days;
    
    uint256[20] public yearlyRewardPercentages = [
        6000, 5500, 5000, 4500, 4000,
        3600, 3200, 2800, 2400, 2000,
        1800, 1600, 1400, 1200, 1000,
        800, 600, 400, 300, 300
    ];
    
    struct NodeStake {
        uint256 stakedAmount;
        uint256 stakeTime;
        uint256 lastClaimTime;
        uint256 lastHeartbeat; // Only used for version updates, not slashing
        bytes32 codeHash;
        string moneroFeeAddress;
        bool active;
        uint256 nodeVersion;
        uint256 slashingStage;
        uint256 lastSlashTime;
    }
    
    mapping(address => NodeStake) public nodes;
    mapping(address => bool) public blacklisted;
    mapping(address => uint256) public claimableRewards;
    mapping(address => uint256) public claimableZXMR;

    struct Delegation {
        uint256 amount;
        uint64 startTime;
        uint64 unlockTime;
        bool active;
    }

    mapping(address => mapping(address => Delegation)) public delegations;
    mapping(address => uint256) public totalDelegatedTo;

    address[] public activeNodes;
    
    address public clusterRegistry;
    
    event Staked(address indexed node, uint256 amount, bytes32 codeHash);
    event Unstaked(address indexed node, uint256 amount);
    event RewardsClaimed(address indexed node, uint256 zfiAmount, uint256 zxmrAmount);
    event Delegated(address indexed operator, address indexed delegator, uint256 amount);
    event DelegationWithdrawn(address indexed operator, address indexed delegator, uint256 amount);
    event Slashed(address indexed node, uint256 amount, string reason);
    event ProgressiveSlash(address indexed node, uint256 stage, uint256 amount, uint256 hoursOffline);
    event Blacklisted(address indexed node, string reason);
    event ZXMRFeesDistributed(uint256 totalAmount, uint256 perNode);
    event VersionUpdated(uint256 newVersion, uint256 deadline);
    event NodeVersionUpdated(address indexed node, uint256 oldVersion, uint256 newVersion);
    event DissolutionPoolFunded(address indexed slashedNode, uint256 amount);
    event DissolutionRewardPaid(address indexed recipient, uint256 amount);
    
    constructor(address _zfiToken) Ownable(msg.sender) EIP712("ZFIStaking", "1") {
        zfiToken = IERC20(_zfiToken);
        deploymentTime = block.timestamp;
    }
    
    function setZXMRToken(address _zxmrToken) external onlyOwner {
        require(address(zxmrToken) == address(0), "Already set");
        zxmrToken = IERC20(_zxmrToken);
    }
    
    function setClusterRegistry(address _registry) external onlyOwner {
        clusterRegistry = _registry;
    }
    
    function announceVersionUpdate(uint256 newVersion) external onlyOwner {
        require(newVersion > currentVersion, "Version must increase");
        currentVersion = newVersion;
        versionUpdateDeadline = block.timestamp + VERSION_UPDATE_PERIOD;
        emit VersionUpdated(newVersion, versionUpdateDeadline);
    }
    
    function updateNodeVersion(bytes32 newCodeHash) external {
        NodeStake storage node = nodes[msg.sender];
        require(node.active, "Not staked");
        require(newCodeHash != bytes32(0), "Invalid code hash");
        
        uint256 oldVersion = node.nodeVersion;
        node.nodeVersion = currentVersion;
        node.codeHash = newCodeHash;
        
        emit NodeVersionUpdated(msg.sender, oldVersion, currentVersion);
    }

    /**
     * @notice Permissionless downtime slashing using EIP-712 signed heartbeat proof
     * @dev Anyone can call with a valid signature from the node proving its last heartbeat
     * @param _node The node to slash
     * @param lastHeartbeatTs The timestamp of the node's last signed heartbeat
     * @param sig The EIP-712 signature from the node
     */
    function slashForDowntimeWithProof(
        address _node,
        uint256 lastHeartbeatTs,
        bytes calldata sig
    ) external {
        require(lastHeartbeatTs <= block.timestamp, "heartbeat in future");
        require(!blacklisted[_node], "Already blacklisted");

        NodeStake storage node = nodes[_node];
        require(node.active, "Not staked");
        require(node.stakedAmount > 0, "No stake");

        // Verify the node itself signed this heartbeat
        bytes32 digest = _hashHeartbeat(_node, lastHeartbeatTs);
        address signer = digest.recover(sig);
        require(signer == _node, "Invalid heartbeat signature");

        uint256 hoursOffline = (block.timestamp - lastHeartbeatTs) / 1 hours;
        _applyProgressiveSlashing(_node, node, hoursOffline);

        // Store for analytics/metrics
        if (lastHeartbeatTs > lastProvedHeartbeat[_node]) {
            lastProvedHeartbeat[_node] = lastHeartbeatTs;
        }
    }

    function _hashHeartbeat(address node, uint256 timestamp) internal view returns (bytes32) {
        return _hashTypedDataV4(
            keccak256(abi.encode(HEARTBEAT_TYPEHASH, node, timestamp))
        );
    }

    function _applyProgressiveSlashing(
        address _node,
        NodeStake storage node,
        uint256 hoursOffline
    ) internal {
        uint256 slashAmount;
        uint256 newStage;

        if (hoursOffline >= 96 && node.slashingStage < 3) {
            newStage = 3;
            slashAmount = node.stakedAmount;
        } else if (hoursOffline >= 72 && node.slashingStage < 2) {
            newStage = 2;
            slashAmount = STAKE_AMOUNT / 4;
        } else if (hoursOffline >= 48 && node.slashingStage < 1) {
            newStage = 1;
            slashAmount = STAKE_AMOUNT / 2;
        } else {
            revert("No slashing applicable");
        }

        require(slashAmount <= node.stakedAmount, "Invalid slash amount");

        node.stakedAmount -= slashAmount;
        node.slashingStage = newStage;
        node.lastSlashTime = block.timestamp;
        totalStaked -= slashAmount;

        uint256 callerReward = slashAmount / 10; // 10% to caller always
        uint256 burnAmount;
        uint256 poolAmount;

        if (newStage == 1) {
            // Stage 1: node kicked from cluster - fund dissolution pool
            bool isThirdSlash = false;
            if (clusterRegistry != address(0)) {
                try IClusterRegistry(clusterRegistry).recordStage1Slash(_node) returns (bool result) {
                    isThirdSlash = result;
                } catch {}
            }
            
            if (isThirdSlash) {
                // 3rd slash from cluster: 70% burn, 10% caller, 20% pool
                poolAmount = slashAmount / 5; // 20%
            } else {
                // 1st/2nd slash from cluster: 80% burn, 10% caller, 10% pool
                poolAmount = slashAmount / 10; // 10%
            }
            burnAmount = slashAmount - callerReward - poolAmount;
            dissolutionPool += poolAmount;
            emit DissolutionPoolFunded(_node, poolAmount);
        } else {
            // Stage 2 & 3: node already out of cluster
            // 90% burn, 10% caller
            burnAmount = slashAmount - callerReward;
        }

        if (node.stakedAmount == 0 || newStage == 3) {
            node.active = false;
            _removeFromActiveNodes(_node);
            blacklisted[_node] = true;
            emit Blacklisted(_node, "Fully slashed for downtime");
        }

        if (callerReward > 0) {
            zfiToken.safeTransfer(msg.sender, callerReward);
        }
        if (burnAmount > 0) {
            zfiToken.safeTransfer(address(0xdead), burnAmount);
        }

        emit ProgressiveSlash(_node, newStage, slashAmount, hoursOffline);
        emit Slashed(_node, slashAmount, "Progressive downtime");
    }

    /// @notice Called by ClusterRegistry to pay dissolution reward
    function payDissolutionReward(address recipient) external returns (bool) {
        require(msg.sender == clusterRegistry, "Only registry");
        
        if (dissolutionPool >= DISSOLUTION_REWARD) {
            dissolutionPool -= DISSOLUTION_REWARD;
            zfiToken.safeTransfer(recipient, DISSOLUTION_REWARD);
            emit DissolutionRewardPaid(recipient, DISSOLUTION_REWARD);
            return true;
        }
        return false;
    }

    /// @notice View function to check if a node can be slashed (for off-chain use)
    /// @dev This is informational only - actual slashing requires proof
    function getSlashingInfo(address _node) external view returns (
        uint256 slashingStage,
        uint256 lastProved,
        bool isBlacklisted
    ) {
        NodeStake memory node = nodes[_node];
        return (node.slashingStage, lastProvedHeartbeat[_node], blacklisted[_node]);
    }
    
    function stake(bytes32 _codeHash, string calldata _moneroFeeAddress) external nonReentrant {
        require(!blacklisted[msg.sender], "Blacklisted");
        require(!nodes[msg.sender].active, "Already staked");
        require(_codeHash != bytes32(0), "Invalid code hash");
        require(bytes(_moneroFeeAddress).length > 0, "Invalid Monero address");
        
        zfiToken.safeTransferFrom(msg.sender, address(this), STAKE_AMOUNT);
        
        nodes[msg.sender] = NodeStake({
            stakedAmount: STAKE_AMOUNT,
            stakeTime: block.timestamp,
            lastClaimTime: block.timestamp,
            lastHeartbeat: block.timestamp,
            codeHash: _codeHash,
            moneroFeeAddress: _moneroFeeAddress,
            active: true,
            nodeVersion: currentVersion,
            slashingStage: 0,
            lastSlashTime: 0
        });
        
        activeNodes.push(msg.sender);
        totalStaked += STAKE_AMOUNT;
        
        emit Staked(msg.sender, STAKE_AMOUNT, _codeHash);
    }
    
    function topUpStake() external nonReentrant {
        require(!blacklisted[msg.sender], "Blacklisted");
        NodeStake storage node = nodes[msg.sender];
        require(node.stakedAmount > 0, "Not staked");
        require(node.stakedAmount < STAKE_AMOUNT, "Already at full stake");

        uint256 deficit = STAKE_AMOUNT - node.stakedAmount;

        zfiToken.safeTransferFrom(msg.sender, address(this), deficit);

        node.stakedAmount = STAKE_AMOUNT;
        node.active = true;
        totalStaked += deficit;

        // Re-add to active nodes if was removed
        bool found = false;
        for (uint256 i = 0; i < activeNodes.length; i++) {
            if (activeNodes[i] == msg.sender) {
                found = true;
                break;
            }
        }
        if (!found) {
            activeNodes.push(msg.sender);
        }

        emit Staked(msg.sender, deficit, node.codeHash);
    }

    function delegateStake(address operator, uint256 amount) external nonReentrant {
        require(amount > 0, "Amount must be positive");
        NodeStake storage node = nodes[operator];
        require(node.active, "Operator not active");
        require(!blacklisted[operator], "Operator blacklisted");

        Delegation storage d = delegations[operator][msg.sender];
        require(!d.active, "Delegation already active");

        zfiToken.safeTransferFrom(msg.sender, address(this), amount);

        d.amount = amount;
        d.startTime = uint64(block.timestamp);
        d.unlockTime = uint64(block.timestamp + LOCK_PERIOD);
        d.active = true;

        totalDelegatedTo[operator] += amount;

        emit Delegated(operator, msg.sender, amount);
    }

    function withdrawDelegation(address operator) external nonReentrant {
        Delegation storage d = delegations[operator][msg.sender];
        require(d.active, "No delegation");
        require(_canExitDelegation(operator, msg.sender), "Still locked");

        uint256 amount = d.amount;

        _claimRewards(msg.sender);

        d.active = false;
        d.amount = 0;

        totalDelegatedTo[operator] -= amount;

        zfiToken.safeTransfer(msg.sender, amount);

        emit DelegationWithdrawn(operator, msg.sender, amount);
    }

    function _canExitDelegation(address operator, address delegator) internal view returns (bool) {
        Delegation storage d = delegations[operator][delegator];

        if (block.timestamp >= d.unlockTime) {
            return true;
        }

        if (blacklisted[operator]) {
            return true;
        }

        NodeStake storage node = nodes[operator];
        if (!node.active) {
            return true;
        }

        return false;
    }

    function _claimRewards(address account) internal {
        uint256 zfiRewards = claimableRewards[account];
        uint256 zxmrRewards = claimableZXMR[account];

        bool isOperator = nodes[account].active && nodes[account].stakedAmount > 0;
        if (!isOperator) {
            zxmrRewards = 0;
        }

        if (zfiRewards > 0) {
            claimableRewards[account] = 0;
            zfiToken.safeTransfer(account, zfiRewards);
        }

        if (zxmrRewards > 0 && address(zxmrToken) != address(0)) {
            claimableZXMR[account] = 0;
            zxmrToken.safeTransfer(account, zxmrRewards);
        }

        if (zfiRewards > 0 || zxmrRewards > 0) {
            emit RewardsClaimed(account, zfiRewards, zxmrRewards);
        }
    }

    function claimRewards() external nonReentrant {
        _claimRewards(msg.sender);
    }

    function unstake() external nonReentrant {
        NodeStake storage node = nodes[msg.sender];
        require(node.active, "Not staked");
        require(block.timestamp >= node.stakeTime + LOCK_PERIOD, "Still locked");

        uint256 amount = node.stakedAmount;

        _claimRewards(msg.sender);

        node.active = false;
        totalStaked -= amount;

        _removeFromActiveNodes(msg.sender);

        zfiToken.safeTransfer(msg.sender, amount);

        emit Unstaked(msg.sender, amount);
    }
    
    function _removeFromActiveNodes(address _node) internal {
        for (uint256 i = 0; i < activeNodes.length; i++) {
            if (activeNodes[i] == _node) {
                activeNodes[i] = activeNodes[activeNodes.length - 1];
                activeNodes.pop();
                break;
            }
        }
    }

    function isBlacklisted(address _node) external view returns (bool) {
        return blacklisted[_node];
    }
    
    function getActiveNodesCount() external view returns (uint256) {
        return activeNodes.length;
    }
    
    function getActiveNodes() external view returns (address[] memory) {
        return activeNodes;
    }
    
    function getNodeInfo(address _node) external view returns (
        uint256 stakedAmount,
        uint256 stakeTime,
        uint256 lastHeartbeat,
        bool active,
        uint256 nodeVersion,
        uint256 slashingStage,
        uint256 lastProved
    ) {
        NodeStake memory node = nodes[_node];
        return (
            node.stakedAmount,
            node.stakeTime,
            node.lastHeartbeat,
            node.active,
            node.nodeVersion,
            node.slashingStage,
            lastProvedHeartbeat[_node]
        );
    }
    
    function getDissolutionPool() external view returns (uint256) {
        return dissolutionPool;
    }
}
