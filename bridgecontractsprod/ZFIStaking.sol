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

    uint256 public constant TOTAL_REWARD_POOL = 425_000_000 * 10**18;
    uint256 public constant DISSOLUTION_REWARD = 50_000 * 10**18;

    bytes32 public constant HEARTBEAT_TYPEHASH = keccak256("Heartbeat(address node,uint256 timestamp)");

    mapping(address => uint256) public lastProvedHeartbeat;

    uint256 public deploymentTime;
    uint256 public totalStaked;
    uint256 public totalDelegated;
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

    uint256 public rewardPerTokenStored;
    uint256 public lastUpdateTime;
    uint256 public totalRewardsDistributed;

    struct NodeStake {
        uint256 stakedAmount;
        uint256 stakeTime;
        uint256 lastClaimTime;
        uint256 lastHeartbeat;
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

    mapping(address => uint256) public rewardPerTokenPaidStake;

    struct Delegation {
        uint256 amount;
        uint64 startTime;
        uint64 unlockTime;
        bool active;
        uint256 rewardPerTokenPaid;
    }

    mapping(address => mapping(address => Delegation)) public delegations; // operator => delegator
    mapping(address => uint256) public totalDelegatedTo; // per operator

    mapping(address => address[]) private delegatorOperators; // delegator => operators
    mapping(address => mapping(address => uint256)) private delegatorOperatorIndex; // delegator => operator => index+1

    mapping(address => address[]) private operatorDelegators; // operator => delegators
    mapping(address => mapping(address => uint256)) private operatorDelegatorIndex; // operator => delegator => index+1

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
    event VersionUpdated(uint256 newVersion, uint256 deadline);
    event NodeVersionUpdated(address indexed node, uint256 oldVersion, uint256 newVersion);
    event DissolutionPoolFunded(address indexed slashedNode, uint256 amount);
    event DissolutionRewardPaid(address indexed recipient, uint256 amount);

    constructor(address _zfiToken) Ownable(msg.sender) EIP712("ZFIStaking", "1") {
        zfiToken = IERC20(_zfiToken);
        deploymentTime = block.timestamp;
        lastUpdateTime = block.timestamp;
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

    function _hashHeartbeat(address node, uint256 timestamp) internal view returns (bytes32) {
        return _hashTypedDataV4(keccak256(abi.encode(HEARTBEAT_TYPEHASH, node, timestamp)));
    }

    function _yearIndexAt(uint256 ts) internal view returns (uint256) {
        if (ts <= deploymentTime) {
            return 0;
        }
        uint256 elapsed = ts - deploymentTime;
        uint256 year = elapsed / 365 days;
        if (year >= 20) {
            return 19;
        }
        return year;
    }

    function _updateGlobalRewards() internal {
        uint256 currentTime = block.timestamp;
        if (currentTime <= lastUpdateTime) {
            return;
        }
        if (totalRewardsDistributed >= TOTAL_REWARD_POOL) {
            lastUpdateTime = currentTime;
            return;
        }

        uint256 supply = totalStaked + totalDelegated;
        if (supply == 0) {
            lastUpdateTime = currentTime;
            return;
        }

        uint256 from = lastUpdateTime;
        uint256 to = currentTime;
        uint256 remaining = TOTAL_REWARD_POOL - totalRewardsDistributed;
        uint256 reward;

        while (from < to && reward < remaining) {
            uint256 yearIndex = _yearIndexAt(from);
            uint256 endOfYear = deploymentTime + (yearIndex + 1) * 365 days;
            uint256 periodEnd = to < endOfYear ? to : endOfYear;
            uint256 dt = periodEnd - from;
            if (dt == 0) {
                from = periodEnd;
                continue;
            }

            uint256 apyBps = yearlyRewardPercentages[yearIndex];
            uint256 periodReward = (supply * apyBps * dt) / (10000 * 365 days);
            if (reward + periodReward > remaining) {
                periodReward = remaining - reward;
            }

            reward += periodReward;
            from = periodEnd;
        }

        if (reward > 0) {
            rewardPerTokenStored += (reward * 1e18) / supply;
            totalRewardsDistributed += reward;
        }

        lastUpdateTime = currentTime;
    }

    function _previewRewardPerToken() internal view returns (uint256) {
        uint256 supply = totalStaked + totalDelegated;
        if (supply == 0) {
            return rewardPerTokenStored;
        }
        if (block.timestamp <= lastUpdateTime || totalRewardsDistributed >= TOTAL_REWARD_POOL) {
            return rewardPerTokenStored;
        }

        uint256 from = lastUpdateTime;
        uint256 to = block.timestamp;
        uint256 distributed = totalRewardsDistributed;
        uint256 remaining = TOTAL_REWARD_POOL - distributed;
        uint256 reward;

        while (from < to && reward < remaining) {
            uint256 yearIndex = _yearIndexAt(from);
            uint256 endOfYear = deploymentTime + (yearIndex + 1) * 365 days;
            uint256 periodEnd = to < endOfYear ? to : endOfYear;
            uint256 dt = periodEnd - from;
            if (dt == 0) {
                from = periodEnd;
                continue;
            }

            uint256 apyBps = yearlyRewardPercentages[yearIndex];
            uint256 periodReward = (supply * apyBps * dt) / (10000 * 365 days);
            if (reward + periodReward > remaining) {
                periodReward = remaining - reward;
            }

            reward += periodReward;
            from = periodEnd;
        }

        if (reward == 0) {
            return rewardPerTokenStored;
        }

        return rewardPerTokenStored + (reward * 1e18) / supply;
    }

    function _updateOperatorStakeRewards(address account) internal {
        NodeStake storage node = nodes[account];
        if (!node.active || node.stakedAmount == 0) {
            rewardPerTokenPaidStake[account] = rewardPerTokenStored;
            return;
        }

        uint256 paid = rewardPerTokenPaidStake[account];
        uint256 delta = rewardPerTokenStored - paid;
        if (delta == 0) {
            return;
        }

        uint256 accrued = (node.stakedAmount * delta) / 1e18;
        if (accrued > 0) {
            claimableRewards[account] += accrued;
        }
        rewardPerTokenPaidStake[account] = rewardPerTokenStored;
    }

    function _updateDelegationRewards(address operator, address delegator) internal {
        Delegation storage d = delegations[operator][delegator];
        if (!d.active || d.amount == 0) {
            d.rewardPerTokenPaid = rewardPerTokenStored;
            return;
        }

        uint256 delta = rewardPerTokenStored - d.rewardPerTokenPaid;
        if (delta == 0) {
            return;
        }

        uint256 gross = (d.amount * delta) / 1e18;
        if (gross > 0) {
            uint256 operatorCut = gross / 5;
            uint256 delegatorShare = gross - operatorCut;
            claimableRewards[delegator] += delegatorShare;
            claimableRewards[operator] += operatorCut;
        }

        d.rewardPerTokenPaid = rewardPerTokenStored;
    }

    function _updateRewardsForAccount(address account) internal {
        _updateOperatorStakeRewards(account);

        address[] storage ops = delegatorOperators[account];
        uint256 len = ops.length;
        for (uint256 i = 0; i < len; i++) {
            _updateDelegationRewards(ops[i], account);
        }

        address[] storage dels = operatorDelegators[account];
        len = dels.length;
        for (uint256 j = 0; j < len; j++) {
            _updateDelegationRewards(account, dels[j]);
        }
    }

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

        bytes32 digest = _hashHeartbeat(_node, lastHeartbeatTs);
        address signer = digest.recover(sig);
        require(signer == _node, "Invalid heartbeat signature");

        _updateGlobalRewards();
        _updateRewardsForAccount(_node);

        uint256 hoursOffline = (block.timestamp - lastHeartbeatTs) / 1 hours;
        _applyProgressiveSlashing(_node, node, hoursOffline);

        if (lastHeartbeatTs > lastProvedHeartbeat[_node]) {
            lastProvedHeartbeat[_node] = lastHeartbeatTs;
        }
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

        uint256 callerReward = slashAmount / 10;
        uint256 burnAmount;
        uint256 poolAmount;

        if (newStage == 1) {
            bool isThirdSlash = false;
            if (clusterRegistry != address(0)) {
                try IClusterRegistry(clusterRegistry).recordStage1Slash(_node) returns (bool result) {
                    isThirdSlash = result;
                } catch {}
            }

            if (isThirdSlash) {
                poolAmount = slashAmount / 5;
            } else {
                poolAmount = slashAmount / 10;
            }
            burnAmount = slashAmount - callerReward - poolAmount;
            dissolutionPool += poolAmount;
            emit DissolutionPoolFunded(_node, poolAmount);
        } else {
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

        _updateGlobalRewards();
        _updateRewardsForAccount(msg.sender);

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

        rewardPerTokenPaidStake[msg.sender] = rewardPerTokenStored;

        activeNodes.push(msg.sender);
        totalStaked += STAKE_AMOUNT;

        emit Staked(msg.sender, STAKE_AMOUNT, _codeHash);
    }

    function topUpStake() external nonReentrant {
        require(!blacklisted[msg.sender], "Blacklisted");
        NodeStake storage node = nodes[msg.sender];
        require(node.stakedAmount > 0, "Not staked");
        require(node.stakedAmount < STAKE_AMOUNT, "Already at full stake");

        _updateGlobalRewards();
        _updateRewardsForAccount(msg.sender);

        uint256 deficit = STAKE_AMOUNT - node.stakedAmount;

        zfiToken.safeTransferFrom(msg.sender, address(this), deficit);

        node.stakedAmount = STAKE_AMOUNT;
        node.active = true;
        totalStaked += deficit;

        rewardPerTokenPaidStake[msg.sender] = rewardPerTokenStored;

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

    function addStake(uint256 amount) external nonReentrant {
        require(amount > 0, "Amount must be positive");
        require(!blacklisted[msg.sender], "Blacklisted");
        NodeStake storage node = nodes[msg.sender];
        require(node.active, "Not staked");

        _updateGlobalRewards();
        _updateRewardsForAccount(msg.sender);

        zfiToken.safeTransferFrom(msg.sender, address(this), amount);

        node.stakedAmount += amount;
        totalStaked += amount;

        rewardPerTokenPaidStake[msg.sender] = rewardPerTokenStored;

        emit Staked(msg.sender, amount, node.codeHash);
    }

    function _addDelegationRelation(address operator, address delegator) internal {
        address[] storage ops = delegatorOperators[delegator];
        if (delegatorOperatorIndex[delegator][operator] == 0) {
            ops.push(operator);
            delegatorOperatorIndex[delegator][operator] = ops.length;
        }

        address[] storage dels = operatorDelegators[operator];
        if (operatorDelegatorIndex[operator][delegator] == 0) {
            dels.push(delegator);
            operatorDelegatorIndex[operator][delegator] = dels.length;
        }
    }

    function _removeDelegationRelation(address operator, address delegator) internal {
        uint256 idx = delegatorOperatorIndex[delegator][operator];
        if (idx != 0) {
            address[] storage ops = delegatorOperators[delegator];
            uint256 lastIndex = ops.length;
            if (idx != lastIndex) {
                address lastOp = ops[lastIndex - 1];
                ops[idx - 1] = lastOp;
                delegatorOperatorIndex[delegator][lastOp] = idx;
            }
            ops.pop();
            delegatorOperatorIndex[delegator][operator] = 0;
        }

        idx = operatorDelegatorIndex[operator][delegator];
        if (idx != 0) {
            address[] storage dels = operatorDelegators[operator];
            uint256 lastIndex2 = dels.length;
            if (idx != lastIndex2) {
                address lastDel = dels[lastIndex2 - 1];
                dels[idx - 1] = lastDel;
                operatorDelegatorIndex[operator][lastDel] = idx;
            }
            dels.pop();
            operatorDelegatorIndex[operator][delegator] = 0;
        }
    }

    function delegateStake(address operator, uint256 amount) external nonReentrant {
        require(amount > 0, "Amount must be positive");
        NodeStake storage node = nodes[operator];
        require(node.active, "Operator not active");
        require(!blacklisted[operator], "Operator blacklisted");

        _updateGlobalRewards();
        _updateRewardsForAccount(msg.sender);

        Delegation storage d = delegations[operator][msg.sender];

        zfiToken.safeTransferFrom(msg.sender, address(this), amount);

        if (!d.active) {
            _addDelegationRelation(operator, msg.sender);
            d.startTime = uint64(block.timestamp);
            d.unlockTime = uint64(block.timestamp + LOCK_PERIOD);
            d.active = true;
            d.rewardPerTokenPaid = rewardPerTokenStored;
        } else {
            uint64 newUnlock = uint64(block.timestamp + LOCK_PERIOD);
            if (newUnlock > d.unlockTime) {
                d.unlockTime = newUnlock;
            }
        }

        d.amount += amount;
        totalDelegatedTo[operator] += amount;
        totalDelegated += amount;

        emit Delegated(operator, msg.sender, amount);
    }

    function withdrawDelegation(address operator) external nonReentrant {
        Delegation storage d = delegations[operator][msg.sender];
        require(d.active, "No delegation");
        require(_canExitDelegation(operator, msg.sender), "Still locked");

        _updateGlobalRewards();
        _updateRewardsForAccount(msg.sender);

        uint256 amount = d.amount;

        totalDelegatedTo[operator] -= amount;
        totalDelegated -= amount;

        d.active = false;
        d.amount = 0;
        d.rewardPerTokenPaid = rewardPerTokenStored;

        _removeDelegationRelation(operator, msg.sender);

        _claimRewards(msg.sender);

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
        _updateGlobalRewards();
        _updateRewardsForAccount(msg.sender);
        _claimRewards(msg.sender);
    }

    function unstake() external nonReentrant {
        NodeStake storage node = nodes[msg.sender];
        require(node.active, "Not staked");
        require(block.timestamp >= node.stakeTime + LOCK_PERIOD, "Still locked");

        _updateGlobalRewards();
        _updateRewardsForAccount(msg.sender);

        uint256 amount = node.stakedAmount;

        node.active = false;
        totalStaked -= amount;

        _removeFromActiveNodes(msg.sender);

        _claimRewards(msg.sender);

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

    function previewRewardPerToken() external view returns (uint256) {
        return _previewRewardPerToken();
    }

    function previewRewards(address account) external view returns (uint256) {
        uint256 rpt = _previewRewardPerToken();
        uint256 rewards = claimableRewards[account];

        NodeStake memory node = nodes[account];
        if (node.active && node.stakedAmount > 0) {
            uint256 deltaStake = rpt - rewardPerTokenPaidStake[account];
            rewards += (node.stakedAmount * deltaStake) / 1e18;
        }

        address[] storage ops = delegatorOperators[account];
        uint256 len = ops.length;
        for (uint256 i = 0; i < len; i++) {
            Delegation storage d = delegations[ops[i]][account];
            if (d.active && d.amount > 0) {
                uint256 delta = rpt - d.rewardPerTokenPaid;
                uint256 gross = (d.amount * delta) / 1e18;
                if (gross > 0) {
                    uint256 operatorCut = gross / 5;
                    uint256 delegatorShare = gross - operatorCut;
                    rewards += delegatorShare;
                }
            }
        }

        address[] storage dels = operatorDelegators[account];
        len = dels.length;
        for (uint256 j = 0; j < len; j++) {
            Delegation storage d2 = delegations[account][dels[j]];
            if (d2.active && d2.amount > 0) {
                uint256 delta2 = rpt - d2.rewardPerTokenPaid;
                uint256 gross2 = (d2.amount * delta2) / 1e18;
                if (gross2 > 0) {
                    uint256 operatorCut2 = gross2 / 5;
                    rewards += operatorCut2;
                }
            }
        }

        return rewards;
    }
}
