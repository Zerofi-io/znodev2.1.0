// SPDX-License-Identifier: MIT
pragma solidity 0.8.20;

import "@openzeppelin/contracts/access/Ownable.sol";
import "@openzeppelin/contracts/token/ERC20/IERC20.sol";

interface IClusterRegistry {
    function getClusterMembers(bytes32 clusterId) external view returns (address[] memory);
    function isClusterActive(bytes32 clusterId) external view returns (bool);
}

interface IZFIStaking {
    function getNodeInfo(address _node) external view returns (
        uint256 stakedAmount,
        uint256 stakeTime,
        uint256 lastHeartbeat,
        bool active,
        uint256 nodeVersion,
        uint256 slashingStage,
        uint256 lastProved
    );
}

contract ZxmrFeePool is Ownable {
    IERC20 public immutable zxmr;
    IClusterRegistry public immutable registry;
    IZFIStaking public staking;
    address public bridge;

    mapping(bytes32 => uint256) public clusterFees;
    mapping(bytes32 => mapping(address => uint256)) public claimed;

    event FeesAdded(bytes32 indexed clusterId, uint256 amount);
    event FeesClaimed(bytes32 indexed clusterId, address indexed node, uint256 amount);
    event BridgeUpdated(address indexed newBridge);
    event StakingUpdated(address indexed newStaking);

    constructor(IERC20 _zxmr, IClusterRegistry _registry) Ownable(msg.sender) {
        require(address(_zxmr) != address(0), "Invalid token");
        require(address(_registry) != address(0), "Invalid registry");
        zxmr = _zxmr;
        registry = _registry;
    }

    function setBridge(address _bridge) external onlyOwner {
        require(_bridge != address(0), "Invalid bridge");
        bridge = _bridge;
        emit BridgeUpdated(_bridge);
    }

    function setStaking(address _staking) external onlyOwner {
        require(_staking != address(0), "Invalid staking");
        staking = IZFIStaking(_staking);
        emit StakingUpdated(_staking);
    }

    modifier onlyBridge() {
        require(msg.sender == bridge, "Not bridge");
        _;
    }

    function addClusterFees(bytes32 clusterId, uint256 amount) external onlyBridge {
        require(amount > 0, "Amount zero");
        clusterFees[clusterId] += amount;
        emit FeesAdded(clusterId, amount);
    }

    function _computeWeights(bytes32 clusterId) internal view returns (address[] memory members, uint256 totalFees, uint256 totalStake) {
        members = registry.getClusterMembers(clusterId);
        uint256 n = members.length;
        require(n > 0, "Cluster empty");

        totalFees = clusterFees[clusterId];
        require(totalFees > 0, "No fees");

        if (address(staking) == address(0)) {
            return (members, totalFees, 0);
        }

        for (uint256 i = 0; i < n; i++) {
            (uint256 staked,, , bool active,,,) = staking.getNodeInfo(members[i]);
            if (active && staked > 0) {
                totalStake += staked;
            }
        }

        return (members, totalFees, totalStake);
    }

    function claimFromCluster(bytes32 clusterId) external {
        (address[] memory members, uint256 total, uint256 totalStake) = _computeWeights(clusterId);
        uint256 n = members.length;

        bool isMember = false;
        uint256 nodeStake = 0;

        if (address(staking) == address(0) || totalStake == 0) {
            for (uint256 i = 0; i < n; i++) {
                if (members[i] == msg.sender) {
                    isMember = true;
                    break;
                }
            }
            require(isMember, "Not member");

            uint256 entitlementPerNode = total / n;
            uint256 alreadyFlat = claimed[clusterId][msg.sender];
            require(entitlementPerNode > alreadyFlat, "Nothing to claim");
            uint256 amountFlat = entitlementPerNode - alreadyFlat;

            claimed[clusterId][msg.sender] = entitlementPerNode;
            require(zxmr.transfer(msg.sender, amountFlat), "Transfer failed");
            emit FeesClaimed(clusterId, msg.sender, amountFlat);
            return;
        }

        for (uint256 i = 0; i < n; i++) {
            if (members[i] == msg.sender) {
                isMember = true;
                (uint256 staked,, , bool active,,,) = staking.getNodeInfo(members[i]);
                if (active && staked > 0) {
                    nodeStake = staked;
                }
                break;
            }
        }
        require(isMember, "Not member");
        require(nodeStake > 0, "No stake");

        uint256 entitlement = (total * nodeStake) / totalStake;
        uint256 already = claimed[clusterId][msg.sender];
        require(entitlement > already, "Nothing to claim");
        uint256 amount = entitlement - already;

        claimed[clusterId][msg.sender] = entitlement;
        require(zxmr.transfer(msg.sender, amount), "Transfer failed");
        emit FeesClaimed(clusterId, msg.sender, amount);
    }

    function getClaimable(bytes32 clusterId, address node) external view returns (uint256) {
        (address[] memory members, uint256 total, uint256 totalStake) = _computeWeights(clusterId);
        uint256 n = members.length;

        bool isMember = false;
        uint256 nodeStake = 0;

        if (address(staking) == address(0) || totalStake == 0) {
            for (uint256 i = 0; i < n; i++) {
                if (members[i] == node) {
                    isMember = true;
                    break;
                }
            }
            if (!isMember) return 0;
            if (total == 0) return 0;

            uint256 entitlementPerNode = total / n;
            uint256 alreadyFlat = claimed[clusterId][node];
            if (entitlementPerNode <= alreadyFlat) return 0;
            return entitlementPerNode - alreadyFlat;
        }

        for (uint256 i = 0; i < n; i++) {
            if (members[i] == node) {
                isMember = true;
                (uint256 staked,, , bool active,,,) = staking.getNodeInfo(members[i]);
                if (active && staked > 0) {
                    nodeStake = staked;
                }
                break;
            }
        }
        if (!isMember) return 0;
        if (nodeStake == 0) return 0;

        uint256 entitlement = (total * nodeStake) / totalStake;
        uint256 already = claimed[clusterId][node];
        if (entitlement <= already) return 0;
        return entitlement - already;
    }
}
