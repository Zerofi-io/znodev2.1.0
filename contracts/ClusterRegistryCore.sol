// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "./ZFIStaking.sol";

/// @title ClusterRegistryCore v3 - registration + off-chain selection + on-chain finalization
/// @notice Minimal registry: nodes register, clusters form off-chain, finalize on-chain
contract ClusterRegistryCore {
    struct NodeInfo {
        bytes32 codeHash;
        bool registered;
        bool inCluster;
    }

    struct ClusterInfo {
        address[] members;
        string moneroAddress;
        uint256 createdAt;
        bool finalized;
    }

    ZFIStaking public staking;

    uint256 public constant STAKE_REQUIRED = 1_000_000 * 10**18;

    mapping(address => NodeInfo) public nodes;
    mapping(address => bytes32) public nodeToCluster;
    address[] public registeredList;

    mapping(bytes32 => ClusterInfo) public clusters;
    mapping(bytes32 => bool) public dissolved;
    mapping(bytes32 => uint256) public clusterStage1SlashCount;
    uint256 public activeClusterCount;

    event NodeRegistered(address indexed node, bytes32 codeHash);
    event NodeUnregistered(address indexed node);
    event ClusterFormed(bytes32 indexed clusterId, address[] members, string moneroAddress);
    event ClusterDissolved(bytes32 indexed clusterId, address indexed caller, uint256 invalidCount, bool rewarded);
    event MemberKicked(bytes32 indexed clusterId, address indexed member);
    event MemberRejoined(bytes32 indexed clusterId, address indexed member);

    constructor(address _staking) {
        staking = ZFIStaking(_staking);
    }

    function registerNode(bytes32 codeHash) external {
        NodeInfo storage n = nodes[msg.sender];
        if (!n.registered) {
            registeredList.push(msg.sender);
        }
        n.codeHash = codeHash;
        n.registered = true;
        emit NodeRegistered(msg.sender, codeHash);
    }

    function unregisterNode() external {
        NodeInfo storage n = nodes[msg.sender];
        require(n.registered, "not registered");
        n.registered = false;
        emit NodeUnregistered(msg.sender);
    }

    function finalizeCluster(address[] calldata members, string calldata moneroAddress)
        external
        returns (bytes32 clusterId)
    {
        require(members.length == 11, "need 11 members");

        bool senderIsMember = false;
        address[] memory sorted = new address[](members.length);

        for (uint256 i = 0; i < members.length; i++) {
            sorted[i] = members[i];
            NodeInfo storage n = nodes[members[i]];
            require(n.registered, "unregistered member");
            require(!n.inCluster, "member already in cluster");
            if (members[i] == msg.sender) {
                senderIsMember = true;
            }
        }
        require(senderIsMember, "sender not in cluster");

        for (uint256 i = 0; i < sorted.length; i++) {
            for (uint256 j = i + 1; j < sorted.length; j++) {
                if (sorted[j] < sorted[i]) {
                    (sorted[i], sorted[j]) = (sorted[j], sorted[i]);
                }
            }
        }

        clusterId = keccak256(abi.encodePacked(sorted));
        ClusterInfo storage c = clusters[clusterId];
        require(!c.finalized, "cluster exists");

        c.members = members;
        c.moneroAddress = moneroAddress;
        c.createdAt = block.timestamp;
        c.finalized = true;

        for (uint256 i = 0; i < members.length; i++) {
            nodes[members[i]].inCluster = true;
            nodeToCluster[members[i]] = clusterId;
        }

        activeClusterCount += 1;
        emit ClusterFormed(clusterId, members, moneroAddress);
    }

    /// @notice Called by staking contract when a node receives stage-1 slash
    /// @return isThirdSlash True if this is the 3rd node slashed from this cluster
    function recordStage1Slash(address node) external returns (bool isThirdSlash) {
        require(msg.sender == address(staking), "Only staking");
        
        bytes32 clusterId = nodeToCluster[node];
        if (clusterId == bytes32(0) || dissolved[clusterId]) {
            return false;
        }
        
        clusterStage1SlashCount[clusterId]++;
        emit MemberKicked(clusterId, node);
        return clusterStage1SlashCount[clusterId] == 3;
    }

    /// @notice Check if a slashed member has topped up and can rejoin their cluster
    /// @dev Called by node after topping up stake to verify they can resume operations
    function canRejoinCluster(address node) external view returns (bool canRejoin, bytes32 clusterId) {
        clusterId = nodeToCluster[node];
        if (clusterId == bytes32(0)) {
            return (false, bytes32(0));
        }
        if (dissolved[clusterId]) {
            return (false, clusterId);
        }
        if (staking.blacklisted(node)) {
            return (false, clusterId);
        }
        (uint256 stakedAmount,,, bool active,,,) = staking.getNodeInfo(node);
        if (!active || stakedAmount < STAKE_REQUIRED) {
            return (false, clusterId);
        }
        return (true, clusterId);
    }

    /// @notice Dissolve a cluster when 3+ members are permanently invalid (blacklisted)
    /// @dev Only blacklisted members count - understaked members can still top up and rejoin
    function dissolveCluster(bytes32 clusterId) external returns (bool rewarded) {
        ClusterInfo storage c = clusters[clusterId];
        require(c.finalized, "cluster not finalized");
        require(!dissolved[clusterId], "already dissolved");

        uint256 blacklistedCount = 0;

        for (uint256 i = 0; i < c.members.length; i++) {
            if (staking.blacklisted(c.members[i])) {
                blacklistedCount++;
            }
        }

        require(blacklistedCount >= 3, "need 3+ blacklisted members");

        for (uint256 i = 0; i < c.members.length; i++) {
            nodes[c.members[i]].inCluster = false;
            nodeToCluster[c.members[i]] = bytes32(0);
        }

        dissolved[clusterId] = true;
        activeClusterCount -= 1;

        rewarded = staking.payDissolutionReward(msg.sender);

        emit ClusterDissolved(clusterId, msg.sender, blacklistedCount, rewarded);
    }

    /// @notice Check if cluster can be dissolved (3+ blacklisted members)
    function canDissolve(bytes32 clusterId) external view returns (bool, uint256 blacklistedCount) {
        ClusterInfo storage c = clusters[clusterId];
        if (!c.finalized || dissolved[clusterId]) {
            return (false, 0);
        }

        for (uint256 i = 0; i < c.members.length; i++) {
            if (staking.blacklisted(c.members[i])) {
                blacklistedCount++;
            }
        }

        return (blacklistedCount >= 3, blacklistedCount);
    }

    /// @notice Get cluster health: how many members are fully operational vs slashed vs blacklisted
    function getClusterHealth(bytes32 clusterId) external view returns (
        uint256 healthy,
        uint256 slashed,
        uint256 blacklisted
    ) {
        ClusterInfo storage c = clusters[clusterId];
        if (!c.finalized) {
            return (0, 0, 0);
        }

        for (uint256 i = 0; i < c.members.length; i++) {
            address member = c.members[i];
            if (staking.blacklisted(member)) {
                blacklisted++;
            } else {
                (uint256 stakedAmount,,, bool active,,,) = staking.getNodeInfo(member);
                if (active && stakedAmount >= STAKE_REQUIRED) {
                    healthy++;
                } else {
                    slashed++;
                }
            }
        }
    }

    function isClusterActive(bytes32 clusterId) external view returns (bool) {
        return clusters[clusterId].finalized && !dissolved[clusterId];
    }

    function getClusterMembers(bytes32 clusterId) external view returns (address[] memory) {
        return clusters[clusterId].members;
    }

    function getRegisteredNodes(uint256 offset, uint256 limit)
        external
        view
        returns (address[] memory slice)
    {
        uint256 n = registeredList.length;
        if (offset >= n) return new address[](0);
        uint256 end = offset + limit;
        if (end > n) end = n;
        slice = new address[](end - offset);
        for (uint256 i = offset; i < end; i++) {
            slice[i - offset] = registeredList[i];
        }
    }

    function canParticipate(address node) external view returns (bool) {
        NodeInfo memory n = nodes[node];
        return n.registered && !n.inCluster;
    }
}
