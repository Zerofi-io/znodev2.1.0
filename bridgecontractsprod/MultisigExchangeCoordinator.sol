// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

contract MultisigExchangeCoordinator {
    // Multisig exchange round tracking
    mapping(bytes32 => mapping(uint8 => mapping(address => string))) public multisigExchangeData;
    mapping(bytes32 => mapping(uint8 => uint8)) public exchangeRoundSubmissions;
    mapping(bytes32 => mapping(uint8 => bool)) public exchangeRoundComplete;
    
    event ExchangeInfoSubmitted(bytes32 indexed clusterId, uint8 round, address indexed node);
    
    /// @notice Submit exchange_multisig_keys output for a specific round
    function submitExchangeInfo(
        bytes32 clusterId,
        uint8 round,
        string calldata exchangeInfo,
        address[] calldata clusterNodes
    ) external {
        // Allow all logical cluster rounds (0-10 by current node config).
        // If MAX_KEY_EXCHANGE_ROUNDS is increased in the node, this bound may need to be revisited.
        require(round <= 15, "Invalid round");
        require(bytes(exchangeInfo).length > 0, "Empty exchange info");
        
        // Verify sender is in cluster
        bool isValid = false;
        for (uint256 i = 0; i < clusterNodes.length; i++) {
            if (clusterNodes[i] == msg.sender) {
                isValid = true;
                break;
            }
        }
        require(isValid, "Not in cluster");
        
        // Only allow one submission per node per round
        require(bytes(multisigExchangeData[clusterId][round][msg.sender]).length == 0, "Already submitted");
        
        multisigExchangeData[clusterId][round][msg.sender] = exchangeInfo;
        exchangeRoundSubmissions[clusterId][round]++;
        
        // Mark round as complete when all nodes have submitted
        if (exchangeRoundSubmissions[clusterId][round] == uint8(clusterNodes.length)) {
            exchangeRoundComplete[clusterId][round] = true;
        }
        
        emit ExchangeInfoSubmitted(clusterId, round, msg.sender);
    }
    
    /// @notice Get all exchange info for a specific round
    function getExchangeRoundInfo(
        bytes32 clusterId,
        uint8 round,
        address[] calldata clusterNodes
    ) external view returns (
        address[] memory addresses,
        string[] memory exchangeInfos
    ) {
        addresses = new address[](clusterNodes.length);
        exchangeInfos = new string[](clusterNodes.length);
        
        for (uint256 i = 0; i < clusterNodes.length; i++) {
            addresses[i] = clusterNodes[i];
            exchangeInfos[i] = multisigExchangeData[clusterId][round][clusterNodes[i]];
        }
        
        return (addresses, exchangeInfos);
    }
    
    /// @notice Check if an exchange round is complete
    function getExchangeRoundStatus(bytes32 clusterId, uint8 round) external view returns (
        bool complete,
        uint8 submitted
    ) {
        return (
            exchangeRoundComplete[clusterId][round],
            exchangeRoundSubmissions[clusterId][round]
        );
    }
}
