// SPDX-License-Identifier: MIT
pragma solidity 0.8.20;

import "@openzeppelin/contracts/access/Ownable.sol";
import "@openzeppelin/contracts/utils/ReentrancyGuard.sol";
import "@openzeppelin/contracts/utils/cryptography/ECDSA.sol";
import "@openzeppelin/contracts/utils/cryptography/MessageHashUtils.sol";

import "./ZxmrToken.sol";
import "./ZxmrFeePool.sol";

contract XMRBridgeV2 is Ownable, ReentrancyGuard {
    using ECDSA for bytes32;
    using MessageHashUtils for bytes32;

    uint256 public constant FEE_BPS = 100;
    uint256 public constant REQUIRED_SIGNATURES = 7;

    ZxmrToken public immutable zxmr;
    ZxmrFeePool public immutable feePool;
    IClusterRegistry public immutable registry;

    mapping(bytes32 => bool) public usedDepositIds;

    event TokensMinted(
        address indexed user,
        bytes32 indexed clusterId,
        bytes32 indexed depositId,
        uint256 userAmount,
        uint256 fee
    );

    event TokensBurned(
        address indexed user,
        bytes32 indexed clusterId,
        string xmrAddress,
        uint256 burnAmount,
        uint256 fee
    );

    constructor(
        IClusterRegistry _registry,
        ZxmrToken _zxmr,
        ZxmrFeePool _feePool
    ) Ownable(msg.sender) {
        require(address(_registry) != address(0), "Invalid registry");
        require(address(_zxmr) != address(0), "Invalid token");
        require(address(_feePool) != address(0), "Invalid pool");
        registry = _registry;
        zxmr = _zxmr;
        feePool = _feePool;
    }

    function mint(
        bytes32 clusterId,
        bytes32 depositId,
        uint256 amount,
        bytes[] calldata signatures
    ) external nonReentrant {
        require(amount > 0, "Amount must be positive");
        require(!usedDepositIds[depositId], "Deposit already processed");
        require(signatures.length >= REQUIRED_SIGNATURES, "Need 7 signatures");
        require(registry.isClusterActive(clusterId), "Cluster not active");

        address[] memory members = registry.getClusterMembers(clusterId);
        require(members.length >= REQUIRED_SIGNATURES, "Invalid cluster");

        bytes32 messageHash = keccak256(
            abi.encodePacked(msg.sender, clusterId, depositId, amount)
        ).toEthSignedMessageHash();

        address[] memory signers = new address[](signatures.length);
        for (uint256 i = 0; i < signatures.length; i++) {
            address signer = messageHash.recover(signatures[i]);
            require(_isMember(members, signer), "Signer not member");
            require(!_isDuplicate(signers, signer, i), "Duplicate signer");
            signers[i] = signer;
        }

        usedDepositIds[depositId] = true;

        uint256 fee = (amount * FEE_BPS) / 10000;
        uint256 userAmount = amount - fee;

        zxmr.mint(msg.sender, userAmount);
        zxmr.mint(address(feePool), fee);
        feePool.addClusterFees(clusterId, fee);

        emit TokensMinted(msg.sender, clusterId, depositId, userAmount, fee);
    }

    function burnForXMR(
        bytes32 clusterId,
        uint256 amount,
        string calldata xmrAddress
    ) external nonReentrant {
        require(amount > 0, "Amount must be positive");
        require(bytes(xmrAddress).length > 0, "Invalid XMR address");
        require(registry.isClusterActive(clusterId), "Cluster not active");

        uint256 fee = (amount * FEE_BPS) / 10000;
        uint256 burnAmount = amount - fee;

        require(zxmr.transferFrom(msg.sender, address(this), amount), "Transfer failed");

        zxmr.burnFrom(address(this), burnAmount);
        zxmr.transfer(address(feePool), fee);
        feePool.addClusterFees(clusterId, fee);

        emit TokensBurned(msg.sender, clusterId, xmrAddress, burnAmount, fee);
    }

    function _isMember(address[] memory members, address addr) internal pure returns (bool) {
        for (uint256 i = 0; i < members.length; i++) {
            if (members[i] == addr) {
                return true;
            }
        }
        return false;
    }

    function _isDuplicate(address[] memory arr, address addr, uint256 upTo) internal pure returns (bool) {
        for (uint256 i = 0; i < upTo; i++) {
            if (arr[i] == addr) {
                return true;
            }
        }
        return false;
    }
}
