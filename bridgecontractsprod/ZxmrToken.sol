// SPDX-License-Identifier: MIT
pragma solidity 0.8.20;

import "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import "@openzeppelin/contracts/access/Ownable.sol";

contract ZxmrToken is ERC20, Ownable {
    address public bridge;

    constructor() ERC20("Wrapped Monero", "zXMR") Ownable(msg.sender) {}

    function setBridge(address _bridge) external onlyOwner {
        require(_bridge != address(0), "Invalid bridge");
        bridge = _bridge;
    }

    modifier onlyBridge() {
        require(msg.sender == bridge, "Not bridge");
        _;
    }

    function mint(address to, uint256 amount) external onlyBridge {
        _mint(to, amount);
    }

    function burnFrom(address from, uint256 amount) external onlyBridge {
        _burn(from, amount);
    }
}

