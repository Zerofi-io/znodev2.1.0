// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import "@openzeppelin/contracts/access/Ownable.sol";

contract ZFI is ERC20, Ownable {
    uint256 public constant TOTAL_SUPPLY = 1_000_000_000 * 10**18;

    constructor() ERC20("ZFI", "ZFI") Ownable(msg.sender) {
        _mint(msg.sender, TOTAL_SUPPLY);
    }
}
