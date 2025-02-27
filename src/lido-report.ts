import dotenv from "dotenv";
import { ethers } from "ethers";
import fs from "fs";

// Load environment variables from .env
dotenv.config();

export const main = async () => {
  // Get RPC URL from environment variables.
  const rpcUrl = process.env.RPC_URL;
  if (!rpcUrl) {
    console.error("RPC_URL must be defined in your .env file.");
    process.exit(1);
  }

  // Validate and checksum all contract addresses using ethers.utils.getAddress.
  let hashConsensusAddr: string,
    lidoAddr: string,
    accountingOracleAddr: string,
    withdrawalVaultAddr: string,
    elRewardsVaultAddr: string,
    burnerAddr: string;

  try {
    hashConsensusAddr = ethers.utils.getAddress(process.env.HASH_CONSENSUS_ADDRESS!);
    lidoAddr = ethers.utils.getAddress(process.env.LIDO_ADDRESS!);
    accountingOracleAddr = ethers.utils.getAddress(process.env.ACCOUNTING_ORACLE_ADDRESS!);
    withdrawalVaultAddr = ethers.utils.getAddress(process.env.WITHDRAWAL_VAULT_ADDRESS!);
    elRewardsVaultAddr = ethers.utils.getAddress(process.env.EL_REWARDS_VAULT_ADDRESS!);
    burnerAddr = ethers.utils.getAddress(process.env.BURNER_ADDRESS!);
  } catch (error) {
    console.error("Invalid Ethereum address in environment variables:", error);
    process.exit(1);
  }

  // Create an ethers provider using the custom RPC URL.
  const provider = new ethers.providers.JsonRpcProvider(rpcUrl);

  // After creating the provider but before making contract calls
  // Get latest block information
  const latestBlock = await provider.getBlock('latest');
  if (!latestBlock) {
    throw new Error("Failed to fetch latest block");
  }

  // Constants and defaults
  const ONE_GWEI = ethers.BigNumber.from("1000000000");
  const DEFAULT_CL_DIFF = ethers.utils.parseEther("0"); 
  const DEFAULT_CL_APPEARED_VALIDATORS = ethers.BigNumber.from("0");
  const DEFAULT_WITHDRAWAL_FINALIZATION_BATCHES: number[] = [];
  const DEFAULT_STAKING_MODULE_IDS: number[] = [];
  const DEFAULT_NUM_EXITED_VALIDATORS: number[] = [];
  const DEFAULT_IS_BUNKER_MODE = false;
  const DEFAULT_EXTRA_DATA_FORMAT = 0;
  const DEFAULT_EXTRA_DATA_HASH = "0x" + "0".repeat(64); // 32 bytes of zeros in hex
  const DEFAULT_EXTRA_DATA_ITEMS_COUNT = 0;

  // Minimum ABIs for the required contract calls.
  const hashConsensusABI = [
    "function getCurrentFrame() view returns (uint256)"
  ];
  const lidoABI = [
    "function getBeaconStat() view returns (uint256 beaconValidators, uint256 beaconBalance)"
  ];
  const accountingOracleABI = [
    "function getConsensusVersion() view returns (uint256)"
  ];

  // Burner ABI, for shares requested to burn
  const burnerABI = [
    "function getSharesRequestedToBurn() view returns (uint256 coverShares, uint256 nonCoverShares)"
  ];

  // Instantiate contract instances using validated addresses.
  const hashConsensus = new ethers.Contract(hashConsensusAddr, hashConsensusABI, provider);
  const lido = new ethers.Contract(lidoAddr, lidoABI, provider);
  const accountingOracle = new ethers.Contract(accountingOracleAddr, accountingOracleABI, provider);
  const burner = new ethers.Contract(burnerAddr, burnerABI, provider);

  // 1) Fetch data from the hashConsensus contract.
  const currentFrameResult = await hashConsensus.getCurrentFrame();
  // Handle different possible return types from getCurrentFrame
  let refSlotBN: ethers.BigNumber;
  if (currentFrameResult && currentFrameResult.refSlot !== undefined) {
    // The contract returned an object with a refSlot property
    refSlotBN = currentFrameResult.refSlot;
  } else {
    // The contract returned a BigNumber (or something else)
    refSlotBN = ethers.BigNumber.isBigNumber(currentFrameResult)
      ? currentFrameResult
      : ethers.BigNumber.from("0"); // Fallback in case of unexpected data
  }
  if (refSlotBN.isZero()) {
    console.warn("Warning: getCurrentFrame() returned 0 or an unexpected result. Please verify the contract.");
  }

  // 2) Query beacon statistics from the Lido contract.
  const [beaconValidators, beaconBalance] = await lido.getBeaconStat();

  // Calculate the "post‐CL" (consensus layer) balance by adding a simulated diff.
  const postCLBalance = beaconBalance.add(DEFAULT_CL_DIFF);
  // Compute the new number of validators.
  const postValidators = ethers.BigNumber.from(beaconValidators).add(DEFAULT_CL_APPEARED_VALIDATORS);
  // Compute the CL balance in Gwei.
  const clBalanceGwei = postCLBalance.div(ONE_GWEI);

  // 3) Query the ETH balances in the vault contracts (Withdrawal / EL Rewards).
  const withdrawalVaultBalanceBN = await provider.getBalance(withdrawalVaultAddr);
  const elRewardsVaultBalanceBN = await provider.getBalance(elRewardsVaultAddr);

  // 4) Query the consensus version from the accounting oracle.
  const consensusVersionBN = await accountingOracle.getConsensusVersion();

  // 5) Format ETH balances from wei to ETH strings.
  const withdrawalVaultBalance = ethers.utils.formatEther(withdrawalVaultBalanceBN);
  const elRewardsVaultBalance = ethers.utils.formatEther(elRewardsVaultBalanceBN);

  // 6) Fetch shares requested to burn
  const [coverShares, nonCoverShares] = await burner.getSharesRequestedToBurn();
  const sharesRequestedToBurnBN = coverShares.add(nonCoverShares);

  // 7) Simulate withdrawal batches (placeholder logic)
  const withdrawalFinalizationBatches = await simulateWithdrawalBatches();

  // 8) Assemble the final report object.
  const report: Record<string, any> = {
    "Process Timestamp": Math.floor(Date.now() / 1000).toString(),
    "Block Number": latestBlock.number.toString(),
    "Block Timestamp": latestBlock.timestamp.toString(),
    "Block Hash": latestBlock.hash,
    "Consensus Version": consensusVersionBN.toString(),
    "Reference Slot": refSlotBN.toString(),
    "CL Balance (Gwei)": clBalanceGwei.toString(),
    "Number of Validators": postValidators.toString(),
    "Withdrawal Vault Balance (ETH)": withdrawalVaultBalance,
    "EL Rewards Vault Balance (ETH)": elRewardsVaultBalance,
    "Shares Requested to Burn": sharesRequestedToBurnBN.toString(),
    "Withdrawal Finalization Batches": JSON.stringify(withdrawalFinalizationBatches),
    "Is Bunker Mode": DEFAULT_IS_BUNKER_MODE,
    // Removed "Vaults Values" and "Vaults In-Out Deltas"
    "Extra Data Format": DEFAULT_EXTRA_DATA_FORMAT,
    "Extra Data Hash": DEFAULT_EXTRA_DATA_HASH,
    "Extra Data Items Count": DEFAULT_EXTRA_DATA_ITEMS_COUNT,
    "Staking Module IDs with Newly Exited Validators": JSON.stringify(DEFAULT_STAKING_MODULE_IDS),
    "Number of Exited Validators by Staking Module": JSON.stringify(DEFAULT_NUM_EXITED_VALIDATORS),
  };

  // Helper function to escape CSV values
  const escapeCSV = (value: any): string => {
    if (value === null || value === undefined) {
      return '';
    }
    const stringValue = String(value);
    // If value contains comma, newline, or double quote, wrap in quotes
    if (stringValue.includes(',') || stringValue.includes('\n') || stringValue.includes('"')) {
      // Double up any existing quotes and wrap in quotes
      return `"${stringValue.replace(/"/g, '""')}"`;
    }
    return stringValue;
  };

  // 9) Build the CSV content with proper escaping
  const headers = Object.keys(report);
  const headerLine = headers.join(',');
  const values = headers.map(h => escapeCSV(report[h]));
  const valueLine = values.join(',');
  const csvContent = `${headerLine}\n${valueLine}\n`;

  // 10) Write the CSV content to file.
  const outputPath = "./report.csv";
  fs.writeFileSync(outputPath, csvContent);
  console.log(`Report written to ${outputPath}`);
};

// Add helper function for withdrawal batch simulation
async function simulateWithdrawalBatches(): Promise<number[]> {
  // Placeholder implementation - replace with actual logic
  return [1, 2, 3];
}

// Only call main() if this file is being run directly.
if (require.main === module) {
  main().catch((error) => {
    console.error(error);
    process.exit(1);
  });
}