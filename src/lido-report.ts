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
    elRewardsVaultAddr: string;

  try {
    hashConsensusAddr = ethers.utils.getAddress(process.env.HASH_CONSENSUS_ADDRESS!);
    lidoAddr = ethers.utils.getAddress(process.env.LIDO_ADDRESS!);
    accountingOracleAddr = ethers.utils.getAddress(process.env.ACCOUNTING_ORACLE_ADDRESS!);
    withdrawalVaultAddr = ethers.utils.getAddress(process.env.WITHDRAWAL_VAULT_ADDRESS!);
    elRewardsVaultAddr = ethers.utils.getAddress(process.env.EL_REWARDS_VAULT_ADDRESS!);
  } catch (error) {
    console.error("Invalid Ethereum address in environment variables:", error);
    process.exit(1);
  }

  // Create an ethers provider using the custom RPC URL.
  const provider = new ethers.providers.JsonRpcProvider(rpcUrl);

  // Constants and defaults
  const ONE_GWEI = ethers.BigNumber.from("1000000000");
  const DEFAULT_CL_DIFF = ethers.utils.parseEther("10"); // 10 ETH diff to simulate CL balance change
  const DEFAULT_CL_APPEARED_VALIDATORS = ethers.BigNumber.from("0");
  const DEFAULT_SHARES_REQUESTED_TO_BURN = ethers.BigNumber.from("0");
  const DEFAULT_WITHDRAWAL_FINALIZATION_BATCHES: number[] = [];
  const DEFAULT_STAKING_MODULE_IDS: number[] = [];
  const DEFAULT_NUM_EXITED_VALIDATORS: number[] = [];
  const DEFAULT_IS_BUNKER_MODE = false;
  const DEFAULT_VAULTS_VALUES: number[] = [];
  const DEFAULT_IN_OUT_DELTAS: number[] = [];
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

  // Instantiate contract instances using validated addresses.
  const hashConsensus = new ethers.Contract(hashConsensusAddr, hashConsensusABI, provider);
  const lido = new ethers.Contract(lidoAddr, lidoABI, provider);
  const accountingOracle = new ethers.Contract(accountingOracleAddr, accountingOracleABI, provider);

  // Fetch data from the hashConsensus contract.
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

  // Query beacon statistics from the Lido contract.
  const [beaconValidators, beaconBalance] = await lido.getBeaconStat();
  // Calculate the "post‐CL" (consensus layer) balance by adding a simulated diff.
  const postCLBalance = beaconBalance.add(DEFAULT_CL_DIFF);
  // Compute the new number of validators.
  const postValidators = ethers.BigNumber.from(beaconValidators).add(DEFAULT_CL_APPEARED_VALIDATORS);
  // Compute the CL balance in Gwei.
  const clBalanceGwei = postCLBalance.div(ONE_GWEI);

  // Query the ETH balances that are held in the vault contracts.
  const withdrawalVaultBalanceBN = await provider.getBalance(withdrawalVaultAddr);
  const elRewardsVaultBalanceBN = await provider.getBalance(elRewardsVaultAddr);

  // Query the consensus version from the accounting oracle.
  const consensusVersionBN = await accountingOracle.getConsensusVersion();

  // Format ETH balances from wei to ETH strings.
  const withdrawalVaultBalance = ethers.utils.formatEther(withdrawalVaultBalanceBN);
  const elRewardsVaultBalance = ethers.utils.formatEther(elRewardsVaultBalanceBN);

  // Assemble the final report object.
  const report: Record<string, any> = {
    "Consensus Version": consensusVersionBN.toString(),
    "Reference Slot": refSlotBN.toString(),
    "CL Balance (Gwei)": clBalanceGwei.toString(),
    "Number of Validators": postValidators.toString(),
    "Withdrawal Vault Balance (ETH)": withdrawalVaultBalance,
    "EL Rewards Vault Balance (ETH)": elRewardsVaultBalance,
    "Shares Requested to Burn": DEFAULT_SHARES_REQUESTED_TO_BURN.toString(),
    "Withdrawal Finalization Batches": JSON.stringify(DEFAULT_WITHDRAWAL_FINALIZATION_BATCHES),
    "Is Bunker Mode": DEFAULT_IS_BUNKER_MODE,
    "Vaults Values": JSON.stringify(DEFAULT_VAULTS_VALUES),
    "Vaults In-Out Deltas": JSON.stringify(DEFAULT_IN_OUT_DELTAS),
    "Extra Data Format": DEFAULT_EXTRA_DATA_FORMAT,
    "Extra Data Hash": DEFAULT_EXTRA_DATA_HASH,
    "Extra Data Items Count": DEFAULT_EXTRA_DATA_ITEMS_COUNT,
    "Staking Module IDs with Newly Exited Validators": JSON.stringify(DEFAULT_STAKING_MODULE_IDS),
    "Number of Exited Validators by Staking Module": JSON.stringify(DEFAULT_NUM_EXITED_VALIDATORS)
  };

  // Build the CSV content.
  const headers = Object.keys(report);
  const headerLine = headers.join(",");
  const values = headers.map(h => report[h]);
  const valueLine = values.join(",");
  const csvContent = `${headerLine}\n${valueLine}\n`;

  // Write the CSV content to file.
  const outputPath = "./report.csv";
  fs.writeFileSync(outputPath, csvContent);
  console.log(`Report written to ${outputPath}`);
};

// Only call main() if this file is being run directly.
if (require.main === module) {
  main().catch((error) => {
    console.error(error);
    process.exit(1);
  });
} 