import dotenv from "dotenv";
import { 
  BigNumber, 
  getAddress, 
  parseEther,
  formatEther,
  JsonRpcProvider,
  Contract
} from "ethers";
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

  // Validate and checksum all contract addresses using ethers' getAddress
  let hashConsensusAddr: string,
    lidoAddr: string,
    accountingOracleAddr: string,
    withdrawalVaultAddr: string,
    elRewardsVaultAddr: string,
    burnerAddr: string;

  try {
    hashConsensusAddr = getAddress(process.env.HASH_CONSENSUS_ADDRESS!);
    lidoAddr = getAddress(process.env.LIDO_ADDRESS!);
    accountingOracleAddr = getAddress(process.env.ACCOUNTING_ORACLE_ADDRESS!);
    withdrawalVaultAddr = getAddress(process.env.WITHDRAWAL_VAULT_ADDRESS!);
    elRewardsVaultAddr = getAddress(process.env.EL_REWARDS_VAULT_ADDRESS!);
    burnerAddr = getAddress(process.env.BURNER_ADDRESS!);
  } catch (error) {
    console.error("Invalid Ethereum address in environment variables:", error);
    process.exit(1);
  }

  // Create an ethers provider using the custom RPC URL.
  const provider = new JsonRpcProvider(rpcUrl);

  // Constants and defaults
  const ONE_GWEI = BigNumber.from("1000000000");
  const DEFAULT_CL_DIFF = parseEther("10");
  const DEFAULT_CL_APPEARED_VALIDATORS = BigNumber.from(0);
  const DEFAULT_WITHDRAWAL_FINALIZATION_BATCHES: number[] = [];
  const DEFAULT_STAKING_MODULE_IDS: number[] = [];
  const DEFAULT_NUM_EXITED_VALIDATORS: number[] = [];
  const DEFAULT_IS_BUNKER_MODE = false;
  const DEFAULT_EXTRA_DATA_FORMAT = 0;
  const DEFAULT_EXTRA_DATA_HASH = "0x" + "0".repeat(64);
  const DEFAULT_EXTRA_DATA_ITEMS_COUNT = 0;

  // Contract ABIs
  const hashConsensusABI = [
    "function getCurrentFrame() view returns (uint256)"
  ];
  const lidoABI = [
    "function getBeaconStat() view returns (uint256 beaconValidators, uint256 beaconBalance)"
  ];
  const accountingOracleABI = [
    "function getConsensusVersion() view returns (uint256)"
  ];
  const burnerABI = [
    "function getSharesRequestedToBurn() view returns (uint256 coverShares, uint256 nonCoverShares)"
  ];

  // Instantiate contract instances using validated addresses.
  const hashConsensus = new Contract(hashConsensusAddr, hashConsensusABI, provider);
  const lido = new Contract(lidoAddr, lidoABI, provider);
  const accountingOracle = new Contract(accountingOracleAddr, accountingOracleABI, provider);
  const burner = new Contract(burnerAddr, burnerABI, provider);

  // 1) Fetch data from the hashConsensus contract.
  const currentFrameResult = await hashConsensus.getCurrentFrame();
  let refSlotBN: BigNumber;
  if (currentFrameResult && currentFrameResult.refSlot !== undefined) {
    refSlotBN = currentFrameResult.refSlot;
  } else {
    refSlotBN = BigNumber.isBigNumber(currentFrameResult)
      ? currentFrameResult
      : BigNumber.from("0");
  }
  if (refSlotBN.isZero()) {
    console.warn("Warning: getCurrentFrame() returned 0 or an unexpected result.");
  }

  // 2) Query beacon statistics from the Lido contract.
  const [beaconValidators, beaconBalance] = await lido.getBeaconStat();
  const postCLBalance = beaconBalance.add(DEFAULT_CL_DIFF);
  const postValidators = BigNumber.from(beaconValidators).add(DEFAULT_CL_APPEARED_VALIDATORS);
  const clBalanceGwei = postCLBalance.div(ONE_GWEI);

  // 3) Query the ETH balances in the vault contracts
  const withdrawalVaultBalanceBN = await provider.getBalance(withdrawalVaultAddr);
  const elRewardsVaultBalanceBN = await provider.getBalance(elRewardsVaultAddr);

  // 4) Query the consensus version from the accounting oracle.
  const consensusVersionBN = await accountingOracle.getConsensusVersion();

  // 5) Format ETH balances from wei to ETH strings.
  const withdrawalVaultBalance = formatEther(withdrawalVaultBalanceBN);
  const elRewardsVaultBalance = formatEther(elRewardsVaultBalanceBN);

  // 6) Fetch shares requested to burn
  const [coverShares, nonCoverShares] = await burner.getSharesRequestedToBurn();
  const sharesRequestedToBurnBN = coverShares.add(nonCoverShares);

  // 7) Simulate withdrawal batches (placeholder logic)
  const withdrawalFinalizationBatches = [1, 2, 3];

  // 8) Assemble the final report object.
  const report: Record<string, any> = {
    "Consensus Version": consensusVersionBN.toString(),
    "Reference Slot": refSlotBN.toString(),
    "CL Balance (Gwei)": clBalanceGwei.toString(),
    "Number of Validators": postValidators.toString(),
    "Withdrawal Vault Balance (ETH)": withdrawalVaultBalance,
    "EL Rewards Vault Balance (ETH)": elRewardsVaultBalance,
    "Shares Requested to Burn": sharesRequestedToBurnBN.toString(),
    "Withdrawal Finalization Batches": JSON.stringify(withdrawalFinalizationBatches),
    "Is Bunker Mode": DEFAULT_IS_BUNKER_MODE,
    "Extra Data Format": DEFAULT_EXTRA_DATA_FORMAT,
    "Extra Data Hash": DEFAULT_EXTRA_DATA_HASH,
    "Extra Data Items Count": DEFAULT_EXTRA_DATA_ITEMS_COUNT,
    "Staking Module IDs with Newly Exited Validators": JSON.stringify(DEFAULT_STAKING_MODULE_IDS),
    "Number of Exited Validators by Staking Module": JSON.stringify(DEFAULT_NUM_EXITED_VALIDATORS),
  };

  // 9) Build the CSV content.
  const headers = Object.keys(report);
  const headerLine = headers.join(",");
  const values = headers.map((h) => report[h]);
  const valueLine = values.join(",");
  const csvContent = `${headerLine}\n${valueLine}\n`;

  // 10) Write the CSV content to file.
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