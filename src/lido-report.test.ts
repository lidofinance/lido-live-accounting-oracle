import fs from "fs";
import { expect } from "chai";
import dotenv from "dotenv";
import { main } from "./lido-report";

// Load environment variables from .env
dotenv.config();

describe("Integration Test for Lido Report", () => {
  const outputPath = "./report.csv";
  const requiredEnv = [
    process.env.RPC_URL,
    process.env.HASH_CONSENSUS_ADDRESS,
    process.env.LIDO_ADDRESS,
    process.env.ACCOUNTING_ORACLE_ADDRESS,
    process.env.WITHDRAWAL_VAULT_ADDRESS,
    process.env.EL_REWARDS_VAULT_ADDRESS,
  ];

  // If any required env variable is missing, skip the tests.
  if (requiredEnv.some((value) => !value)) {
    test.skip("Skipping integration test: required env variables are not defined", () => {});
    return;
  }

  beforeAll(() => {
    // Remove the report file if it exists from a previous run.
    if (fs.existsSync(outputPath)) {
      fs.unlinkSync(outputPath);
    }
  });

  test("should produce a valid CSV file using real .env variables", async () => {
    // Increase timeout as network calls may take longer than usual
    jest.setTimeout(30000);

    await main();

    // Check that the output file was created.
    const fileExists = fs.existsSync(outputPath);
    expect(fileExists).to.be.true;

    // Read the CSV content.
    const csvContent = fs.readFileSync(outputPath, "utf8");
    const headerLine = csvContent.split("\n")[0];

    // Expected CSV headers.
    const expectedHeaders = [
      "Consensus Version",
      "Reference Slot",
      "CL Balance (Gwei)",
      "Number of Validators",
      "Withdrawal Vault Balance (ETH)",
      "EL Rewards Vault Balance (ETH)",
      "Shares Requested to Burn",
      "Withdrawal Finalization Batches",
      "Is Bunker Mode",
      "Vaults Values",
      "Vaults In-Out Deltas",
      "Extra Data Format",
      "Extra Data Hash",
      "Extra Data Items Count",
      "Staking Module IDs with Newly Exited Validators",
      "Number of Exited Validators by Staking Module"
    ];

    expectedHeaders.forEach((header) => {
      expect(headerLine).to.contain(header);
    });
  });

  afterAll(() => {
    // Clean up: remove the report file after the test runs.
    if (fs.existsSync(outputPath)) {
      fs.unlinkSync(outputPath);
    }
  });
});