import fs from "fs";
import { expect } from "chai";
import dotenv from "dotenv";
import { ethers } from "ethers";
import sinon from "sinon";
import { main } from "./lido-report";

// Load environment variables from .env
dotenv.config();

describe("Lido Report", () => {
  const outputPath = "./report.csv";
  let provider: sinon.SinonStubbedInstance<ethers.providers.JsonRpcProvider>;
  let hashConsensus: sinon.SinonStubbedInstance<ethers.Contract>;
  let lido: sinon.SinonStubbedInstance<ethers.Contract>;
  let accountingOracle: sinon.SinonStubbedInstance<ethers.Contract>;
  let burner: sinon.SinonStubbedInstance<ethers.Contract>;

  beforeEach(() => {
    // Clean up any existing report file
    if (fs.existsSync(outputPath)) {
      fs.unlinkSync(outputPath);
    }

    // Create stub for provider
    provider = sinon.createStubInstance(ethers.providers.JsonRpcProvider);
    
    // Create stubs for contracts
    hashConsensus = sinon.createStubInstance(ethers.Contract);
    lido = sinon.createStubInstance(ethers.Contract);
    accountingOracle = sinon.createStubInstance(ethers.Contract);
    burner = sinon.createStubInstance(ethers.Contract);

    // Stub provider.getBalance
    provider.getBalance.resolves(ethers.utils.parseEther("10"));

    // Stub contract method responses
    hashConsensus.getCurrentFrame.resolves(ethers.BigNumber.from("1000"));
    lido.getBeaconStat.resolves([
      ethers.BigNumber.from("1000"), // beaconValidators
      ethers.utils.parseEther("32000") // beaconBalance (32 ETH per validator)
    ]);
    accountingOracle.getConsensusVersion.resolves(ethers.BigNumber.from("1"));
    burner.getSharesRequestedToBurn.resolves([
      ethers.BigNumber.from("100"), // coverShares
      ethers.BigNumber.from("200")  // nonCoverShares
    ]);

    // Stub ethers.Contract constructor
    sinon.stub(ethers, "Contract").callsFake((address, abi, provider) => {
      switch(address) {
        case process.env.HASH_CONSENSUS_ADDRESS:
          return hashConsensus;
        case process.env.LIDO_ADDRESS:
          return lido;
        case process.env.ACCOUNTING_ORACLE_ADDRESS:
          return accountingOracle;
        case process.env.BURNER_ADDRESS:
          return burner;
        default:
          return sinon.createStubInstance(ethers.Contract);
      }
    });

    // Stub JsonRpcProvider constructor
    sinon.stub(ethers.providers, "JsonRpcProvider").returns(provider);
  });

  afterEach(() => {
    // Clean up after each test
    sinon.restore();
    if (fs.existsSync(outputPath)) {
      fs.unlinkSync(outputPath);
    }
  });

  describe("Environment Variables", () => {
    const requiredEnvVars = [
      "RPC_URL",
      "HASH_CONSENSUS_ADDRESS",
      "LIDO_ADDRESS",
      "ACCOUNTING_ORACLE_ADDRESS",
      "WITHDRAWAL_VAULT_ADDRESS",
      "EL_REWARDS_VAULT_ADDRESS",
      "BURNER_ADDRESS"
    ];

    requiredEnvVars.forEach(envVar => {
      it(`should error if ${envVar} is missing`, async () => {
        const originalEnv = process.env[envVar];
        process.env[envVar] = undefined;
        
        await expect(main()).to.be.rejected;
        
        process.env[envVar] = originalEnv;
      });
    });
  });

  describe("Contract Interactions", () => {
    it("should handle getCurrentFrame returning an object with refSlot", async () => {
      hashConsensus.getCurrentFrame.resolves({ refSlot: ethers.BigNumber.from("2000") });
      await main();
      const csvContent = fs.readFileSync(outputPath, "utf8");
      expect(csvContent).to.include("2000"); // Reference Slot value
    });

    it("should handle getCurrentFrame returning zero", async () => {
      hashConsensus.getCurrentFrame.resolves(ethers.BigNumber.from("0"));
      const consoleWarn = sinon.stub(console, "warn");
      
      await main();
      
      expect(consoleWarn.calledWith(
        "Warning: getCurrentFrame() returned 0 or an unexpected result. Please verify the contract."
      )).to.be.true;
    });

    it("should correctly sum cover and non-cover shares", async () => {
      const coverShares = ethers.BigNumber.from("100");
      const nonCoverShares = ethers.BigNumber.from("200");
      burner.getSharesRequestedToBurn.resolves([coverShares, nonCoverShares]);
      
      await main();
      
      const csvContent = fs.readFileSync(outputPath, "utf8");
      expect(csvContent).to.include("300"); // Total shares (100 + 200)
    });
  });

  describe("CSV Output", () => {
    it("should create a valid CSV file with all required headers", async () => {
      await main();

      const csvContent = fs.readFileSync(outputPath, "utf8");
      const headers = csvContent.split("\n")[0].split(",");

      const requiredHeaders = [
        "Consensus Version",
        "Reference Slot",
        "CL Balance (Gwei)",
        "Number of Validators",
        "Withdrawal Vault Balance (ETH)",
        "EL Rewards Vault Balance (ETH)",
        "Shares Requested to Burn",
        "Withdrawal Finalization Batches",
        "Is Bunker Mode",
        "Extra Data Format",
        "Extra Data Hash",
        "Extra Data Items Count",
        "Staking Module IDs with Newly Exited Validators",
        "Number of Exited Validators by Staking Module"
      ];

      requiredHeaders.forEach(header => {
        expect(headers).to.include(header);
      });
    });

    it("should format numbers correctly in the CSV", async () => {
      await main();

      const csvContent = fs.readFileSync(outputPath, "utf8");
      const values = csvContent.split("\n")[1].split(",");
      
      // Check that numeric values are properly formatted
      values.forEach(value => {
        if (value.match(/^[0-9]+$/)) {
          expect(parseInt(value)).to.not.be.NaN;
        }
      });
    });
  });

  describe("Error Handling", () => {
    it("should handle contract call failures gracefully", async () => {
      hashConsensus.getCurrentFrame.rejects(new Error("RPC Error"));
      
      await expect(main()).to.be.rejected;
    });

    it("should handle file system errors", async () => {
      sinon.stub(fs, "writeFileSync").throws(new Error("File system error"));
      
      await expect(main()).to.be.rejected;
    });
  });
}); 