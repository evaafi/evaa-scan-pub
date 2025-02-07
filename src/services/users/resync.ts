#!/usr/bin/env ts-node

import { Address, Dictionary, TonClient } from "@ton/ton";
import { getFriendlyAddress, MyDatabase } from "../../db/database";

import {
  PoolConfig as EvaaPoolConfig,
  MAINNET_ALTS_POOL_CONFIG,
  MAINNET_LP_POOL_CONFIG,
  MAINNET_POOL_CONFIG,
  PoolAssetConfig,
} from "@evaafi/sdk";
import * as dotenv from "dotenv";
import { PoolConfig } from "pg";
import { GetResult } from "../indexer/types";

dotenv.config();

let tonClient = undefined;

async function getAllUserScData(
  userContractAddress: Address
): Promise<GetResult> {
  const res = await tonClient
    .provider(userContractAddress)
    .get("getAllUserScData", []);

  return {
    exit_code: 0,
    gas_used: 0,
    stack: res.stack,
  };
}

async function getUserData(
  db: MyDatabase,
  userContractAddress: Address
): Promise<{
  codeVersion: number;
  userAddress: Address;
  principals: Map<PoolAssetConfig, bigint>;
  state: number;
}> {
  const result = await getAllUserScData(userContractAddress);
  if (result.exit_code !== 0) {
    throw new Error(
      `User contract ${getFriendlyAddress(
        userContractAddress
      )} returned exit code ${result.exit_code}`
    );
  }
  const codeVersion = result.stack.readNumber();
  result.stack.readCell(); // skip master cell
  const userAddress = result.stack.readCell().beginParse().loadAddress();
  const principalsCell = result.stack.readCellOpt();

  const parser = principalsCell.beginParse();
  const principalsDict = parser.loadDictDirect(
    Dictionary.Keys.BigUint(256),
    Dictionary.Values.BigInt(64)
  );

  const state = result.stack.readNumber();

  if (!principalsCell) {
    throw new Error("No principals cell found in the getPrincipals response");
  }

  const principals: Map<PoolAssetConfig, bigint> = new Map();

  for (const asset of db.evaaPool.poolAssetsConfig) {
    if (principalsDict && principalsDict.has(asset.assetId)) {
      principals.set(asset, principalsDict.get(asset.assetId));
    }
  }

  return {
    codeVersion,
    userAddress,
    principals,
    state,
  };
}

async function syncUsers(db: MyDatabase) {
  try {
    let offset = 0;
    const limit = 100;
    let total = 0;

    while (true) {
      const { users, count } = await db.getUsers(limit, offset);

      for (const user of users) {
        console.log(
          `Syncing user: wallet ${getFriendlyAddress(
            user.wallet_address
          )}, contract ${getFriendlyAddress(user.contract_address)}`
        );
        const { codeVersion, userAddress, principals, state } =
          await getUserData(db, user.contract_address);
        const now = Date.now();
        const existingUser = await db.getUser(
          getFriendlyAddress(user.contract_address)
        );
        if (existingUser) {
          await db.updateUser(
            getFriendlyAddress(user.contract_address),
            codeVersion,
            now,
            now,
            principals,
            state
          );
          console.log(
            `User ${getFriendlyAddress(user.contract_address)} updated`
          );
        } else {
          await db.addUser(
            getFriendlyAddress(userAddress),
            getFriendlyAddress(user.contract_address),
            codeVersion,
            now,
            now,
            principals,
            state
          );
          console.log(
            `User ${getFriendlyAddress(user.contract_address)} added`
          );
        }
      }

      total += users.length;

      if (total >= count) {
        break;
      }

      offset += limit;
    }
  } catch (err) {
    console.error(`Failed to sync user:`, err);
  }
}

async function syncAllUsers() {
  if (tonClient == undefined) {
    tonClient = new TonClient({
      endpoint: process.env.RPC_URL,
      apiKey: process.env.RPC_API_KEY,
    });
  }

  const pools = {
    main: MAINNET_POOL_CONFIG,
    lp: MAINNET_LP_POOL_CONFIG,
    alts: MAINNET_ALTS_POOL_CONFIG,
  };

  let currentPool: EvaaPoolConfig = pools["main"];
  if (process.env.POOL) {
    currentPool = pools[process.env.POOL];
  }

  const pgConfig: PoolConfig = {
    max: parseInt(process.env.DB_MAX_CONNECTIONS || "5"),
    user: process.env.DB_USER,
    password: process.env.DB_PASS,
    database: process.env.DB_NAME,
    host: process.env.DB_HOST,
    port: Number(process.env.DB_PORT) || 5432,
  };
  const db = new MyDatabase(pgConfig, currentPool);
  await db.init();

  await syncUsers(db);

  console.log("Synchronization of user principals finished");
}

syncAllUsers().catch((err) => {
  console.error("Fatal error during user synchronization:", err);
});
