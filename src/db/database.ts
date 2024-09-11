import {Log, User} from "./types";
import {Pool, PoolConfig} from "pg";
import { Address } from "@ton/core";
import { PoolConfig as EvaaPoolConifg, PoolAssetConfig, PoolAssetsConfig } from "@evaafi/sdk";
import { UserPrincipals } from "../services/indexer/types";
import { AssertionError } from "assert";

export function getFriendlyAddress(address: string | Address): string {
    if (typeof address === 'string') {
        address = Address.parse(address);
    }
    return address.toString({
        bounceable: true,
        urlSafe: true,
        testOnly: false,
    });
}

export class MyDatabase {
    private pgPool: Pool;
    public evaaPool: EvaaPoolConifg;

    private scannerTxsTable: string;
    private logsTable: string;
    private usersTable: string;

    getAssetsColumnNames(assets: PoolAssetsConfig): string[] {
        let result: string[] = [];

        for (const asset of assets) {
            result.push(asset.name.toLowerCase() + "_principal");
        }

        return result;
    }

    createNumberString(n: number): string {
        return Array.from({ length: n }, (_, i) => '$' + (i + 1).toString()).join(', ');  // $1, $2,...
    }

    constructor(pgConfig: PoolConfig, evaaPool: EvaaPoolConifg) {
        this.pgPool = new Pool(pgConfig);
        this.evaaPool = evaaPool;
        this.scannerTxsTable = process.env.SCANNER_TXS_TABLE;
        this.logsTable = process.env.LOGS_TABLE;
        this.usersTable = process.env.USERS_TABLE;
    }

    async init() {

        await this.pgPool.query(`
              CREATE TABLE IF NOT EXISTS ${this.scannerTxsTable} (
                  id SERIAL PRIMARY KEY,
                  hash NUMERIC(78, 0) NOT NULL UNIQUE,
                  utime TIMESTAMP NOT NULL
              )
          `);
        
        await this.pgPool.query(`
            CREATE TABLE IF NOT EXISTS ${this.logsTable}(
                id BIGSERIAL PRIMARY KEY,
                hash NUMERIC(78, 0) NOT NULL,
                utime INTEGER NOT NULL,
                tx_type VARCHAR NOT NULL,
                sender_address VARCHAR NOT NULL,
                user_address VARCHAR NOT NULL,
                attached_asset_address NUMERIC(78, 0),
                attached_asset_amount NUMERIC(20, 0),
                attached_asset_principal NUMERIC(19, 0),
                attached_asset_total_supply_principal NUMERIC(19, 0),
                attached_asset_total_borrow_principal NUMERIC(19, 0),
                attached_asset_s_rate NUMERIC(20, 0),
                attached_asset_b_rate NUMERIC(20, 0),
                redeemed_asset_address NUMERIC(78, 0),
                redeemed_asset_amount NUMERIC(20, 0),
                redeemed_asset_principal NUMERIC(19, 0),
                redeemed_asset_total_supply_principal NUMERIC(19, 0),
                redeemed_asset_total_borrow_principal NUMERIC(19, 0),
                redeemed_asset_s_rate NUMERIC(20, 0),
                redeemed_asset_b_rate NUMERIC(20, 0),
                created_at TIMESTAMP DEFAULT now()
            )
      `);

      let principalCollumns = this.getAssetsColumnNames(this.evaaPool.poolAssetsConfig).map(x => x + " NUMERIC(19, 0) NOT NULL DEFAULT 0,\n");
      
      await this.pgPool.query(`
            CREATE TABLE IF NOT EXISTS ${this.usersTable}(
                id SERIAL PRIMARY KEY,
                wallet_address VARCHAR NOT NULL,
                contract_address VARCHAR UNIQUE NOT NULL,
                code_version INTEGER NOT NULL,
                created_at TIMESTAMP NOT NULL,
                updated_at TIMESTAMP NOT NULL,
                ` + principalCollumns.join(' ') + `
                state NUMERIC(19, 0) NOT NULL
            )
        `);
    }

    async addTransaction(hash: bigint, utime: number): Promise<boolean> {
        const result = await this.pgPool.query(`
            WITH cte1 AS (
                SELECT id
                FROM scaner_txs
                WHERE hash = $1
            ), insert_result AS (
                INSERT INTO ${process.env.SCANNER_TXS_TABLE}(hash, utime)
                VALUES($1, $2)
                ON CONFLICT DO NOTHING
            )
            SELECT 
            CASE
                WHEN EXISTS (SELECT 1 FROM cte1) THEN true
                ELSE false
            END AS exists;
        `, [hash.toString(), new Date(utime * 1000).toISOString()]);
        
        return result.rows[0].exists;
    }

    async isTxExists(hash: bigint) {
        const result = await this.pgPool.query(`
            SELECT * FROM ${process.env.SCANNER_TXS_TABLE} WHERE hash = $1
        `, [hash.toString()])
        return result.rows.length > 0
    }

    async addAttachedAssetLog(log: Log) {
        await this.pgPool.query(
            `
            INSERT INTO ${process.env.LOGS_TABLE}(
                hash,
                utime,
                tx_type,
                sender_address,
                user_address,
                attached_asset_address,
                attached_asset_amount,
                attached_asset_principal,
                attached_asset_total_supply_principal,
                attached_asset_total_borrow_principal,
                attached_asset_s_rate,
                attached_asset_b_rate
            ) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        `,
            [
                log.hash.toString(),
                log.utime,
                log.txType,
                getFriendlyAddress(log.senderAddress),
                getFriendlyAddress(log.userAddress),
                log.attachedAssetAddress.toString(),
                log.attachedAssetAmount.toString(),
                log.attachedAssetPrincipal.toString(),
                log.attachedAssetTotalSupplyPrincipal.toString(),
                log.attachedAssetTotalBorrowPrincipal.toString(),
                log.attachedAssetSRate.toString(),
                log.attachedAssetBRate.toString()
            ],
        );
    }

    async addRedeemedAssetLog(log: Log) {
        await this.pgPool.query(
            `
                INSERT INTO ${process.env.LOGS_TABLE}(hash,
                                 utime,
                                 tx_type,
                                 sender_address,
                                 user_address,
                                 redeemed_asset_address,
                                 redeemed_asset_amount,
                                 redeemed_asset_principal,
                                 redeemed_asset_total_supply_principal,
                                 redeemed_asset_total_borrow_principal,
                                 redeemed_asset_s_rate,
                                 redeemed_asset_b_rate)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            `,
            [
                log.hash.toString(),
                log.utime,
                log.txType,
                getFriendlyAddress(log.senderAddress),
                getFriendlyAddress(log.userAddress),
                log.redeemedAssetAddress.toString(),
                log.redeemedAssetAmount.toString(),
                log.redeemedAssetPrincipal.toString(),
                log.redeemedAssetTotalSupplyPrincipal.toString(),
                log.redeemedAssetTotalBorrowPrincipal.toString(),
                log.redeemedAssetSRate.toString(),
                log.redeemedAssetBRate.toString()
            ],
        );
    }

    async addLog(log: Log) {
        await this.pgPool.query(
            `
                INSERT INTO ${process.env.LOGS_TABLE}(hash,
                                 utime,
                                 tx_type,
                                 sender_address,
                                 user_address,
                                 attached_asset_address,
                                 attached_asset_amount,
                                 attached_asset_principal,
                                 attached_asset_total_supply_principal,
                                 attached_asset_total_borrow_principal,
                                 attached_asset_s_rate,
                                 attached_asset_b_rate,
                                 redeemed_asset_address,
                                 redeemed_asset_amount,
                                 redeemed_asset_principal,
                                 redeemed_asset_total_supply_principal,
                                 redeemed_asset_total_borrow_principal,
                                 redeemed_asset_s_rate,
                                 redeemed_asset_b_rate)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
            `,
            [
                log.hash.toString(),
                log.utime,
                log.txType,
                getFriendlyAddress(log.senderAddress),
                getFriendlyAddress(log.userAddress),
                log.attachedAssetAddress.toString(),
                log.attachedAssetAmount.toString(),
                log.attachedAssetPrincipal.toString(),
                log.attachedAssetTotalSupplyPrincipal.toString(),
                log.attachedAssetTotalBorrowPrincipal.toString(),
                log.attachedAssetSRate.toString(),
                log.attachedAssetBRate.toString(),
                log.redeemedAssetAddress.toString(),
                log.redeemedAssetAmount.toString(),
                log.redeemedAssetPrincipal.toString(),
                log.redeemedAssetTotalSupplyPrincipal.toString(),
                log.redeemedAssetTotalBorrowPrincipal.toString(),
                log.redeemedAssetSRate.toString(),
                log.redeemedAssetBRate.toString()
            ],
        );
    }

    async addUser(
        wallet_address: string, contract_address: string, code_version: number,
        created_at: number, updated_at: number, /*ton_principal: bigint,
        jusdt_principal: bigint, jusdc_principal: bigint, stton_principal: bigint, tston_principal: bigint, usdt_principal: bigint,*/ principals: UserPrincipals, state: number) {
        const columns = this.getAssetsColumnNames(Array.from(principals.keys()));

        await this.pgPool.query(`
            INSERT INTO ${this.usersTable}(wallet_address, contract_address, code_version, created_at, updated_at, 
                              ` + columns.join(', ') + `, state)
            VALUES(` + this.createNumberString(columns.length + 6) + `)
        `, [wallet_address, contract_address, code_version, new Date(created_at).toUTCString(), new Date(updated_at).toUTCString(),
            /*ton_principal.toString(), jusdt_principal.toString(), jusdc_principal.toString(), stton_principal.toString(),
            tston_principal.toString(), usdt_principal.toString(),*/...Array.from(principals.values()).map(x => x.toString()), state.toString()])
    }

    async getUser(contract_address: string): Promise<User> {
        const result = await this.pgPool.query(`
            SELECT * FROM ${this.usersTable} WHERE contract_address = $1
        `, [contract_address]);

        if(result.rows.length === 0) return undefined;
        const row = result.rows[0];

        let principals: UserPrincipals = new Map<PoolAssetConfig, bigint>();
        for (const asset of this.evaaPool.poolAssetsConfig) { 
            principals.set(asset, BigInt(row[asset.name.toLowerCase() + "_principal"]));
        }

        return {
            id: row.id,
            wallet_address: row.wallet_address,
            contract_address: row.contract_address,
            codeVersion: row.code_version,
            createdAt: row.created_at.getTime(),
            updatedAt: row.updated_at.getTime(),
            /*tonPrincipal: BigInt(row.ton_principal),
            jusdtPrincipal: BigInt(row.jusdt_principal),
            jusdcPrincipal: BigInt(row.jusdc_principal),
            sttonPrincipal: BigInt(row.stton_principal),
            tstonPrincipal: BigInt(row.tston_principal),
            usdtPrincipal: BigInt(row.usdt_principal),*/
            principals: principals,
            state: BigInt(row.state)
        }
    }

    async updateUser(contract_address: string, code_version: number, created_at: number, updated_at,
                     /*tonPrincipal: bigint, jusdtPrincipal: bigint, jusdcPrincipal: bigint, sttonPrincipal: bigint,
                     tstonPrincipal: bigint, usdtPrincipal: bigint,*/ principals: UserPrincipals, state: number) {
        const principalsArr = Array.from(principals.values());
        const assetsArr = this.getAssetsColumnNames(Array.from(principals.keys()));
        const mappedStrings = assetsArr.map((asset, index) => `${asset} = $${index + 8},\n`).join(" ");
                
        console.log(`UPDATE ${this.usersTable}
            SET
                code_version = $1,
                created_at = CASE WHEN created_at > $2 THEN $3 ELSE created_at END,
                updated_at = CASE WHEN updated_at < $4 THEN $5 ELSE updated_at END,
                ` /*ton_principal = $ c 8,
                jusdt_principal = $7,
                jusdc_principal = $8,
                stton_principal = $9,
                tston_principal = $10,
                usdt_principal = $11,
                ` + */
                +
                mappedStrings
                +
                `state = $7
            WHERE
                contract_address = $6;
        `);
        await this.pgPool.query(`
            UPDATE ${this.usersTable}
            SET
                code_version = $1,
                created_at = CASE WHEN created_at > $2 THEN $3 ELSE created_at END,
                updated_at = CASE WHEN updated_at < $4 THEN $5 ELSE updated_at END,
                ` /*ton_principal = $ c 8,
                jusdt_principal = $7,
                jusdc_principal = $8,
                stton_principal = $9,
                tston_principal = $10,
                usdt_principal = $11,
                ` + */
                +
                mappedStrings
                +
                `state = $7
            WHERE
                contract_address = $6;
        `, [code_version, new Date(created_at).toUTCString(), new Date(created_at).toUTCString(), new Date(updated_at).toUTCString(),
            new Date(updated_at).toUTCString(), contract_address, state.toString(),/* tonPrincipal.toString(), jusdtPrincipal.toString(), jusdcPrincipal.toString(),
            sttonPrincipal.toString(), tstonPrincipal.toString(), usdtPrincipal.toString(),*/...principalsArr.map(x => x.toString())])
    }

    async updateUserTime(contract_address: string, created_at: number, updated_at: number) {
        await this.pgPool.query(`
            UPDATE ${this.usersTable}
            SET
                created_at = CASE WHEN created_at > $1 THEN $2 ELSE created_at END,
                updated_at = CASE WHEN updated_at < $3 THEN $4 ELSE updated_at END
            WHERE
                contract_address = $5;
        `, [new Date(created_at).toUTCString(), new Date(created_at).toUTCString(), new Date(updated_at).toUTCString(), new Date(updated_at).toUTCString(), contract_address])
    }
}
