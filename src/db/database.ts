import {Log, Task, User} from "./types";
import {Pool, PoolConfig} from "pg";
import { Address } from "@ton/core";

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
    private pool: Pool;

    constructor(pgConfig: PoolConfig) {
        this.pool = new Pool(pgConfig);
    }

    async init() {
        await this.pool.query(`
              CREATE TABLE IF NOT EXISTS scaner_txs(
                  id SERIAL PRIMARY KEY,
                  hash NUMERIC(78, 0) NOT NULL UNIQUE,
                  utime TIMESTAMP NOT NULL
              )
          `);

        await this.pool.query(`
            CREATE TABLE IF NOT EXISTS logs(
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
    }

    async addTransaction(hash: bigint, utime: number): Promise<boolean> {
        const result = await this.pool.query(`
            WITH cte1 AS (
                SELECT id
                FROM scaner_txs
                WHERE hash = $1
            ), insert_result AS (
                INSERT INTO scaner_txs(hash, utime)
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
        const result = await this.pool.query(`
            SELECT * FROM scaner_txs WHERE hash = $1
        `, [hash.toString()])
        return result.rows.length > 0
    }

    async addAttachedAssetLog(log: Log) {
        await this.pool.query(
            `
            INSERT INTO logs(
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
        await this.pool.query(
            `
                INSERT INTO logs(hash,
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
        await this.pool.query(
            `
                INSERT INTO logs(hash,
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
}
